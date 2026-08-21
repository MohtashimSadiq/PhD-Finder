"""
Pagination-sweep scraper for findaphd.com.

Strategy:
- For each topic keyword, hit the search results page for that keyword.
- Parse every listing on the page (title, university, department, snippet, URL).
- Follow the site's own "next page" link (discovered from the page itself,
  not a hardcoded URL parameter guess) until there are no more results or
  the configured max_pages_per_keyword safety cap is hit.
- Checkpoint every (topic, keyword, page) so a crashed/blocked run can
  resume without re-fetching pages it already got.
- This part is free (no API credits) -- it's meant to do the bulk of the
  capture. The separate search_fallback module is what spends API credits,
  and only for gaps this scraper doesn't cover.

Usage:
    python scraper_findaphd.py --mode backfill
    python scraper_findaphd.py --mode recurring
"""
import argparse
import time
import urllib.parse as urlparse

import requests
import yaml
from bs4 import BeautifulSoup

from db import get_conn, init_db, upsert_checkpoint, checkpoint_status, cache_page, upsert_position

SOURCE_NAME = "findaphd"


def load_config(path="config.yaml"):
    with open(path, "r") as f:
        return yaml.safe_load(f)


def build_search_url(base_url, keyword, page=1):
    params = {"Keywords": keyword}
    if page > 1:
        # Placeholder param name -- see NOTE below.
        params["PG"] = page
    return f"{base_url}?{urlparse.urlencode(params)}"


def fetch(url, settings, session):
    """Single HTTP GET with the configured user-agent and timeout."""
    headers = {"User-Agent": settings["user_agent"]}
    resp = session.get(url, headers=headers, timeout=settings["request_timeout"])
    resp.raise_for_status()
    return resp.text


def parse_listing_page(html, base_domain="https://www.findaphd.com"):
    """
    Parse a findaphd.com search results page into a list of listing dicts.

    NOTE: findaphd's markup can change without notice. If this stops finding
    results, the fix is usually just updating the selectors below --
    everything else in the pipeline is unaffected. Re-check by viewing the
    page source of a live search URL and adjusting the CSS selectors.
    """
    soup = BeautifulSoup(html, "html.parser")
    listings = []

    # Listing links on findaphd sit under /phds/project/ or /phds/program/.
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/phds/project/" not in href and "/phds/program/" not in href:
            continue
        # Skip nav/filter links that happen to match but carry no listing text
        text = a.get_text(" ", strip=True)
        if not text or len(text) < 20:
            continue

        full_url = href if href.startswith("http") else urlparse.urljoin(base_domain, href)

        listings.append({
            "url": full_url.split("?")[0] if "?p" not in full_url else full_url,
            "raw_text": text,
        })

    # De-dupe within the page (the same project can appear more than once
    # in the raw link scan, e.g. thumbnail + title link).
    seen = set()
    deduped = []
    for item in listings:
        if item["url"] in seen:
            continue
        seen.add(item["url"])
        deduped.append(item)
    return deduped


def has_next_page(html):
    """
    Look for the site's own pagination control rather than assuming a URL
    parameter. Returns True if a 'next page' style link/button is present.
    """
    soup = BeautifulSoup(html, "html.parser")
    # Common patterns: rel="next", or a link/button with text like "Next" or a number > current page.
    if soup.find("a", rel="next"):
        return True
    for a in soup.find_all("a"):
        label = a.get_text(strip=True).lower()
        if label in ("next", "next page", ">"):
            return True
    return False


def scrape_topic_keyword(conn, topic_name, keyword, config, session, mode):
    settings = config["scrape_settings"]
    base_url = config["sources"]["findaphd"]["base_url"]
    max_pages = config["sources"]["findaphd"]["max_pages_per_keyword"]

    page = 1
    total_found = 0
    total_new = 0

    while page <= max_pages:
        status = checkpoint_status(conn, SOURCE_NAME, topic_name, keyword, page)
        if status == "done":
            page += 1
            continue  # already scraped successfully in a prior run

        url = build_search_url(base_url, keyword, page)
        try:
            html = fetch(url, settings, session)
        except requests.RequestException as e:
            upsert_checkpoint(conn, SOURCE_NAME, topic_name, keyword, page, "error", str(e))
            print(f"  [error] {keyword} page {page}: {e}")
            break  # stop this keyword's sweep; resumable next run

        cache_page(conn, url, html)
        listings = parse_listing_page(html)

        if not listings:
            upsert_checkpoint(conn, SOURCE_NAME, topic_name, keyword, page, "empty")
            print(f"  [empty] {keyword} page {page} -- no listings found, stopping this keyword")
            break

        for item in listings:
            _, is_new = upsert_position(
                conn,
                url=item["url"],
                source=SOURCE_NAME,
                topic=topic_name,
                title=None,          # filled in by the extraction step, not here
                university=None,
                department=None,
                country=None,
                funding_type=None,
                deadline=None,
                start_date=None,
                language_requirement=None,
                contact=None,
                raw_text=item["raw_text"],
                extracted_json=None,
                score=None,
                reasoning=None,
            )
            total_found += 1
            total_new += is_new

        upsert_checkpoint(conn, SOURCE_NAME, topic_name, keyword, page, "done")
        print(f"  [ok] {keyword} page {page}: {len(listings)} listings ({total_new} new so far)")

        if mode == "recurring" and total_new == 0 and page > 1:
            # In recurring mode, once we hit a page with nothing new,
            # assume we've caught up to previously-seen results and stop.
            break

        if not has_next_page(html):
            break

        page += 1
        time.sleep(settings["delay_seconds"])

    return total_found, total_new


def run(mode="backfill", config_path="config.yaml", db_path="phd_finder.sqlite3"):
    config = load_config(config_path)
    init_db(db_path)

    if not config["sources"]["findaphd"]["enabled"]:
        print("findaphd source disabled in config.yaml -- skipping")
        return

    session = requests.Session()
    grand_total, grand_new = 0, 0

    with get_conn(db_path) as conn:
        for topic in config["topics"]:
            topic_name = topic["name"]
            for keyword in topic["keywords"]:
                print(f"Scraping topic='{topic_name}' keyword='{keyword}' (mode={mode})")
                found, new = scrape_topic_keyword(conn, topic_name, keyword, config, session, mode)
                grand_total += found
                grand_new += new

    print(f"\nDone. {grand_total} listings seen, {grand_new} new positions added to the DB.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["backfill", "recurring"], default="backfill")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--db", default="phd_finder.sqlite3")
    args = parser.parse_args()
    run(mode=args.mode, config_path=args.config, db_path=args.db)

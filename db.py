"""
SQLite schema + helper functions for the PhD position finder.

Run once to initialize: python db.py
"""
import sqlite3
from contextlib import contextmanager

DB_PATH = "phd_finder.sqlite3"

SCHEMA = """
CREATE TABLE IF NOT EXISTS positions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    url TEXT UNIQUE NOT NULL,
    source TEXT NOT NULL,              -- e.g. 'findaphd', 'euraxess'
    topic TEXT,                        -- which topic/keyword surfaced it
    title TEXT,
    university TEXT,
    department TEXT,
    country TEXT,
    funding_type TEXT,                 -- fully_funded | partial | unfunded | unclear
    deadline TEXT,                     -- YYYY-MM-DD or NULL
    start_date TEXT,
    language_requirement TEXT,
    contact TEXT,
    raw_text TEXT,                     -- cached extracted page text (for re-parsing without re-scraping)
    extracted_json TEXT,               -- full structured Gemini extraction output
    score INTEGER,                     -- 0-100 match score
    reasoning TEXT,                    -- why it scored this way
    first_seen TEXT DEFAULT (datetime('now')),
    last_checked TEXT DEFAULT (datetime('now')),
    notified INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_positions_score ON positions(score DESC);
CREATE INDEX IF NOT EXISTS idx_positions_deadline ON positions(deadline);

-- Tracks scrape progress so a bulk run can resume after a crash/block
-- instead of restarting or re-spending search-API credits.
CREATE TABLE IF NOT EXISTS scrape_checkpoints (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    topic TEXT NOT NULL,
    keyword TEXT NOT NULL,
    page INTEGER NOT NULL,
    status TEXT NOT NULL,              -- 'pending' | 'done' | 'error' | 'empty'
    error_message TEXT,
    updated_at TEXT DEFAULT (datetime('now')),
    UNIQUE(source, topic, keyword, page)
);

-- Raw HTML cache, keyed by URL, so failed extraction runs can be retried
-- without re-fetching from the network.
CREATE TABLE IF NOT EXISTS page_cache (
    url TEXT PRIMARY KEY,
    html TEXT NOT NULL,
    fetched_at TEXT DEFAULT (datetime('now'))
);
"""


@contextmanager
def get_conn(db_path: str = DB_PATH):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db(db_path: str = DB_PATH):
    with get_conn(db_path) as conn:
        conn.executescript(SCHEMA)
    print(f"Initialized DB at {db_path}")


def upsert_checkpoint(conn, source, topic, keyword, page, status, error_message=None):
    conn.execute(
        """
        INSERT INTO scrape_checkpoints (source, topic, keyword, page, status, error_message, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
        ON CONFLICT(source, topic, keyword, page)
        DO UPDATE SET status=excluded.status,
                       error_message=excluded.error_message,
                       updated_at=datetime('now')
        """,
        (source, topic, keyword, page, status, error_message),
    )


def checkpoint_status(conn, source, topic, keyword, page):
    row = conn.execute(
        "SELECT status FROM scrape_checkpoints WHERE source=? AND topic=? AND keyword=? AND page=?",
        (source, topic, keyword, page),
    ).fetchone()
    return row["status"] if row else None


def cache_page(conn, url, html):
    conn.execute(
        """
        INSERT INTO page_cache (url, html, fetched_at) VALUES (?, ?, datetime('now'))
        ON CONFLICT(url) DO UPDATE SET html=excluded.html, fetched_at=datetime('now')
        """,
        (url, html),
    )


def get_cached_page(conn, url):
    row = conn.execute("SELECT html FROM page_cache WHERE url=?", (url,)).fetchone()
    return row["html"] if row else None


def upsert_position(conn, **fields):
    """
    Insert a new position or, if the URL already exists, just bump last_checked.
    Extraction/scoring fields are only set on first insert here; a separate
    re-score pass can update score/reasoning later without re-scraping.
    """
    existing = conn.execute("SELECT id FROM positions WHERE url=?", (fields["url"],)).fetchone()
    if existing:
        conn.execute(
            "UPDATE positions SET last_checked=datetime('now') WHERE url=?",
            (fields["url"],),
        )
        return existing["id"], False

    cols = list(fields.keys())
    placeholders = ", ".join("?" for _ in cols)
    col_names = ", ".join(cols)
    cur = conn.execute(
        f"INSERT INTO positions ({col_names}) VALUES ({placeholders})",
        [fields[c] for c in cols],
    )
    return cur.lastrowid, True


if __name__ == "__main__":
    init_db()

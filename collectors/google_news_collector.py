"""
collectors/google_news_collector.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Google News RSS collector for TRON-related search queries.

Searches Google News for the following keywords and stores results in
the raw_articles table with source='google_news':
    · "TRON cryptocurrency"
    · "TRX coin"
    · "Justin Sun"

Google News RSS URL format:
    https://news.google.com/rss/search?q=TRON+cryptocurrency&hl=en&gl=US&ceid=US:en

Usage:
    python -m collectors.google_news_collector   # from project root
    python collectors/google_news_collector.py   # direct run
"""

import calendar
import html
import logging
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote_plus

import feedparser

logger = logging.getLogger(__name__)

# ── Paths / constants ─────────────────────────────────────────────────────────

DB_PATH = Path(__file__).parent.parent / "data" / "sentinel.db"

_GNEWS_BASE    = "https://news.google.com/rss/search"
_SUMMARY_MAX   = 500
_HTML_TAG_RE   = re.compile(r"<[^>]+>")

# Keywords to search; each generates one RSS request.
QUERIES: list[dict] = [
    {"keyword": "TRON cryptocurrency", "language": "en"},
    {"keyword": "TRX coin",            "language": "en"},
    {"keyword": "Justin Sun",          "language": "en"},
]

# ── SQL ───────────────────────────────────────────────────────────────────────

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS raw_articles (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    title        TEXT NOT NULL,
    link         TEXT NOT NULL UNIQUE,
    published_at TEXT,
    source       TEXT NOT NULL,
    summary      TEXT,
    language     TEXT NOT NULL DEFAULT 'en',
    collected_at TEXT NOT NULL
)
"""

_CREATE_INDEX = """
CREATE INDEX IF NOT EXISTS idx_raw_articles_source
    ON raw_articles (source)
"""

_INSERT = """
INSERT OR IGNORE INTO raw_articles
    (title, link, published_at, source, summary, language, collected_at)
VALUES
    (:title, :link, :published_at, :source, :summary, :language, :collected_at)
"""

# ── Helpers ───────────────────────────────────────────────────────────────────


def _strip_html(text: str) -> str:
    """Remove HTML markup and decode entities."""
    text = _HTML_TAG_RE.sub(" ", text)
    text = html.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def _parse_published(entry) -> str:
    """Return ISO-8601 UTC string for the entry's publication date."""
    struct = getattr(entry, "published_parsed", None)
    if struct:
        try:
            ts = calendar.timegm(struct)
            return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
        except (ValueError, OverflowError, OSError):
            pass
    return datetime.now(tz=timezone.utc).isoformat()


def _gnews_url(keyword: str) -> str:
    """Build the Google News RSS search URL for *keyword*."""
    q = quote_plus(keyword)
    return f"{_GNEWS_BASE}?q={q}&hl=en&gl=US&ceid=US:en"


# ── Database ──────────────────────────────────────────────────────────────────


def open_db(db_path: Path = DB_PATH) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute(_CREATE_TABLE)
    conn.execute(_CREATE_INDEX)
    conn.commit()
    return conn


# ── Fetching ──────────────────────────────────────────────────────────────────


def fetch_keyword(kw_cfg: dict) -> list[dict]:
    """
    Fetch Google News RSS for one keyword and return a list of article dicts.

    Google News RSS entries carry the publisher's redirect URL in ``entry.link``
    which is unique per article and therefore suitable as the dedup key.
    """
    keyword  = kw_cfg["keyword"]
    language = kw_cfg["language"]
    url      = _gnews_url(keyword)
    now_utc  = datetime.now(tz=timezone.utc).isoformat()

    logger.info("Fetching Google News: %r  →  %s", keyword, url)

    try:
        parsed = feedparser.parse(url)
    except Exception as exc:
        logger.error("[GoogleNews] Fetch failed for %r: %s", keyword, exc)
        return []

    if parsed.get("bozo") and not parsed.get("entries"):
        logger.warning("[GoogleNews] Malformed feed, no entries for %r", keyword)
        return []

    articles: list[dict] = []
    for entry in parsed.get("entries", []):
        link = getattr(entry, "link", None)
        if not link:
            continue

        title = _strip_html(getattr(entry, "title", "") or "")
        if not title:
            continue

        # Google News titles often include " - Publisher Name" at the end.
        # Keep as-is to preserve source attribution.
        summary = _strip_html(getattr(entry, "summary", "") or "")[:_SUMMARY_MAX]

        articles.append({
            "title":        title,
            "link":         link,
            "published_at": _parse_published(entry),
            "source":       "google_news",
            "summary":      summary,
            "language":     language,
            "collected_at": now_utc,
        })

    logger.info("[GoogleNews] %r → %d article(s)", keyword, len(articles))
    return articles


# ── Collection orchestration ──────────────────────────────────────────────────


def collect_all(conn: sqlite3.Connection) -> int:
    """
    Iterate all QUERIES, insert new articles, return total new-row count.
    Duplicates (same link) are silently skipped via INSERT OR IGNORE.
    """
    total_new = 0
    cur       = conn.cursor()

    for kw_cfg in QUERIES:
        keyword   = kw_cfg["keyword"]
        new_count = 0
        try:
            for article in fetch_keyword(kw_cfg):
                cur.execute(_INSERT, article)
                new_count += cur.rowcount
            conn.commit()
            logger.info("[GoogleNews] %r  →  %d new article(s)", keyword, new_count)
        except sqlite3.Error as exc:
            conn.rollback()
            logger.error("[GoogleNews] DB error for %r: %s", keyword, exc)

        total_new += new_count

    return total_new


# ── Entry point ───────────────────────────────────────────────────────────────


def main() -> None:
    logging.basicConfig(
        level  = logging.INFO,
        format = "%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
        datefmt= "%H:%M:%S",
    )

    conn = open_db()
    print(f"\n数据库位置: {DB_PATH.resolve()}\n")

    try:
        new_count = collect_all(conn)
        total = conn.execute(
            "SELECT COUNT(*) FROM raw_articles WHERE source = 'google_news'"
        ).fetchone()[0]

        sep = "─" * 52
        print(f"\n{sep}")
        print(f"  Google News 本次新增 : {new_count:>5} 条")
        print(f"  Google News 总存量   : {total:>5} 条")
        print(f"{sep}\n")

        # Per-keyword preview (most recently collected)
        latest = conn.execute(
            "SELECT title, published_at FROM raw_articles "
            "WHERE source='google_news' "
            "ORDER BY collected_at DESC LIMIT 5"
        ).fetchall()
        if latest:
            print("最新抓取（Google News）：")
            for title, pub in latest:
                display = title if len(title) <= 60 else title[:59] + "…"
                print(f"  {display}")
                print(f"  {'':2} {pub}")
        print()
    finally:
        conn.close()


if __name__ == "__main__":
    main()

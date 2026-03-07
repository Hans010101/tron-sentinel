"""
collectors/rss_collector.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~
RSS feed collector for crypto news sources.

Fetches articles from CoinDesk, Decrypt, CoinTelegraph, and BlockBeats,
then stores them in a local SQLite database (data/sentinel.db).

Usage:
    python -m collectors.rss_collector   # from project root
    python collectors/rss_collector.py   # direct run
"""

import calendar
import html
import logging
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator

import feedparser

logger = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────

DB_PATH = Path(__file__).parent.parent / "data" / "sentinel.db"

FEEDS: list[dict] = [
    # ── English crypto media ──────────────────────────────────────────────────
    {
        "source":   "CoinDesk",
        "url":      "https://feeds.feedburner.com/CoinDesk",
        "language": "en",
    },
    {
        "source":   "CoinTelegraph",
        "url":      "https://cointelegraph.com/rss",
        "language": "en",
    },
    {
        "source":   "Decrypt",
        "url":      "https://decrypt.co/feed",
        "language": "en",
    },
    {
        "source":   "TheBlock",
        "url":      "https://www.theblock.co/rss.xml",
        "language": "en",
    },
    {
        "source":   "Blockworks",
        "url":      "https://blockworks.co/feed",
        "language": "en",
    },
    {
        "source":   "BitcoinMagazine",
        "url":      "https://bitcoinmagazine.com/.rss/full/",
        "language": "en",
    },
    {
        "source":   "DLNews",
        "url":      "https://www.dlnews.com/arc/outboundfeeds/rss/",
        "language": "en",
    },
    {
        "source":   "Protos",
        "url":      "https://protos.com/feed/",
        "language": "en",
    },
    {
        "source":   "TheDefiant",
        "url":      "https://thedefiant.io/feed",
        "language": "en",
    },
    # ── Google News (English) ─────────────────────────────────────────────────
    {
        "source":   "GoogleNews_TRON",
        "url":      "https://news.google.com/rss/search?q=TRON+TRX+cryptocurrency&hl=en&gl=US&ceid=US:en",
        "language": "en",
    },
    {
        "source":   "GoogleNews_JustinSun",
        "url":      "https://news.google.com/rss/search?q=Justin+Sun+TRON&hl=en&gl=US&ceid=US:en",
        "language": "en",
    },
    # ── Chinese crypto media ──────────────────────────────────────────────────
    {
        "source":   "BlockBeats",
        "url":      "https://www.theblockbeats.info/rss",
        "language": "zh",
    },
    {
        "source":   "JinSeCaiJing",
        "url":      "https://www.jinse.cn/rss",
        "language": "zh",
    },
    {
        "source":   "PANews",
        "url":      "https://www.panewslab.com/rss/zh/index.xml",
        "language": "zh",
    },
    {
        "source":   "ShenChaoTechFlow",
        "url":      "https://www.techflowpost.com/rss",
        "language": "zh",
    },
    {
        "source":   "Bitpush",
        "url":      "https://www.bitpush.news/feed",
        "language": "zh",
    },
    {
        "source":   "8BTC",
        "url":      "https://www.8btc.com/feed",
        "language": "zh",
    },
    {
        "source":   "BlockTempo",
        "url":      "https://www.blocktempo.com/feed/",
        "language": "zh",
    },
    # ── Mainstream media ──────────────────────────────────────────────────────
    {
        "source":   "Reuters_Tech",
        "url":      "https://news.google.com/rss/search?q=TRON+cryptocurrency+site:reuters.com&hl=en&gl=US&ceid=US:en",
        "language": "en",
    },
    {
        "source":   "BBC_Crypto",
        "url":      "https://news.google.com/rss/search?q=TRON+cryptocurrency+site:bbc.com&hl=en&gl=US&ceid=US:en",
        "language": "en",
    },
    {
        "source":   "Guardian_Crypto",
        "url":      "https://news.google.com/rss/search?q=TRON+cryptocurrency+site:theguardian.com&hl=en&gl=US&ceid=US:en",
        "language": "en",
    },
    {
        "source":   "SCMP_Crypto",
        "url":      "https://news.google.com/rss/search?q=TRON+Justin+Sun+site:scmp.com&hl=en&gl=US&ceid=US:en",
        "language": "en",
    },
    {
        "source":   "Forbes_Crypto",
        "url":      "https://news.google.com/rss/search?q=TRON+cryptocurrency+site:forbes.com&hl=en&gl=US&ceid=US:en",
        "language": "en",
    },
]

_SUMMARY_MAX_LEN = 500

# ── Relevance filter ──────────────────────────────────────────────────────
# Only keep articles whose title mentions TRON-related keywords, to avoid
# ingesting unrelated general crypto news from broad feeds.

_RELEVANCE_KEYWORDS = (
    "tron", "trx", "justin sun", "孙宇晨", "波场",
    "sun yuchen", "usdd", "bittorrent", "sunpump", "tron foundation",
    "tron network",
)

_NOISE_TITLE_PATTERNS = (
    "price today", "live price", "price prediction", "to usd",
    "marketcap and", "price analysis", "price forecast",
)


def _is_relevant(title: str) -> bool:
    """Return True if the title is relevant to TRON/Justin Sun and not noise."""
    t = title.lower()
    # Reject noise regardless of keywords
    for noise in _NOISE_TITLE_PATTERNS:
        if noise in t:
            return False
    # Must contain at least one relevance keyword
    return any(kw in t for kw in _RELEVANCE_KEYWORDS)

# ── SQL ────────────────────────────────────────────────────────────────────────

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

# ── Helpers ────────────────────────────────────────────────────────────────────

_HTML_TAG_RE = re.compile(r"<[^>]+>")


def _strip_html(text: str) -> str:
    """Remove HTML tags and decode HTML entities."""
    text = _HTML_TAG_RE.sub(" ", text)
    text = html.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def _parse_published(entry: feedparser.FeedParserDict) -> str:
    """
    Return an ISO-8601 UTC timestamp string for the entry's publication date.

    feedparser normalises all timestamps to UTC struct_time via
    ``published_parsed``.  ``calendar.timegm`` converts UTC struct_time to a
    POSIX timestamp without the local-timezone error that ``time.mktime`` would
    introduce.  Falls back to the current time when the field is absent or
    malformed.
    """
    struct = getattr(entry, "published_parsed", None)
    if struct:
        try:
            ts = calendar.timegm(struct)
            return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
        except (ValueError, OverflowError, OSError):
            pass
    return datetime.now(tz=timezone.utc).isoformat()


def _get_summary(entry: feedparser.FeedParserDict) -> str:
    """Extract, sanitise, and truncate the entry summary to 500 characters."""
    raw = getattr(entry, "summary", None) or getattr(entry, "description", None) or ""
    return _strip_html(raw)[:_SUMMARY_MAX_LEN]


# ── Database ───────────────────────────────────────────────────────────────────

def init_db(db_path: Path = DB_PATH) -> sqlite3.Connection:
    """
    Open (or create) the SQLite database, ensure the schema exists, and return
    the connection.  The parent ``data/`` directory is created automatically.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")   # better concurrent read performance
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute(_CREATE_TABLE)
    conn.execute(_CREATE_INDEX)
    conn.commit()
    logger.debug("Database ready: %s", db_path.resolve())
    return conn


# ── Feed fetching ──────────────────────────────────────────────────────────────

def fetch_feed(feed_cfg: dict) -> Generator[dict, None, None]:
    """
    Parse a single RSS/Atom feed and yield article dicts ready for DB insert.

    Skips entries that have no usable URL or no title.  If the feed is entirely
    unreachable an error is logged and the generator exits without raising.
    """
    source: str = feed_cfg["source"]
    url: str    = feed_cfg["url"]
    lang: str   = feed_cfg.get("language", "en")

    logger.info("Fetching %-15s %s", f"[{source}]", url)

    try:
        parsed = feedparser.parse(url)
    except Exception as exc:                         # network-level failure
        logger.error("[%s] Could not fetch feed: %s", source, exc)
        return

    # feedparser sets bozo=True for malformed XML but may still have entries.
    if parsed.get("bozo") and not parsed.get("entries"):
        exc = parsed.get("bozo_exception", "unknown error")
        logger.warning("[%s] Feed is malformed and has no entries: %s", source, exc)
        return

    now_utc = datetime.now(tz=timezone.utc).isoformat()

    for entry in parsed.get("entries", []):
        link = getattr(entry, "link", None) or getattr(entry, "id", None)
        if not link:
            continue

        title = _strip_html(getattr(entry, "title", "") or "")
        if not title:
            continue

        # Skip articles not related to TRON/Justin Sun
        if not _is_relevant(title):
            continue

        yield {
            "title":        title,
            "link":         link,
            "published_at": _parse_published(entry),
            "source":       source,
            "summary":      _get_summary(entry),
            "language":     lang,
            "collected_at": now_utc,
        }


# ── Collection orchestration ───────────────────────────────────────────────────

def collect_all(conn: sqlite3.Connection) -> int:
    """
    Iterate every configured feed, insert new articles, and return the total
    count of rows actually inserted (duplicates are silently skipped).
    """
    total_new = 0
    cur = conn.cursor()

    for feed_cfg in FEEDS:
        source = feed_cfg["source"]
        new_in_feed = 0
        try:
            for article in fetch_feed(feed_cfg):
                cur.execute(_INSERT, article)
                new_in_feed += cur.rowcount   # 1 if inserted, 0 if duplicate
            conn.commit()
            logger.info("[%s] Inserted %d new article(s)", source, new_in_feed)
        except sqlite3.Error as exc:
            conn.rollback()
            logger.error("[%s] DB error, rolled back: %s", source, exc)

        total_new += new_in_feed

    return total_new


# ── Entry point ────────────────────────────────────────────────────────────────

def main() -> None:
    """
    Standalone test runner.  Fetches all feeds, persists results, and prints
    a concise summary to stdout.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    conn = init_db()
    print(f"\n数据库位置: {DB_PATH.resolve()}\n")

    try:
        new_count = collect_all(conn)
        total = conn.execute("SELECT COUNT(*) FROM raw_articles").fetchone()[0]

        sep = "─" * 52
        print(f"\n{sep}")
        print(f"  本次新增文章 : {new_count:>6} 条")
        print(f"  数据库总存量 : {total:>6} 条")
        print(f"{sep}")

        # Per-source breakdown
        rows = conn.execute(
            "SELECT source, COUNT(*) FROM raw_articles GROUP BY source ORDER BY COUNT(*) DESC"
        ).fetchall()
        if rows:
            print("\n各来源存量：")
            for src, cnt in rows:
                bar = "█" * min(cnt // 2, 30)
                print(f"  {src:<15} {cnt:>5} 条  {bar}")

        # Preview newest 5
        latest = conn.execute(
            "SELECT source, title, published_at FROM raw_articles "
            "ORDER BY collected_at DESC LIMIT 5"
        ).fetchall()
        if latest:
            print("\n最新抓取 5 条：")
            for src, title, pub in latest:
                display = title if len(title) <= 58 else title[:57] + "…"
                print(f"  [{src:<13}] {display}")
                print(f"  {'':16} {pub}")
        print()
    finally:
        conn.close()


if __name__ == "__main__":
    main()

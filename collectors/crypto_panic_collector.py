"""
collectors/crypto_panic_collector.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
CryptoPanic API collector for TRX-related news.

Uses the free CryptoPanic API (no auth token required for public posts)
to fetch curated crypto news with community sentiment votes.

Results are stored in the same ``raw_articles`` SQLite table.
"""

import json
import logging
import sqlite3
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent.parent / "data" / "sentinel.db"

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

_INSERT = """
INSERT OR IGNORE INTO raw_articles
    (title, link, published_at, source, summary, language, collected_at)
VALUES
    (:title, :link, :published_at, :source, :summary, :language, :collected_at)
"""

_API_URL = "https://cryptopanic.com/api/free/v1/posts/?currencies=TRX&kind=news&public=true"


def open_db(db_path: Path = DB_PATH) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(_CREATE_TABLE)
    conn.commit()
    return conn


def collect_crypto_panic(conn: sqlite3.Connection) -> int:
    """
    Fetch TRX news from CryptoPanic free API and store in raw_articles.
    Returns count of newly inserted rows.
    """
    logger.info("Fetching CryptoPanic TRX news")

    try:
        req = urllib.request.Request(_API_URL, headers={
            "User-Agent": "Mozilla/5.0 (compatible; TRONSentinel/1.0)",
        })
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read().decode())
    except Exception as exc:
        logger.warning("CryptoPanic API fetch failed: %s", exc)
        return 0

    results = data.get("results", [])
    logger.info("CryptoPanic: fetched %d posts", len(results))

    now_utc = datetime.now(tz=timezone.utc).isoformat()
    inserted = 0
    cur = conn.cursor()

    for post in results:
        title = post.get("title", "").strip()
        url = post.get("url", "").strip()
        if not title or not url:
            continue

        published = post.get("published_at", "")
        if published:
            try:
                dt = datetime.fromisoformat(published.replace("Z", "+00:00"))
                published = dt.isoformat()
            except (ValueError, TypeError):
                published = now_utc
        else:
            published = now_utc

        # Build summary from votes and source info
        votes = post.get("votes", {})
        source_info = post.get("source", {})
        source_name = source_info.get("title", "CryptoPanic") if isinstance(source_info, dict) else "CryptoPanic"

        vote_parts = []
        for key in ("positive", "negative", "important", "liked", "lol"):
            val = votes.get(key, 0)
            if val:
                vote_parts.append(f"{key}: {val}")
        vote_str = " | ".join(vote_parts) if vote_parts else "no votes yet"

        summary = f"Source: {source_name} | Votes: {vote_str}"

        row = {
            "title": title,
            "link": url,
            "published_at": published,
            "source": f"CryptoPanic ({source_name})",
            "summary": summary[:500],
            "language": "en",
            "collected_at": now_utc,
        }
        cur.execute(_INSERT, row)
        inserted += cur.rowcount

    conn.commit()
    logger.info("CryptoPanic: inserted %d new articles", inserted)
    return inserted


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    conn = open_db()
    print(f"\nDatabase: {DB_PATH.resolve()}\n")
    try:
        count = collect_crypto_panic(conn)
        print(f"\nCryptoPanic: inserted {count} new articles\n")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

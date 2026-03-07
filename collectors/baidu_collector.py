"""
collectors/baidu_collector.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Baidu News RSS collector for Chinese-language TRON coverage.

Queries Baidu News RSS with Chinese keywords to capture mainland China
media coverage of TRON/TRX and Justin Sun.

Results are stored in the same ``raw_articles`` SQLite table.
"""

import logging
import sqlite3
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
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

_BAIDU_KEYWORDS = [
    "波场 TRON",
    "孙宇晨",
    "TRX 波场",
]

_BAIDU_RSS_TPL = "https://news.baidu.com/ns?word={keyword}&tn=newsrss&sr=0&cl=2&rn=20&ct=0"


def open_db(db_path: Path = DB_PATH) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(_CREATE_TABLE)
    conn.commit()
    return conn


def _fetch_baidu_rss(keyword: str) -> list[dict]:
    """Fetch and parse a Baidu News RSS feed for the given keyword."""
    encoded = urllib.parse.quote(keyword)
    url = _BAIDU_RSS_TPL.format(keyword=encoded)
    logger.info("Fetching Baidu News RSS: %s", keyword)

    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (compatible; TRONSentinel/1.0)",
        })
        with urllib.request.urlopen(req, timeout=20) as resp:
            xml_data = resp.read()
    except Exception as exc:
        logger.warning("Baidu RSS fetch failed for '%s': %s", keyword, exc)
        return []

    items = []
    try:
        root = ET.fromstring(xml_data)
        for item in root.iter("item"):
            title_el = item.find("title")
            link_el = item.find("link")
            desc_el = item.find("description")
            pub_el = item.find("pubDate")

            title = title_el.text.strip() if title_el is not None and title_el.text else ""
            link = link_el.text.strip() if link_el is not None and link_el.text else ""
            if not title or not link:
                continue

            description = desc_el.text.strip() if desc_el is not None and desc_el.text else ""
            pub_date = ""
            if pub_el is not None and pub_el.text:
                try:
                    dt = parsedate_to_datetime(pub_el.text.strip())
                    pub_date = dt.isoformat()
                except Exception:
                    pass

            items.append({
                "title": title,
                "link": link,
                "description": description[:500],
                "published_at": pub_date,
            })
    except ET.ParseError as exc:
        logger.warning("Baidu RSS XML parse error for '%s': %s", keyword, exc)

    return items


def collect_baidu(conn: sqlite3.Connection) -> int:
    """
    Run all Baidu News keyword searches and store results.
    Returns count of newly inserted rows.
    """
    now_utc = datetime.now(tz=timezone.utc).isoformat()
    inserted = 0
    cur = conn.cursor()

    for keyword in _BAIDU_KEYWORDS:
        items = _fetch_baidu_rss(keyword)
        logger.info("Baidu '%s': fetched %d items", keyword, len(items))

        for item in items:
            row = {
                "title": item["title"],
                "link": item["link"],
                "published_at": item["published_at"] or now_utc,
                "source": f"Baidu News ({keyword})",
                "summary": item["description"],
                "language": "zh",
                "collected_at": now_utc,
            }
            cur.execute(_INSERT, row)
            inserted += cur.rowcount

    conn.commit()
    logger.info("Baidu News: inserted %d new articles total", inserted)
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
        count = collect_baidu(conn)
        print(f"\nBaidu News: inserted {count} new articles\n")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

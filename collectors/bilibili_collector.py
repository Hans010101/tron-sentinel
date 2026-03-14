"""
collectors/bilibili_collector.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Bilibili search collector for TRON Sentinel.

Uses Bilibili's public search API to collect video results related to
TRON/Justin Sun. No authentication required.

Results are stored in the same ``raw_articles`` SQLite table.
"""

import json
import logging
import sqlite3
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
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

_SEARCH_KEYWORDS = ["孙宇晨", "波场TRON", "TRON区块链"]

_SEARCH_URL = "https://api.bilibili.com/x/web-interface/search/all"

_NOISE_TITLE_PATTERNS = (
    "price today", "live price", "price prediction", "to usd",
    "marketcap and",
)


def open_db(db_path: Path = DB_PATH) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(_CREATE_TABLE)
    conn.commit()
    return conn


def _strip_html_tags(text: str) -> str:
    """Remove HTML highlight tags from Bilibili search results."""
    import re
    return re.sub(r"<[^>]+>", "", text)


def _search_bilibili(keyword: str) -> list[dict]:
    """Search Bilibili and return video results."""
    params = urllib.parse.urlencode({
        "keyword": keyword,
        "search_type": "video",
        "order": "pubdate",
        "page": 1,
        "pagesize": 20,
    })
    url = f"{_SEARCH_URL}?{params}"
    logger.info("Fetching Bilibili search: %s", keyword)

    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": "https://www.bilibili.com",
        })
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read().decode())
    except Exception as exc:
        logger.warning("Bilibili search failed for '%s': %s", keyword, exc)
        return []

    if data.get("code") != 0:
        logger.warning("Bilibili API error for '%s': %s", keyword, data.get("message", ""))
        return []

    result_data = data.get("data", {})
    results = result_data.get("result", [])

    # result may be a list of category groups or direct video list
    videos = []
    if isinstance(results, list):
        for item in results:
            if isinstance(item, dict) and item.get("result_type") == "video":
                videos.extend(item.get("data", []))
            elif isinstance(item, dict) and "bvid" in item:
                videos.append(item)

    return videos


def collect_bilibili(conn: sqlite3.Connection) -> int:
    """
    Search Bilibili for TRON-related videos and store results.
    Returns count of newly inserted rows.
    """
    now_dt     = datetime.now(tz=timezone.utc)
    now_utc    = now_dt.isoformat()
    cutoff_15d = now_dt - timedelta(days=15)
    inserted   = 0
    cur        = conn.cursor()

    for keyword in _SEARCH_KEYWORDS:
        videos = _search_bilibili(keyword)
        logger.info("Bilibili '%s': fetched %d videos", keyword, len(videos))

        for video in videos:
            title = _strip_html_tags(video.get("title", "")).strip()
            if not title or len(title) < 5:
                continue

            title_lower = title.lower()
            if any(p in title_lower for p in _NOISE_TITLE_PATTERNS):
                continue

            bvid = video.get("bvid", "")
            arcurl = video.get("arcurl", "")
            url = arcurl if arcurl else f"https://www.bilibili.com/video/{bvid}" if bvid else ""
            if not url:
                continue

            author = video.get("author", "unknown")
            play = video.get("play", 0)
            danmaku = video.get("video_review") or video.get("danmaku", 0)
            description = video.get("description", "")
            pubdate = video.get("pubdate", 0)

            published_at = now_utc
            if pubdate and isinstance(pubdate, (int, float)):
                try:
                    dt = datetime.fromtimestamp(pubdate, tz=timezone.utc)
                    if dt < cutoff_15d:
                        continue
                    published_at = dt.isoformat()
                except (ValueError, OSError):
                    pass

            summary = (
                f"UP: {author} | Plays: {play} | Danmaku: {danmaku}\n"
                f"{description[:300]}"
            )

            row = {
                "title": title,
                "link": url,
                "published_at": published_at,
                "source": f"Bilibili ({author})",
                "summary": summary[:500],
                "language": "zh",
                "collected_at": now_utc,
            }
            cur.execute(_INSERT, row)
            inserted += cur.rowcount

    conn.commit()
    logger.info("Bilibili: inserted %d new videos total", inserted)
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
        count = collect_bilibili(conn)
        print(f"\nBilibili: inserted {count} new videos\n")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

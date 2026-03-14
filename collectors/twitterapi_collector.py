"""
collectors/twitterapi_collector.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
twitterapi.io Advanced Search collector for TRON/TRX/Justin Sun tweets.

Fetches top tweets via the twitterapi.io Advanced Search API and stores
them in the local SQLite database (data/sentinel.db).

Requires environment variable:
    TWITTERAPI_KEY  – your twitterapi.io API key

If the key is not set, the collector silently skips with a notice.

Usage:
    python -m collectors.twitterapi_collector   # from project root
    python collectors/twitterapi_collector.py   # direct run
"""

import json
import logging
import os
import sqlite3
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────

DB_PATH  = Path(__file__).parent.parent / "data" / "sentinel.db"
_API_URL = "https://api.twitterapi.io/twitter/tweet/advanced_search"
_QUERY   = "TRON OR TRX OR Justin Sun OR 孙宇晨 OR 波场"
_MAX_TWEETS = 20

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

# ── Database ───────────────────────────────────────────────────────────────────

def open_db(db_path: Path = DB_PATH) -> sqlite3.Connection:
    """Open (or create) the SQLite database and ensure the schema exists."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute(_CREATE_TABLE)
    conn.execute(_CREATE_INDEX)
    conn.commit()
    return conn


# ── Helpers ────────────────────────────────────────────────────────────────────

def _parse_twitter_date(date_str: str) -> str:
    """
    Parse Twitter date format "Wed Mar 12 14:30:00 +0000 2025"
    or ISO format, returning an ISO-8601 UTC string.
    Falls back to current time on failure.
    """
    if not date_str:
        return datetime.now(tz=timezone.utc).isoformat()
    # Twitter's standard format
    for fmt in ("%a %b %d %H:%M:%S %z %Y", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            dt = datetime.strptime(date_str, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).isoformat()
        except ValueError:
            continue
    return datetime.now(tz=timezone.utc).isoformat()


def _extract_tweets(data: dict | list) -> list[dict]:
    """
    Extract tweet objects from various possible API response shapes:
      - {"tweets": [...]}
      - {"data": [...]}
      - {"results": [...]}
      - [...]  (top-level list)
    """
    if isinstance(data, list):
        return data
    for key in ("tweets", "data", "results"):
        val = data.get(key)
        if isinstance(val, list):
            return val
    return []


# ── Collector ──────────────────────────────────────────────────────────────────

def collect_twitterapi(conn: sqlite3.Connection) -> int:
    """
    Fetch top tweets about TRON/TRX/Justin Sun via twitterapi.io Advanced Search.

    Returns the count of newly inserted tweets.
    Silently returns 0 if TWITTERAPI_KEY is not configured.
    """
    api_key = os.environ.get("TWITTERAPI_KEY", "").strip()
    if not api_key:
        print("  [TwitterAPI.io]  未配置 TWITTERAPI_KEY，跳过")
        return 0

    params = urllib.parse.urlencode({"query": _QUERY, "queryType": "Top"})
    url    = f"{_API_URL}?{params}"
    req    = urllib.request.Request(
        url,
        headers={
            "X-API-Key": api_key,
            "Accept":    "application/json",
        },
    )

    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        print(f"  [TwitterAPI.io]  HTTP错误 {exc.code}: {exc.reason}")
        logger.error("TwitterAPI.io HTTP error: %s %s", exc.code, exc.reason)
        return 0
    except Exception as exc:
        print(f"  [TwitterAPI.io]  请求失败: {exc}")
        logger.error("TwitterAPI.io request failed: %s", exc)
        return 0

    tweets    = _extract_tweets(data)
    now_utc   = datetime.now(tz=timezone.utc)
    cutoff_30d = now_utc - timedelta(days=30)
    now_utc_s  = now_utc.isoformat()

    cur       = conn.cursor()
    new_count = 0

    for tweet in tweets[:_MAX_TWEETS]:
        if not isinstance(tweet, dict):
            continue

        # Extract fields – handle different key naming conventions
        text       = (tweet.get("text") or tweet.get("full_text") or "").strip()
        tweet_id   = tweet.get("id") or tweet.get("id_str") or ""
        username   = (
            tweet.get("author", {}).get("username")
            or tweet.get("user", {}).get("screen_name")
            or tweet.get("username")
            or "unknown"
        )
        display_name = (
            tweet.get("author", {}).get("name")
            or tweet.get("user", {}).get("name")
            or tweet.get("name")
            or username
        )
        created_at = (
            tweet.get("createdAt")
            or tweet.get("created_at")
            or tweet.get("timestamp")
            or ""
        )

        # Engagement metrics
        public_metrics = tweet.get("public_metrics") or tweet.get("metrics") or {}
        likes    = (tweet.get("likeCount")    or public_metrics.get("like_count")    or tweet.get("favorite_count")    or 0)
        retweets = (tweet.get("retweetCount") or public_metrics.get("retweet_count") or tweet.get("retweet_count")    or 0)
        replies  = (tweet.get("replyCount")   or public_metrics.get("reply_count")   or tweet.get("reply_count")      or 0)
        quotes   = (tweet.get("quoteCount")   or public_metrics.get("quote_count")   or tweet.get("quote_count")      or 0)

        if not text or not tweet_id:
            continue

        published_at = _parse_twitter_date(created_at)

        # 30-day freshness filter
        try:
            pub_dt = datetime.fromisoformat(published_at)
            if pub_dt.tzinfo is None:
                pub_dt = pub_dt.replace(tzinfo=timezone.utc)
            if pub_dt < cutoff_30d:
                continue
        except Exception:
            pass

        tweet_url = f"https://twitter.com/{username}/status/{tweet_id}"
        title     = f"@{username}: {text[:100]}"
        summary   = (
            f"{text[:500]}\n"
            f"❤️ {likes}  🔁 {retweets}  💬 {replies}  🔖 {quotes}"
        )

        article = {
            "title":        title,
            "link":         tweet_url,
            "published_at": published_at,
            "source":       f"Twitter/X (@{username})",
            "summary":      summary,
            "language":     "en",
            "collected_at": now_utc_s,
        }

        try:
            cur.execute(_INSERT, article)
            new_count += cur.rowcount
        except sqlite3.Error as exc:
            logger.warning("TwitterAPI DB insert error: %s", exc)

    conn.commit()
    print(f"  [TwitterAPI.io]  成功 {new_count} 条新推文（共解析 {min(len(tweets), _MAX_TWEETS)} 条）")
    return new_count


# ── Entry point ────────────────────────────────────────────────────────────────

def main() -> None:
    """Standalone test runner."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    conn = open_db()
    print(f"\n数据库位置: {DB_PATH.resolve()}\n")
    try:
        count = collect_twitterapi(conn)
        total = conn.execute("SELECT COUNT(*) FROM raw_articles").fetchone()[0]
        print(f"\n  本次新增推文 : {count} 条")
        print(f"  数据库总存量 : {total} 条\n")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

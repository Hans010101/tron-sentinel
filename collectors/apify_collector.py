"""
collectors/apify_collector.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Apify-powered data collectors for TRON Sentinel.

Uses the Apify REST API to run two Actors:
    1. Twitter/X Tweet Scraper  (kaitoeasyapi/twitter-x-data-tweet-scraper-pay-per-result-cheapest)
    2. Google Search Scraper    (apify/google-search-scraper)

Collected data is stored in the same ``raw_articles`` SQLite table used
by the RSS collector, so the dashboard and sentiment pipeline work
seamlessly.

The APIFY_TOKEN environment variable must be set.  On Cloud Run this is
injected via Secret Manager; locally it lives in ``.env``.
"""

import json
import logging
import os
import sqlite3
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent.parent / "data" / "sentinel.db"

_BASE_URL = "https://api.apify.com/v2"

# ── SQL (same schema as rss_collector) ───────────────────────────────────────

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


# ── Database ─────────────────────────────────────────────────────────────────

def open_db(db_path: Path = DB_PATH) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute(_CREATE_TABLE)
    conn.execute(_CREATE_INDEX)
    conn.commit()
    return conn


# ── Apify REST helpers ───────────────────────────────────────────────────────

def _get_token() -> str:
    token = os.environ.get("APIFY_TOKEN", "").strip()
    if not token:
        raise RuntimeError("APIFY_TOKEN environment variable is not set")
    return token


def _api_request(method: str, path: str, token: str,
                 body: dict | None = None, timeout: int = 30) -> dict:
    """Make an authenticated request to the Apify REST API."""
    url = f"{_BASE_URL}{path}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


def _run_actor(actor_id: str, input_data: dict, token: str,
               poll_interval: int = 5, max_wait: int = 120) -> str | None:
    """
    Start an Apify Actor run, poll until it finishes, and return the
    default dataset ID.  Returns None if the run fails or times out.
    """
    logger.info("Starting Apify Actor: %s", actor_id)
    result = _api_request(
        "POST",
        f"/acts/{actor_id}/runs?waitForFinish=0",
        token,
        body=input_data,
    )
    run_data = result.get("data", {})
    run_id = run_data.get("id")
    dataset_id = run_data.get("defaultDatasetId")

    if not run_id:
        logger.error("Failed to start Actor %s: %s", actor_id, result)
        return None

    logger.info("Actor run started: %s (dataset: %s)", run_id, dataset_id)

    # Poll for completion
    elapsed = 0
    while elapsed < max_wait:
        time.sleep(poll_interval)
        elapsed += poll_interval
        status_resp = _api_request("GET", f"/actor-runs/{run_id}", token)
        status = status_resp.get("data", {}).get("status")
        logger.debug("Run %s status: %s (%ds)", run_id, status, elapsed)
        if status == "SUCCEEDED":
            logger.info("Actor run %s completed successfully", run_id)
            return dataset_id
        if status in ("FAILED", "ABORTED", "TIMED-OUT"):
            logger.error("Actor run %s ended with status: %s", run_id, status)
            return None

    logger.warning("Actor run %s timed out after %ds", run_id, max_wait)
    return None


def _get_dataset_items(dataset_id: str, token: str,
                       limit: int = 50) -> list[dict]:
    """Fetch items from an Apify dataset."""
    result = _api_request(
        "GET",
        f"/datasets/{dataset_id}/items?limit={limit}&format=json",
        token,
    )
    # The dataset items endpoint returns a bare list
    if isinstance(result, list):
        return result
    return result.get("data", result.get("items", []))


# ── Twitter/X collector ──────────────────────────────────────────────────────

_TWITTER_ACTOR = "kaitoeasyapi~twitter-x-data-tweet-scraper-pay-per-result-cheapest"

_TWITTER_INPUT = {
    "twitterContent": "TRON OR #TRON OR #TRX OR $TRX crypto",
    "maxItems": 30,
    "queryType": "Latest",
    "lang": "en",
}


def collect_twitter(conn: sqlite3.Connection, token: str | None = None) -> int:
    """
    Run the Tweet Scraper Actor and store results in raw_articles.
    Returns count of newly inserted rows.
    """
    token = token or _get_token()
    dataset_id = _run_actor(_TWITTER_ACTOR, _TWITTER_INPUT, token,
                            poll_interval=8, max_wait=180)
    if not dataset_id:
        logger.warning("Twitter collection: no dataset returned")
        return 0

    items = _get_dataset_items(dataset_id, token, limit=30)
    logger.info("Twitter: fetched %d items from dataset", len(items))

    now_utc = datetime.now(tz=timezone.utc).isoformat()
    inserted = 0
    cur = conn.cursor()

    for tweet in items:
        # Skip bot/spam tweets and empty results
        if tweet.get("noResults"):
            continue
        text = tweet.get("text", "")
        if not text or len(text) < 10:
            continue

        tweet_url = tweet.get("url", "")
        if not tweet_url:
            tweet_id = tweet.get("id", "")
            if tweet_id:
                tweet_url = f"https://x.com/i/status/{tweet_id}"
            else:
                continue

        # Build author display name
        author = tweet.get("author", {})
        if isinstance(author, dict):
            author_name = author.get("userName") or author.get("name", "unknown")
        else:
            author_name = str(author) if author else "unknown"

        likes = tweet.get("likeCount", 0)
        retweets = tweet.get("retweetCount", 0)
        created = tweet.get("createdAt", "")

        # Parse Twitter date format "Sat Mar 07 03:43:38 +0000 2026"
        published_at = now_utc
        if created:
            try:
                dt = datetime.strptime(created, "%a %b %d %H:%M:%S %z %Y")
                published_at = dt.isoformat()
            except (ValueError, TypeError):
                pass

        title = f"@{author_name}: {text[:120]}"
        summary = (
            f"{text[:400]}\n\n"
            f"Likes: {likes} | Retweets: {retweets}"
        )

        row = {
            "title": title,
            "link": tweet_url,
            "published_at": published_at,
            "source": "Twitter/X (Apify)",
            "summary": summary,
            "language": tweet.get("lang", "en"),
            "collected_at": now_utc,
        }
        cur.execute(_INSERT, row)
        inserted += cur.rowcount

    conn.commit()
    logger.info("Twitter: inserted %d new tweets", inserted)
    return inserted


# ── Google Search News collector ─────────────────────────────────────────────

_GOOGLE_ACTOR = "apify~google-search-scraper"

_GOOGLE_INPUT = {
    "queries": "TRON blockchain news\nTRX crypto news\nJustin Sun TRON",
    "maxPagesPerQuery": 1,
    "countryCode": "us",
    "searchLanguage": "en",
    "quickDateRange": "w1",
}


def collect_google_news(conn: sqlite3.Connection,
                        token: str | None = None) -> int:
    """
    Run the Google Search Scraper Actor and store organic results
    in raw_articles.  Returns count of newly inserted rows.
    """
    token = token or _get_token()
    dataset_id = _run_actor(_GOOGLE_ACTOR, _GOOGLE_INPUT, token,
                            poll_interval=8, max_wait=180)
    if not dataset_id:
        logger.warning("Google News collection: no dataset returned")
        return 0

    items = _get_dataset_items(dataset_id, token, limit=10)
    logger.info("Google: fetched %d search result pages", len(items))

    now_utc = datetime.now(tz=timezone.utc).isoformat()
    inserted = 0
    cur = conn.cursor()

    for page in items:
        organic = page.get("organicResults", [])
        for result in organic:
            title = result.get("title", "")
            url = result.get("url", "")
            if not title or not url:
                continue

            description = result.get("description", "")
            date_str = result.get("date")
            published_at = date_str if date_str else now_utc

            displayed_url = result.get("displayedUrl", "")
            source_name = displayed_url.split("/")[0] if displayed_url else "Google"

            row = {
                "title": title,
                "link": url,
                "published_at": published_at,
                "source": f"Google ({source_name})",
                "summary": description[:500],
                "language": "en",
                "collected_at": now_utc,
            }
            cur.execute(_INSERT, row)
            inserted += cur.rowcount

    conn.commit()
    logger.info("Google News: inserted %d new articles", inserted)
    return inserted


# ── Combined collection entry point ──────────────────────────────────────────

def collect_all(conn: sqlite3.Connection) -> dict:
    """
    Run both Apify collectors.
    Returns {"twitter": N, "google": N} with insert counts.
    """
    token = _get_token()
    twitter_count = 0
    google_count = 0

    try:
        twitter_count = collect_twitter(conn, token)
    except Exception as exc:
        logger.exception("Apify Twitter collection failed: %s", exc)

    try:
        google_count = collect_google_news(conn, token)
    except Exception as exc:
        logger.exception("Apify Google News collection failed: %s", exc)

    return {"twitter": twitter_count, "google": google_count}


# ── Standalone entry point ───────────────────────────────────────────────────

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    conn = open_db()
    print(f"\nDatabase: {DB_PATH.resolve()}\n")

    try:
        result = collect_all(conn)
        sep = "-" * 52
        print(f"\n{sep}")
        print(f"  Twitter/X tweets inserted : {result['twitter']}")
        print(f"  Google news inserted      : {result['google']}")
        print(f"{sep}\n")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

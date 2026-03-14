"""
collectors/apify_collector.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Apify-powered data collectors for TRON Sentinel.

Uses the Apify REST API to run Actors:
    1. Twitter/X Tweet Scraper  (kaitoeasyapi/twitter-x-data-tweet-scraper-pay-per-result-cheapest)
    2. Google Search Scraper    (apify/google-search-scraper)
    3. YouTube Search Scraper   (scrapesmith/youtube-free-search-scraper)
    4. Reddit Posts Scraper     (vulnv/reddit-posts-search-scraper)
    5. TikTok Scraper           (clockworks/tiktok-scraper)
    6. Weibo Scraper            (piotrv1001/weibo-scraper)

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
from datetime import datetime, timedelta, timezone
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


# ── Freshness helper ─────────────────────────────────────────────────────────

def _is_fresh(published_at_str: str | None, cutoff: datetime) -> bool:
    """Return True if the article is within the retention window."""
    if not published_at_str:
        return True
    try:
        dt = datetime.fromisoformat(published_at_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt >= cutoff
    except Exception:
        return True


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
    "twitterContent": (
        "\"Justin Sun\" OR \"孙宇晨\" OR \"TRON Network\" OR #TRX "
        "OR #TRON OR $TRX OR TRON crypto"
    ),
    "maxItems": 50,
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

    items = _get_dataset_items(dataset_id, token, limit=50)
    logger.info("Twitter: fetched %d items from dataset", len(items))

    _now = datetime.now(tz=timezone.utc)
    now_utc = _now.isoformat()
    _cutoff = _now - timedelta(days=15)
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
        if not _is_fresh(row["published_at"], _cutoff):
            continue
        cur.execute(_INSERT, row)
        inserted += cur.rowcount

    conn.commit()
    logger.info("Twitter: inserted %d new tweets", inserted)
    return inserted


# ── Google Search News collector ─────────────────────────────────────────────

_GOOGLE_ACTOR = "apify~google-search-scraper"

_GOOGLE_INPUT = {
    "queries": (
        "\"Justin Sun\"\n"
        "\"TRON Foundation\" OR \"TRON Network\"\n"
        "\"Justin Sun\" crypto\n"
        "TRON blockchain news -price -\"price today\"\n"
        "\"Sun Yuchen\"\n"
        "孙宇晨\n"
        "波场TRON\n"
        "孙宇晨 最新消息\n"
        "波场 区块链"
    ),
    "maxPagesPerQuery": 1,
    "countryCode": "us",
    "searchLanguage": "en",
    "quickDateRange": "w1",
}

# ── Noise filter for Google/news results ──────────────────────────────────

_NOISE_TITLE_PATTERNS = (
    "price today", "live price", "price prediction", "to usd",
    "marketcap and", "price analysis", "price forecast",
    "trading at", "price surge", "price drop",
)


def _is_noise_title(title: str) -> bool:
    """Return True if the title looks like a price/market page, not real news."""
    t = title.lower().strip()
    if len(t) < 8:
        return True
    for pattern in _NOISE_TITLE_PATTERNS:
        if pattern in t:
            return True
    return False


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

    _now = datetime.now(tz=timezone.utc)
    now_utc = _now.isoformat()
    _cutoff = _now - timedelta(days=15)
    inserted = 0
    cur = conn.cursor()

    for page in items:
        organic = page.get("organicResults", [])
        for result in organic:
            title = result.get("title", "")
            url = result.get("url", "")
            if not title or not url:
                continue

            # Filter out price/market noise pages
            if _is_noise_title(title):
                logger.debug("Google: skipping noise title: %s", title[:80])
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
            if not _is_fresh(row["published_at"], _cutoff):
                continue
            cur.execute(_INSERT, row)
            inserted += cur.rowcount

    conn.commit()
    logger.info("Google News: inserted %d new articles", inserted)
    return inserted


# ── YouTube collector ─────────────────────────────────────────────────────────

_YOUTUBE_ACTOR = "scrapesmith~youtube-free-search-scraper"

_YOUTUBE_QUERIES = [
    "Justin Sun TRON",
    "波场 孙宇晨",
    "TRON blockchain",
]


def collect_youtube(conn: sqlite3.Connection, token: str | None = None) -> int:
    """
    Run the YouTube Search Scraper Actor and store results in raw_articles.
    Returns count of newly inserted rows.
    """
    token = token or _get_token()
    input_data = {
        "searchQueries": _YOUTUBE_QUERIES,
        "videosPerSearch": 15,
    }
    dataset_id = _run_actor(_YOUTUBE_ACTOR, input_data, token,
                            poll_interval=8, max_wait=180)
    if not dataset_id:
        logger.warning("YouTube collection: no dataset returned")
        return 0

    items = _get_dataset_items(dataset_id, token, limit=50)
    logger.info("YouTube: fetched %d items from dataset", len(items))

    _now = datetime.now(tz=timezone.utc)
    now_utc = _now.isoformat()
    _cutoff = _now - timedelta(days=15)
    inserted = 0
    cur = conn.cursor()

    for video in items:
        title = video.get("title", "").strip()
        url = video.get("url") or video.get("link", "")
        if not title or not url:
            continue
        if _is_noise_title(title):
            continue

        channel = video.get("channelName") or video.get("channel", "unknown")
        views = video.get("viewCount") or video.get("views", 0)
        published = video.get("publishedAt") or video.get("date", "")

        published_at = now_utc
        if published:
            try:
                dt = datetime.fromisoformat(published.replace("Z", "+00:00"))
                published_at = dt.isoformat()
            except (ValueError, TypeError):
                published_at = now_utc

        summary = f"Channel: {channel} | Views: {views}"

        row = {
            "title": title,
            "link": url,
            "published_at": published_at,
            "source": f"YouTube ({channel})",
            "summary": summary[:500],
            "language": "en",
            "collected_at": now_utc,
        }
        if not _is_fresh(row["published_at"], _cutoff):
            continue
        cur.execute(_INSERT, row)
        inserted += cur.rowcount

    conn.commit()
    logger.info("YouTube: inserted %d new videos", inserted)
    return inserted


# ── Reddit collector ──────────────────────────────────────────────────────────

_REDDIT_ACTOR = "vulnv~reddit-posts-search-scraper"

_REDDIT_SEARCHES = [
    {"keyword": "TRON TRX", "limit": 25, "sort": "new", "time_filter": "week"},
    {"keyword": "Justin Sun", "limit": 25, "sort": "new", "time_filter": "week"},
]


def collect_reddit(conn: sqlite3.Connection, token: str | None = None) -> int:
    """
    Run the Reddit Posts Scraper Actor for each keyword and store results.
    Returns count of newly inserted rows.
    """
    token = token or _get_token()
    _now = datetime.now(tz=timezone.utc)
    now_utc = _now.isoformat()
    _cutoff = _now - timedelta(days=15)
    inserted = 0
    cur = conn.cursor()

    for search in _REDDIT_SEARCHES:
        dataset_id = _run_actor(_REDDIT_ACTOR, search, token,
                                poll_interval=8, max_wait=180)
        if not dataset_id:
            logger.warning("Reddit collection for '%s': no dataset", search["keyword"])
            continue

        items = _get_dataset_items(dataset_id, token, limit=30)
        logger.info("Reddit '%s': fetched %d items", search["keyword"], len(items))

        for post in items:
            title = post.get("title", "").strip()
            url = post.get("url") or post.get("permalink", "")
            if not title or not url:
                continue
            if _is_noise_title(title):
                continue

            # Ensure full Reddit URL
            if url.startswith("/r/"):
                url = f"https://www.reddit.com{url}"

            subreddit = post.get("subreddit", "")
            score = post.get("score") or post.get("ups", 0)
            num_comments = post.get("num_comments") or post.get("comments", 0)
            selftext = post.get("selftext") or post.get("body", "")
            created = post.get("created_utc") or post.get("created", "")

            published_at = now_utc
            if created:
                try:
                    if isinstance(created, (int, float)):
                        dt = datetime.fromtimestamp(created, tz=timezone.utc)
                        published_at = dt.isoformat()
                    else:
                        dt = datetime.fromisoformat(str(created).replace("Z", "+00:00"))
                        published_at = dt.isoformat()
                except (ValueError, TypeError, OSError):
                    pass

            summary = (
                f"r/{subreddit} | Score: {score} | Comments: {num_comments}\n"
                f"{selftext[:300]}"
            )

            row = {
                "title": title,
                "link": url,
                "published_at": published_at,
                "source": f"Reddit (r/{subreddit})" if subreddit else "Reddit",
                "summary": summary[:500],
                "language": "en",
                "collected_at": now_utc,
            }
            if not _is_fresh(row["published_at"], _cutoff):
                continue
            cur.execute(_INSERT, row)
            inserted += cur.rowcount

    conn.commit()
    logger.info("Reddit: inserted %d new posts", inserted)
    return inserted


# ── TikTok collector ──────────────────────────────────────────────────────────

_TIKTOK_ACTOR = "clockworks~tiktok-scraper"

_TIKTOK_QUERIES = ["TRON crypto", "Justin Sun", "波场"]


def collect_tiktok(conn: sqlite3.Connection, token: str | None = None) -> int:
    """
    Run the TikTok Scraper Actor and store results in raw_articles.
    Returns count of newly inserted rows.
    """
    token = token or _get_token()
    input_data = {
        "searchQueries": _TIKTOK_QUERIES,
        "resultsPerPage": 15,
    }
    dataset_id = _run_actor(_TIKTOK_ACTOR, input_data, token,
                            poll_interval=10, max_wait=240)
    if not dataset_id:
        logger.warning("TikTok collection: no dataset returned")
        return 0

    items = _get_dataset_items(dataset_id, token, limit=50)
    logger.info("TikTok: fetched %d items from dataset", len(items))

    _now = datetime.now(tz=timezone.utc)
    now_utc = _now.isoformat()
    _cutoff = _now - timedelta(days=15)
    inserted = 0
    cur = conn.cursor()

    for video in items:
        desc = video.get("text") or video.get("desc") or video.get("description", "")
        if not desc or len(desc.strip()) < 5:
            continue

        url = video.get("webVideoUrl") or video.get("url", "")
        video_id = video.get("id", "")
        if not url and video_id:
            url = f"https://www.tiktok.com/@unknown/video/{video_id}"
        if not url:
            continue

        if _is_noise_title(desc):
            continue

        author_info = video.get("authorMeta") or video.get("author", {})
        if isinstance(author_info, dict):
            author = author_info.get("name") or author_info.get("nickName", "unknown")
        else:
            author = str(author_info) if author_info else "unknown"

        likes = video.get("diggCount") or video.get("likes", 0)
        comments = video.get("commentCount") or video.get("comments", 0)
        plays = video.get("playCount") or video.get("plays", 0)
        created = video.get("createTimeISO") or video.get("createTime", "")

        published_at = now_utc
        if created:
            try:
                if isinstance(created, (int, float)):
                    dt = datetime.fromtimestamp(created, tz=timezone.utc)
                    published_at = dt.isoformat()
                else:
                    dt = datetime.fromisoformat(str(created).replace("Z", "+00:00"))
                    published_at = dt.isoformat()
            except (ValueError, TypeError, OSError):
                pass

        title = f"@{author}: {desc[:120]}"
        summary = (
            f"{desc[:300]}\n\n"
            f"Likes: {likes} | Comments: {comments} | Plays: {plays}"
        )

        row = {
            "title": title,
            "link": url,
            "published_at": published_at,
            "source": "TikTok (Apify)",
            "summary": summary[:500],
            "language": "en",
            "collected_at": now_utc,
        }
        if not _is_fresh(row["published_at"], _cutoff):
            continue
        cur.execute(_INSERT, row)
        inserted += cur.rowcount

    conn.commit()
    logger.info("TikTok: inserted %d new videos", inserted)
    return inserted


# ── Weibo collector ───────────────────────────────────────────────────────────

_WEIBO_ACTOR = "piotrv1001~weibo-scraper"

_WEIBO_RELEVANCE_KEYWORDS = (
    "tron", "trx", "波场", "孙宇晨", "justin sun", "usdd",
    "bittorrent", "sunpump", "sun yuchen",
)


def collect_weibo(conn: sqlite3.Connection, token: str | None = None) -> int:
    """
    Run the Weibo Scraper Actor and store TRON-related posts.
    The actor scrapes Weibo's main feed; we filter for TRON relevance.
    Returns count of newly inserted rows.
    """
    token = token or _get_token()
    input_data = {"limit": 100}
    dataset_id = _run_actor(_WEIBO_ACTOR, input_data, token,
                            poll_interval=10, max_wait=180)
    if not dataset_id:
        logger.warning("Weibo collection: no dataset returned")
        return 0

    items = _get_dataset_items(dataset_id, token, limit=100)
    logger.info("Weibo: fetched %d items from dataset", len(items))

    _now = datetime.now(tz=timezone.utc)
    now_utc = _now.isoformat()
    _cutoff = _now - timedelta(days=15)
    inserted = 0
    cur = conn.cursor()

    for post in items:
        text = post.get("text") or post.get("content", "")
        if not text or len(text.strip()) < 10:
            continue

        # Filter for TRON relevance (Weibo feed is general)
        text_lower = text.lower()
        if not any(kw in text_lower for kw in _WEIBO_RELEVANCE_KEYWORDS):
            continue

        url = post.get("url") or post.get("link", "")
        post_id = post.get("id") or post.get("mid", "")
        if not url and post_id:
            url = f"https://weibo.com/{post_id}"
        if not url:
            continue

        user_info = post.get("user") or {}
        if isinstance(user_info, dict):
            user_name = user_info.get("screen_name") or user_info.get("name", "unknown")
        else:
            user_name = "unknown"

        likes = post.get("attitudes_count") or post.get("likes", 0)
        comments = post.get("comments_count") or post.get("comments", 0)
        reposts = post.get("reposts_count") or post.get("reposts", 0)
        created = post.get("created_at", "")

        published_at = now_utc
        if created:
            try:
                dt = datetime.strptime(created, "%a %b %d %H:%M:%S %z %Y")
                published_at = dt.isoformat()
            except (ValueError, TypeError):
                pass

        title = f"@{user_name}: {text[:120]}"
        summary = (
            f"{text[:300]}\n\n"
            f"Likes: {likes} | Comments: {comments} | Reposts: {reposts}"
        )

        row = {
            "title": title,
            "link": url,
            "published_at": published_at,
            "source": "Weibo (Apify)",
            "summary": summary[:500],
            "language": "zh",
            "collected_at": now_utc,
        }
        if not _is_fresh(row["published_at"], _cutoff):
            continue
        cur.execute(_INSERT, row)
        inserted += cur.rowcount

    conn.commit()
    logger.info("Weibo: inserted %d new posts", inserted)
    return inserted


# ── Combined collection entry point ──────────────────────────────────────────

def collect_all(conn: sqlite3.Connection) -> dict:
    """
    Run all Apify collectors.
    Returns dict with insert counts per platform.
    """
    token = _get_token()
    counts: dict[str, int] = {}

    for name, fn in [
        ("twitter", collect_twitter),
        ("google", collect_google_news),
        ("youtube", collect_youtube),
        ("reddit", collect_reddit),
        ("tiktok", collect_tiktok),
        ("weibo", collect_weibo),
    ]:
        try:
            counts[name] = fn(conn, token)
        except Exception as exc:
            logger.exception("Apify %s collection failed: %s", name, exc)
            counts[name] = 0

    return counts


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

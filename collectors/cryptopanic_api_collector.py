"""
collectors/cryptopanic_api_collector.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
CryptoPanic free API collector – multi-strategy edition.

CryptoPanic aggregates crypto news from Reuters, Bloomberg, CoinDesk,
Forbes, BBC, CNBC and 300+ other sources.  The free public API requires
no authentication token.

Four fetch strategies per run:
    1. TRX hot    – currencies=TRX&filter=hot        (coin-specific)
    2. Global hot – filter=hot                        (industry-wide)
    3. Bullish    – filter=bullish&regions=en,cn      (sentiment)
    4. Bearish    – filter=bearish&regions=en,cn      (sentiment)

Each strategy fetches up to 40 posts (API default per page).
Results are stored in raw_articles with source = "CryptoPanic:{media}".
URL-based deduplication is handled by INSERT OR IGNORE.
Only articles from the last 30 days are kept.
"""

import json
import logging
import sqlite3
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

DB_PATH  = Path(__file__).parent.parent / "data" / "sentinel.db"
_BASE    = "https://cryptopanic.com/api/free/v1/posts/"
_TIMEOUT = 20

# ── SQL (identical schema to rss_collector / apify_collector) ─────────────────

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

# ── Fetch strategies ──────────────────────────────────────────────────────────

_STRATEGIES: list[dict] = [
    {
        "label":  "TRX热门",
        "params": "currencies=TRX&filter=hot&public=true",
        "lang":   "en",
    },
    {
        "label":  "全站热门",
        "params": "filter=hot&public=true",
        "lang":   "en",
    },
    {
        "label":  "看涨情绪",
        "params": "filter=bullish&regions=en,cn&public=true",
        "lang":   "en",
    },
    {
        "label":  "看跌情绪",
        "params": "filter=bearish&regions=en,cn&public=true",
        "lang":   "en",
    },
]

_NOISE_PATTERNS = (
    "price today", "live price", "price prediction", "to usd",
    "price analysis", "price forecast", "trading at",
)


def _is_noise(title: str) -> bool:
    t = title.lower()
    return any(p in t for p in _NOISE_PATTERNS)


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


# ── API fetch ─────────────────────────────────────────────────────────────────

def _fetch_posts(params: str) -> list[dict]:
    """Call CryptoPanic API and return the results list (up to 40 items)."""
    url = f"{_BASE}?{params}"
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 (compatible; TRONSentinel/1.0)"},
    )
    try:
        with urllib.request.urlopen(req, timeout=_TIMEOUT) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        return data.get("results", [])
    except urllib.error.HTTPError as exc:
        body_snippet = ""
        try:
            body_snippet = exc.read().decode("utf-8", errors="replace")[:300]
        except Exception:
            pass
        logger.warning(
            "CryptoPanic API HTTP %s for params=%s  body=%s",
            exc.code, params, body_snippet,
        )
        print(
            f"  [CryptoPanic API]  HTTP {exc.code} {exc.reason} "
            f"(params={params[:60]})"
        )
        if body_snippet:
            print(f"  [CryptoPanic API]  响应内容: {body_snippet}")
        return []
    except Exception as exc:
        logger.warning("CryptoPanic API fetch failed (%s): %s", params, exc)
        print(f"  [CryptoPanic API]  请求失败 (params={params[:60]}): {exc}")
        return []


# ── Insert helpers ────────────────────────────────────────────────────────────

def _insert_posts(
    cur: sqlite3.Cursor,
    posts: list[dict],
    default_lang: str,
    cutoff: datetime,
    now_utc: str,
) -> int:
    """Insert posts into raw_articles, return count of new rows."""
    inserted = 0
    for post in posts:
        title = (post.get("title") or "").strip()
        url   = (post.get("url")   or "").strip()
        if not title or not url:
            continue
        if _is_noise(title):
            continue

        # published_at
        pub_raw = post.get("published_at", "")
        try:
            pub_dt = datetime.fromisoformat(pub_raw.replace("Z", "+00:00"))
            if pub_dt.tzinfo is None:
                pub_dt = pub_dt.replace(tzinfo=timezone.utc)
            if pub_dt < cutoff:
                continue
            published_at = pub_dt.isoformat()
        except (ValueError, AttributeError):
            published_at = now_utc

        # source: "CryptoPanic:{media_title}"
        src_info    = post.get("source") or {}
        media_title = (src_info.get("title") or "CryptoPanic") if isinstance(src_info, dict) else "CryptoPanic"
        source      = f"CryptoPanic:{media_title}"

        # currencies mentioned
        currencies_list = post.get("currencies") or []
        coins = ", ".join(
            c.get("code", "") for c in currencies_list if isinstance(c, dict)
        )

        # vote summary
        votes = post.get("votes") or {}
        pos   = votes.get("positive", 0)
        neg   = votes.get("negative", 0)
        imp   = votes.get("important", 0)
        lkd   = votes.get("liked", 0)
        vote_str = f"👍{pos} 👎{neg} ❗{imp} ♥{lkd}"

        summary = f"Coins: {coins or 'N/A'} | {vote_str} | via {media_title}"

        row = {
            "title":        title,
            "link":         url,
            "published_at": published_at,
            "source":       source,
            "summary":      summary[:500],
            "language":     default_lang,
            "collected_at": now_utc,
        }
        cur.execute(_INSERT, row)
        inserted += cur.rowcount

    return inserted


# ── Main collector ────────────────────────────────────────────────────────────

def collect_cryptopanic_api(conn: sqlite3.Connection) -> dict[str, int]:
    """
    Run all four fetch strategies and store results in raw_articles.

    Returns a dict mapping strategy label → newly inserted count.
    Also returns a "total" key with the aggregate.
    """
    now_dt   = datetime.now(tz=timezone.utc)
    now_utc  = now_dt.isoformat()
    cutoff   = now_dt - timedelta(days=30)
    cur      = conn.cursor()
    counts: dict[str, int] = {}

    for i, strategy in enumerate(_STRATEGIES):
        label  = strategy["label"]
        params = strategy["params"]
        lang   = strategy["lang"]

        posts = _fetch_posts(params)
        n = _insert_posts(cur, posts, lang, cutoff, now_utc)
        counts[label] = n
        logger.info("CryptoPanic [%s]: fetched %d posts, inserted %d new", label, len(posts), n)

        conn.commit()
        if i < len(_STRATEGIES) - 1:
            time.sleep(0.5)   # be polite to the free API

    counts["total"] = sum(counts.values())
    return counts


# ── Standalone entry point ────────────────────────────────────────────────────

def main() -> None:
    logging.basicConfig(
        level  = logging.INFO,
        format = "%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
        datefmt= "%H:%M:%S",
    )
    conn = open_db()
    print(f"\n数据库位置: {DB_PATH.resolve()}\n")
    try:
        counts = collect_cryptopanic_api(conn)
        print("\n  CryptoPanic API 采集结果：")
        for label, n in counts.items():
            if label != "total":
                print(f"    {label:<10}  {n} 条新记录")
        print(f"    {'合计':<10}  {counts['total']} 条\n")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

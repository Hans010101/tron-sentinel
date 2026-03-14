"""
collectors/cryptopanic_api_collector.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
CoinGecko News API collector – free, no auth token required.

CoinGecko aggregates crypto news from CoinDesk, Reuters, Bloomberg,
Forbes, BBC, Decrypt, The Block, and 100+ other publishers.

Endpoint:
    GET https://api.coingecko.com/api/v3/news
    Returns up to 100 articles per call, no key required.

Source is stored as the originating publisher name (``news_site`` field),
e.g. "CoinDesk", "Reuters", "Forbes". URL-based deduplication via
INSERT OR IGNORE. 30-day freshness filter applied.

Function renamed from collect_cryptopanic_api() →  collect_crypto_news_api()
so callers in main.py are updated accordingly.
"""

import json
import logging
import sqlite3
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

DB_PATH  = Path(__file__).parent.parent / "data" / "sentinel.db"
_API_URL = "https://api.coingecko.com/api/v3/news"
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

def _fetch_news() -> list[dict]:
    """
    Call CoinGecko /news endpoint and return the article list.

    On HTTP error: prints status + response snippet, returns [].
    On any other error: prints the exception, returns [].
    """
    req = urllib.request.Request(
        _API_URL,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; TRONSentinel/1.0)",
            "Accept":     "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=_TIMEOUT) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        # CoinGecko wraps items in a "data" key
        if isinstance(data, dict):
            items = data.get("data", data.get("results", []))
        else:
            items = data  # top-level list fallback
        logger.info("CoinGecko News: received %d items", len(items))
        return items if isinstance(items, list) else []
    except urllib.error.HTTPError as exc:
        body_snippet = ""
        try:
            body_snippet = exc.read().decode("utf-8", errors="replace")[:300]
        except Exception:
            pass
        logger.warning(
            "CoinGecko News API HTTP %s  body=%s", exc.code, body_snippet
        )
        print(f"  [CoinGecko News]  HTTP {exc.code} {exc.reason}")
        if body_snippet:
            print(f"  [CoinGecko News]  响应内容: {body_snippet}")
        return []
    except Exception as exc:
        logger.warning("CoinGecko News API fetch failed: %s", exc)
        print(f"  [CoinGecko News]  请求失败: {exc}")
        return []


# ── Main collector ────────────────────────────────────────────────────────────

def collect_crypto_news_api(conn: sqlite3.Connection) -> dict[str, int]:
    """
    Fetch CoinGecko news and store in raw_articles.

    Returns a dict:
        {
          "fetched":  <total items received from API>,
          "inserted": <new rows actually written to DB>,
          "skipped":  <duplicates / outside 30-day window>,
        }
    """
    now_dt   = datetime.now(tz=timezone.utc)
    now_utc  = now_dt.isoformat()
    cutoff   = now_dt - timedelta(days=30)
    cur      = conn.cursor()

    items = _fetch_news()
    fetched  = len(items)
    inserted = 0
    skipped  = 0

    for item in items:
        if not isinstance(item, dict):
            skipped += 1
            continue

        title = (item.get("title") or "").strip()
        url   = (item.get("url") or "").strip()
        if not title or not url:
            skipped += 1
            continue

        # Published timestamp
        pub_raw = item.get("published_at") or ""
        try:
            pub_dt = datetime.fromisoformat(pub_raw.replace("Z", "+00:00"))
            if pub_dt.tzinfo is None:
                pub_dt = pub_dt.replace(tzinfo=timezone.utc)
            if pub_dt < cutoff:
                skipped += 1
                continue
            published_at = pub_dt.isoformat()
        except (ValueError, AttributeError):
            published_at = now_utc

        # Source = originating publisher, fallback to "CoinGecko News"
        news_site = (item.get("news_site") or "").strip() or "CoinGecko News"

        # Summary from description field
        description = (item.get("description") or "").strip()
        author      = (item.get("author") or "").strip()
        category    = (item.get("category") or "").strip()
        summary_parts = []
        if description:
            summary_parts.append(description[:400])
        if author:
            summary_parts.append(f"作者: {author}")
        if category:
            summary_parts.append(f"分类: {category}")
        summary = " | ".join(summary_parts)[:500]

        row = {
            "title":        title,
            "link":         url,
            "published_at": published_at,
            "source":       news_site,
            "summary":      summary,
            "language":     "en",
            "collected_at": now_utc,
        }
        cur.execute(_INSERT, row)
        if cur.rowcount:
            inserted += 1
        else:
            skipped += 1

    conn.commit()

    print(
        f"  [CoinGecko News]  获取 {fetched} 条，"
        f"新增 {inserted} 条，跳过 {skipped} 条（重复/过旧）"
    )
    logger.info(
        "CoinGecko News: fetched=%d inserted=%d skipped=%d",
        fetched, inserted, skipped,
    )
    return {"fetched": fetched, "inserted": inserted, "skipped": skipped}


# ── Backward-compat alias ─────────────────────────────────────────────────────
# main.py still calls collect_cryptopanic_api() until updated; keep alias.
collect_cryptopanic_api = collect_crypto_news_api


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
        result = collect_crypto_news_api(conn)
        print(f"\n  获取总数 : {result['fetched']} 条")
        print(f"  新增入库 : {result['inserted']} 条")
        print(f"  跳过     : {result['skipped']} 条\n")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

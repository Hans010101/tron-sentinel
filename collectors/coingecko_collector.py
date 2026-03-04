"""
collectors/coingecko_collector.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
CoinGecko public API collector for TRX real-time market data.

Endpoint:
    GET https://api.coingecko.com/api/v3/coins/tron

Fields collected:
    · price_usd   – current USD spot price
    · change_24h  – 24-hour percentage change
    · market_cap  – USD market capitalisation
    · volume_24h  – 24-hour USD trading volume

Data is stored in the market_data table (created if absent).
One row is inserted per collection run (historical record).

Note: the CoinGecko free tier allows ~10–30 requests/minute.
      Running once per 30-minute scheduler cycle is well within limits.

Usage:
    python -m collectors.coingecko_collector   # from project root
    python collectors/coingecko_collector.py   # direct run
"""

import json
import logging
import sqlite3
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

# ── Paths / constants ─────────────────────────────────────────────────────────

DB_PATH = Path(__file__).parent.parent / "data" / "sentinel.db"

_API_URL = (
    "https://api.coingecko.com/api/v3/coins/tron"
    "?localization=false&tickers=false"
    "&community_data=false&developer_data=false&sparkline=false"
)

_HEADERS = {
    "Accept":     "application/json",
    "User-Agent": "TRON-Sentinel/1.0 (github.com/Hans010101/TRON-Sentinel)",
}

# ── SQL ───────────────────────────────────────────────────────────────────────

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS market_data (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    coin         TEXT NOT NULL,
    source       TEXT NOT NULL,
    price_usd    REAL,
    change_24h   REAL,
    market_cap   REAL,
    volume_24h   REAL,
    tvl          REAL,
    extra_json   TEXT,
    collected_at TEXT NOT NULL
)
"""

_CREATE_INDEX = """
CREATE INDEX IF NOT EXISTS idx_market_data_coin_source
    ON market_data (coin, source)
"""

_INSERT = """
INSERT INTO market_data
    (coin, source, price_usd, change_24h, market_cap, volume_24h,
     extra_json, collected_at)
VALUES
    (:coin, :source, :price_usd, :change_24h, :market_cap, :volume_24h,
     :extra_json, :collected_at)
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


# ── Fetching ──────────────────────────────────────────────────────────────────


def _http_get(url: str) -> dict:
    """Fetch *url* and return parsed JSON. Raises on HTTP / network errors."""
    req = urllib.request.Request(url, headers=_HEADERS)
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode())


def fetch_trx_market() -> dict:
    """
    Call CoinGecko /coins/tron and return a market_data row dict.

    Dict keys:
        coin, source, price_usd, change_24h, market_cap, volume_24h,
        extra_json, collected_at
    """
    logger.info("Fetching TRX market data from CoinGecko…")
    raw = _http_get(_API_URL)

    md      = raw.get("market_data", {})
    price   = md.get("current_price",            {}).get("usd")
    change  = md.get("price_change_percentage_24h")
    mktcap  = md.get("market_cap",               {}).get("usd")
    volume  = md.get("total_volume",             {}).get("usd")

    logger.info(
        "TRX  price=$%.5f  Δ24h=%+.2f%%  mktcap=$%.0f  vol=$%.0f",
        price or 0, change or 0, mktcap or 0, volume or 0,
    )

    extra = {
        "market_cap_rank": raw.get("market_cap_rank"),
        "last_updated":    raw.get("last_updated"),
        "ath_usd":         md.get("ath", {}).get("usd"),
    }

    return {
        "coin":         "TRX",
        "source":       "coingecko",
        "price_usd":    price,
        "change_24h":   change,
        "market_cap":   mktcap,
        "volume_24h":   volume,
        "extra_json":   json.dumps(extra, ensure_ascii=False),
        "collected_at": datetime.now(tz=timezone.utc).isoformat(),
    }


# ── Collection ────────────────────────────────────────────────────────────────


def collect(conn: sqlite3.Connection) -> bool:
    """
    Fetch TRX market data and insert one row into market_data.
    Returns True on success, False on any error.
    """
    try:
        row = fetch_trx_market()
        conn.execute(_INSERT, row)
        conn.commit()
        logger.info("CoinGecko TRX data saved to market_data.")
        return True
    except urllib.error.HTTPError as exc:
        logger.error("CoinGecko HTTP %d: %s", exc.code, exc.reason)
    except urllib.error.URLError as exc:
        logger.error("CoinGecko network error: %s", exc.reason)
    except Exception as exc:
        logger.exception("CoinGecko unexpected error: %s", exc)
    return False


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
        ok = collect(conn)
        if ok:
            row = conn.execute(
                "SELECT price_usd, change_24h, market_cap, volume_24h, collected_at "
                "FROM market_data WHERE source='coingecko' "
                "ORDER BY collected_at DESC LIMIT 1"
            ).fetchone()
            if row:
                price, change, mcap, vol, ts = row
                sep = "─" * 52
                change_str = f"{change:+.2f}%" if change is not None else "N/A"
                print(f"\n{sep}")
                print(f"  TRX 价格    : ${price:.5f}")
                print(f"  24h 涨跌幅  : {change_str}")
                print(f"  市值        : ${mcap:>16,.0f}")
                print(f"  24h 交易量  : ${vol:>16,.0f}")
                print(f"  采集时间    : {ts}")
                print(f"{sep}\n")
        else:
            print("  ✗  CoinGecko 数据获取失败\n")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

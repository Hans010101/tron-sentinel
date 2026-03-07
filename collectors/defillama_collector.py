"""
collectors/defillama_collector.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
DeFiLlama API collector for TRON TVL data.

Endpoint:
    GET https://api.llama.fi/v2/chains  (find Tron entry)

Stores data in the market_data table alongside CoinGecko data.
"""

import json
import logging
import sqlite3
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent.parent / "data" / "sentinel.db"

_CHAINS_URL = "https://api.llama.fi/v2/chains"

_HEADERS = {
    "Accept": "application/json",
    "User-Agent": "TRON-Sentinel/1.0",
}

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

_INSERT = """
INSERT INTO market_data
    (coin, source, price_usd, change_24h, market_cap, volume_24h,
     tvl, extra_json, collected_at)
VALUES
    (:coin, :source, :price_usd, :change_24h, :market_cap, :volume_24h,
     :tvl, :extra_json, :collected_at)
"""


def open_db(db_path: Path = DB_PATH) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute(_CREATE_TABLE)
    conn.commit()
    return conn


def _http_get_json(url: str) -> list | dict:
    req = urllib.request.Request(url, headers=_HEADERS)
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode())


def collect(conn: sqlite3.Connection) -> bool:
    """Fetch TRON TVL from DeFiLlama and insert one row. Returns True on success."""
    try:
        chains = _http_get_json(_CHAINS_URL)
        tron = next((c for c in chains if c.get("name", "").lower() == "tron"), None)
        if not tron:
            logger.error("DeFiLlama: Tron chain not found in /v2/chains")
            return False
        tvl = float(tron["tvl"])
        logger.info("DeFiLlama TRON TVL: $%.2fB", tvl / 1e9)

        row = {
            "coin": "TRX",
            "source": "defillama",
            "price_usd": None,
            "change_24h": None,
            "market_cap": None,
            "volume_24h": None,
            "tvl": tvl,
            "extra_json": json.dumps({"tvl_raw": tvl}),
            "collected_at": datetime.now(tz=timezone.utc).isoformat(),
        }
        conn.execute(_INSERT, row)
        conn.commit()
        logger.info("DeFiLlama data saved to market_data.")
        return True
    except urllib.error.HTTPError as exc:
        logger.error("DeFiLlama HTTP %d: %s", exc.code, exc.reason)
    except urllib.error.URLError as exc:
        logger.error("DeFiLlama network error: %s", exc.reason)
    except Exception as exc:
        logger.exception("DeFiLlama unexpected error: %s", exc)
    return False


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)-8s] %(name)s: %(message)s")
    conn = open_db()
    try:
        ok = collect(conn)
        print("OK" if ok else "FAILED")
    finally:
        conn.close()

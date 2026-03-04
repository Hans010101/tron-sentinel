"""
collectors/defillama_collector.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
DeFiLlama API collector for TRON chain TVL (Total Value Locked).

Endpoint:
    GET https://api.llama.fi/chains

The response is a JSON array of all supported chains.  This collector
locates the TRON entry (matched by name or gecko_id) and stores its TVL
in the market_data table (created if absent).

One row is inserted per collection run (historical record).

Usage:
    python -m collectors.defillama_collector   # from project root
    python collectors/defillama_collector.py   # direct run
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

_API_URL = "https://api.llama.fi/chains"

_HEADERS = {
    "Accept":     "application/json",
    "User-Agent": "TRON-Sentinel/1.0 (github.com/Hans010101/TRON-Sentinel)",
}

# Lowercase strings that identify the TRON chain in the DeFiLlama response.
_TRON_IDS: frozenset[str] = frozenset({"tron", "trx"})

# ── SQL (same market_data table as coingecko_collector) ───────────────────────

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
    (coin, source, tvl, extra_json, collected_at)
VALUES
    (:coin, :source, :tvl, :extra_json, :collected_at)
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


def _http_get(url: str) -> list:
    """Fetch *url* and return parsed JSON list. Raises on HTTP / network errors."""
    req = urllib.request.Request(url, headers=_HEADERS)
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode())


def fetch_tron_tvl() -> dict:
    """
    Fetch DeFiLlama /chains, find the TRON entry, and return a market_data
    row dict ready for DB insertion.

    Raises:
        ValueError  – when the TRON chain cannot be found in the response.
    """
    logger.info("Fetching TRON TVL from DeFiLlama…")
    chains: list[dict] = _http_get(_API_URL)

    tron: dict | None = None
    for chain in chains:
        name      = (chain.get("name")      or "").lower()
        gecko_id  = (chain.get("gecko_id")  or "").lower()
        if name in _TRON_IDS or gecko_id in _TRON_IDS:
            tron = chain
            break

    if tron is None:
        raise ValueError(
            "TRON chain not found in DeFiLlama /chains response. "
            "The API schema may have changed."
        )

    tvl = tron.get("tvl") or tron.get("totalLiquidity") or 0.0
    logger.info("TRON TVL = $%.2fB  ($%,.0f)", tvl / 1e9, tvl)

    extra = {
        "name":        tron.get("name"),
        "gecko_id":    tron.get("gecko_id"),
        "tokenSymbol": tron.get("tokenSymbol"),
        "cmcId":       tron.get("cmcId"),
        "chainId":     tron.get("chainId"),
    }

    return {
        "coin":         "TRX",
        "source":       "defillama",
        "tvl":          tvl,
        "extra_json":   json.dumps(extra, ensure_ascii=False),
        "collected_at": datetime.now(tz=timezone.utc).isoformat(),
    }


# ── Collection ────────────────────────────────────────────────────────────────


def collect(conn: sqlite3.Connection) -> bool:
    """
    Fetch TRON TVL and insert one row into market_data.
    Returns True on success, False on any error.
    """
    try:
        row = fetch_tron_tvl()
        conn.execute(_INSERT, row)
        conn.commit()
        logger.info("DeFiLlama TRON TVL saved to market_data.")
        return True
    except ValueError as exc:
        logger.error("DeFiLlama: %s", exc)
    except urllib.error.HTTPError as exc:
        logger.error("DeFiLlama HTTP %d: %s", exc.code, exc.reason)
    except urllib.error.URLError as exc:
        logger.error("DeFiLlama network error: %s", exc.reason)
    except Exception as exc:
        logger.exception("DeFiLlama unexpected error: %s", exc)
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
                "SELECT tvl, collected_at FROM market_data "
                "WHERE source='defillama' ORDER BY collected_at DESC LIMIT 1"
            ).fetchone()
            if row:
                tvl, ts = row
                sep = "─" * 52
                print(f"\n{sep}")
                print(f"  TRON TVL    : ${tvl / 1e9:.2f}B  (${tvl:>16,.0f})")
                print(f"  采集时间    : {ts}")
                print(f"{sep}\n")
        else:
            print("  ✗  DeFiLlama TVL 获取失败\n")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

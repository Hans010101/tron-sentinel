"""
scripts/health_check.py
~~~~~~~~~~~~~~~~~~~~~~~
TRON Sentinel system health checker.

Inspects the SQLite database and local filesystem to produce a JSON
health report covering:
    - Database accessibility and size
    - Recent collection activity (last 24 h article count)
    - Per-collector last-success timestamps
    - LLM analysis coverage
    - Disk usage of the DB file
    - Overall status: "healthy" | "degraded" | "critical"

Usage (standalone):
    python scripts/health_check.py
    python scripts/health_check.py --json   # machine-readable output

Imported by entrypoint.py to power the /health endpoint.
"""

import argparse
import json
import os
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ── Paths ──────────────────────────────────────────────────────────────────────

ROOT      = Path(__file__).parent.parent
DB_PATH   = ROOT / "data" / "sentinel.db"
JSON_PATH = ROOT / "dashboard" / "data.json"

# ── Collector source groups (must stay in sync with main.py) ──────────────────

_COLLECTOR_SOURCES: dict[str, list[str]] = {
    "rss":          [
        "CoinDesk", "CoinTelegraph", "Decrypt", "TheBlock", "Blockworks",
        "BitcoinMagazine", "DLNews", "Protos", "TheDefiant", "BlockBeats",
        "JinSeCaiJing", "PANews", "ShenChaoTechFlow", "Bitpush", "8BTC",
        "BlockTempo", "GoogleNews_TRON", "GoogleNews_JustinSun",
        "Reuters_Tech", "BBC_Crypto", "Guardian_Crypto", "SCMP_Crypto",
        "Forbes_Crypto",
    ],
    "twitter":      ["apify_twitter"],
    "google_news":  ["apify_google"],
    "youtube":      ["apify_youtube"],
    "reddit":       ["apify_reddit"],
    "tiktok":       ["apify_tiktok"],
    "weibo":        ["apify_weibo"],
    "bilibili":     ["bilibili"],
    "baidu":        ["baidu_news"],
    "crypto_panic": ["crypto_panic"],
    "coingecko":    ["coingecko"],   # market_data table
    "defillama":    ["defillama"],   # market_data table
}


# ── Core check functions ───────────────────────────────────────────────────────

def _fmt_ts(ts_str: str | None) -> str | None:
    """Return a short relative-time label for an ISO-8601 string."""
    if not ts_str:
        return None
    try:
        dt = datetime.fromisoformat(ts_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        mins = int(
            (datetime.now(timezone.utc) - dt.astimezone(timezone.utc)).total_seconds() / 60
        )
        if mins < 60:
            return f"{mins}m ago"
        if mins < 1440:
            return f"{mins // 60}h ago"
        return f"{mins // 1440}d ago"
    except Exception:
        return ts_str[:19]


def _db_check(conn: sqlite3.Connection, now: datetime) -> dict:
    """Return database-level health metrics."""
    cutoff_24h = (now - timedelta(hours=24)).isoformat()

    total = conn.execute("SELECT COUNT(*) FROM raw_articles").fetchone()[0]
    last_24h = conn.execute(
        "SELECT COUNT(*) FROM raw_articles WHERE collected_at >= ?",
        (cutoff_24h,),
    ).fetchone()[0]
    last_row = conn.execute(
        "SELECT collected_at FROM raw_articles ORDER BY collected_at DESC LIMIT 1"
    ).fetchone()
    last_collected = last_row[0] if last_row else None

    db_size_mb = round(DB_PATH.stat().st_size / 1_048_576, 2) if DB_PATH.exists() else 0

    return {
        "total_articles":    total,
        "articles_last_24h": last_24h,
        "last_collected_at": last_collected,
        "last_collected_ago": _fmt_ts(last_collected),
        "db_size_mb":        db_size_mb,
    }


def _collector_check(conn: sqlite3.Connection, now: datetime) -> dict:
    """Return last-activity timestamps for each collector."""
    result: dict[str, dict] = {}
    cutoff_24h = (now - timedelta(hours=24)).isoformat()

    for name, sources in _COLLECTOR_SOURCES.items():
        table = "market_data" if name in ("coingecko", "defillama") else "raw_articles"
        placeholders = ",".join(["?"] * len(sources))
        try:
            row = conn.execute(
                f"SELECT MAX(collected_at) FROM {table} "
                f"WHERE source IN ({placeholders})",
                sources,
            ).fetchone()
            last_ts  = row[0] if row else None
            count_24 = conn.execute(
                f"SELECT COUNT(*) FROM {table} "
                f"WHERE source IN ({placeholders}) AND collected_at >= ?",
                sources + [cutoff_24h],
            ).fetchone()[0]
        except Exception:
            last_ts  = None
            count_24 = 0

        result[name] = {
            "last_success":     last_ts,
            "last_success_ago": _fmt_ts(last_ts),
            "count_last_24h":   count_24,
        }

    return result


def _llm_check(conn: sqlite3.Connection) -> dict:
    """Return LLM analysis coverage metrics."""
    try:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(raw_articles)")}
        if "llm_analyzed" not in cols:
            return {"coverage_pct": 0, "analyzed": 0, "total": 0, "available": False}
        total    = conn.execute("SELECT COUNT(*) FROM raw_articles").fetchone()[0]
        analyzed = conn.execute(
            "SELECT COUNT(*) FROM raw_articles WHERE llm_analyzed = 1"
        ).fetchone()[0]
        pct = round(analyzed / max(total, 1) * 100, 1)
        return {"coverage_pct": pct, "analyzed": analyzed, "total": total, "available": True}
    except Exception:
        return {"coverage_pct": 0, "analyzed": 0, "total": 0, "available": False}


def _trend_check(conn: sqlite3.Connection) -> dict:
    """Return whether trend_data table exists and has recent entries."""
    try:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(trend_data)")}
        if not cols:
            return {"available": False}
        row = conn.execute(
            "SELECT date FROM trend_data ORDER BY date DESC LIMIT 1"
        ).fetchone()
        return {"available": True, "last_date": row[0] if row else None}
    except Exception:
        return {"available": False}


def _dashboard_check() -> dict:
    """Return whether data.json exists and how old it is."""
    if not JSON_PATH.exists():
        return {"exists": False, "age_minutes": None}
    try:
        mtime = datetime.fromtimestamp(JSON_PATH.stat().st_mtime, tz=timezone.utc)
        age   = int(
            (datetime.now(timezone.utc) - mtime).total_seconds() / 60
        )
        return {"exists": True, "age_minutes": age}
    except Exception:
        return {"exists": True, "age_minutes": None}


# ── Status rollup ──────────────────────────────────────────────────────────────

def _overall_status(db: dict, collectors: dict) -> str:
    """
    Derive an overall status string.

    healthy   – DB accessible, >0 articles in last 24 h
    degraded  – DB accessible but low collection activity
    critical  – DB inaccessible or 0 articles ever
    """
    if db["total_articles"] == 0:
        return "critical"
    if db["articles_last_24h"] == 0:
        return "degraded"
    return "healthy"


# ── Public API ─────────────────────────────────────────────────────────────────

def run_health_check(db_path: Path = DB_PATH) -> dict:
    """
    Perform all health checks and return a unified report dict.

    The report is suitable for JSON serialisation and HTTP responses.
    """
    now = datetime.now(timezone.utc)
    report: dict = {
        "checked_at": now.isoformat(),
        "status":     "critical",
        "database":   {"accessible": False},
        "collectors": {},
        "llm":        {},
        "trends":     {},
        "dashboard":  _dashboard_check(),
        "error":      None,
    }

    if not db_path.exists():
        report["error"] = f"Database not found: {db_path}"
        return report

    try:
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA journal_mode=WAL")

        db_metrics  = _db_check(conn, now)
        collectors  = _collector_check(conn, now)
        llm         = _llm_check(conn)
        trends      = _trend_check(conn)
        conn.close()

        report["database"]   = {"accessible": True, **db_metrics}
        report["collectors"] = collectors
        report["llm"]        = llm
        report["trends"]     = trends
        report["status"]     = _overall_status(db_metrics, collectors)

    except Exception as exc:
        report["error"]  = str(exc)
        report["status"] = "critical"

    return report


# ── CLI entry point ────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="TRON Sentinel health check")
    parser.add_argument("--json", action="store_true", help="Output raw JSON")
    args = parser.parse_args()

    report = run_health_check()
    status = report["status"]

    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
        sys.exit(0 if status == "healthy" else 1)

    # Human-readable output
    icon = {"healthy": "✓", "degraded": "⚠", "critical": "✗"}.get(status, "?")
    print(f"\n{'─' * 54}")
    print(f"  TRON Sentinel  Health Check  [{icon} {status.upper()}]")
    print(f"  {report['checked_at'][:19]} UTC")
    print(f"{'─' * 54}")

    db = report["database"]
    if db.get("accessible"):
        print(f"\n  数据库")
        print(f"    大小         : {db['db_size_mb']} MB")
        print(f"    总文章数     : {db['total_articles']:,}")
        print(f"    近24h新增    : {db['articles_last_24h']:,}")
        print(f"    最近采集     : {db.get('last_collected_ago', '--')}")
    else:
        print(f"\n  数据库  ✗  {report.get('error', '不可访问')}")

    print(f"\n  采集器状态")
    for name, info in report.get("collectors", {}).items():
        cnt = info.get("count_last_24h", 0)
        ago = info.get("last_success_ago") or "从未"
        mark = "✓" if cnt > 0 else "–"
        print(f"    [{mark}] {name:<14}  近24h: {cnt:>4} 条  上次: {ago}")

    llm = report.get("llm", {})
    if llm.get("available"):
        print(f"\n  LLM 分析覆盖: {llm['coverage_pct']}%  ({llm['analyzed']}/{llm['total']} 篇)")
    else:
        print(f"\n  LLM 分析  : 列未就绪")

    dash = report.get("dashboard", {})
    dash_age = dash.get("age_minutes")
    if dash.get("exists"):
        age_str = f"{dash_age} 分钟前" if dash_age is not None else "未知"
        print(f"  Dashboard JSON: 存在，更新于 {age_str}")
    else:
        print(f"  Dashboard JSON: 不存在")

    print(f"\n{'─' * 54}\n")
    sys.exit(0 if status == "healthy" else 1)


if __name__ == "__main__":
    main()

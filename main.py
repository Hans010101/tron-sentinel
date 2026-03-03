"""
main.py
~~~~~~~
TRON Sentinel – three-step data pipeline orchestrator.

Executes in order:
    Step 1  RSS collection       collectors/rss_collector.py
    Step 2  Sentiment analysis   analyzers/sentiment_analyzer.py
    Step 3  Telegram alerting    alerting/telegram_alerter.py

Then queries the populated SQLite database and writes
dashboard/data.json so the live dashboard can display real data.

Usage:
    python main.py
"""

import json
import logging
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ── Paths ──────────────────────────────────────────────────────────────────────

ROOT      = Path(__file__).parent
DB_PATH   = ROOT / "data"      / "sentinel.db"
JSON_PATH = ROOT / "dashboard" / "data.json"

# ── Logging ────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
    datefmt= "%H:%M:%S",
)
logger = logging.getLogger("sentinel.main")

# ── Step runner ────────────────────────────────────────────────────────────────

_SEP  = "─" * 58
_SEP2 = "═" * 58


def run_step(num: int, label: str, fn, *args, **kwargs):
    """
    Call fn(*args, **kwargs), print a progress header and elapsed time.

    Returns ``(return_value, success: bool)``.  Exceptions are caught,
    logged, and reported inline so the pipeline continues with the
    remaining steps.
    """
    print(f"\n{_SEP}")
    print(f"  [{num}/3]  {label}")
    print(_SEP)
    t0 = time.perf_counter()
    try:
        result  = fn(*args, **kwargs)
        elapsed = time.perf_counter() - t0
        print(f"  ✓  完成  ({elapsed:.2f}s)")
        return result, True
    except Exception as exc:
        elapsed = time.perf_counter() - t0
        print(f"  ✗  失败  ({elapsed:.2f}s)")
        print(f"     {type(exc).__name__}: {exc}")
        logger.exception("Step %d failed: %s", num, label)
        return None, False


# ── Step implementations ───────────────────────────────────────────────────────

def do_collect() -> int:
    """Run the RSS collector; returns the count of newly inserted articles."""
    from collectors.rss_collector import init_db, collect_all  # noqa: PLC0415
    conn = init_db(DB_PATH)
    try:
        return collect_all(conn)
    finally:
        conn.close()


def do_analyze() -> int:
    """Run VADER sentiment analysis; returns the count of newly scored rows."""
    from analyzers.sentiment_analyzer import open_db, analyze_pending  # noqa: PLC0415
    conn = open_db(DB_PATH)
    try:
        return analyze_pending(conn)
    finally:
        conn.close()


def do_alert() -> int:
    """Dispatch Telegram alerts; returns the count of messages sent."""
    from alerting.telegram_alerter import (  # noqa: PLC0415
        TelegramAlerter, fetch_negative_articles, open_db as alert_open_db,
    )
    conn = alert_open_db(DB_PATH)
    try:
        articles = fetch_negative_articles(conn, limit=5)
        return TelegramAlerter().send_alerts(articles, delay=0.5)
    finally:
        conn.close()


# ── Dashboard JSON builder ─────────────────────────────────────────────────────

# Language metadata for the bar-chart section.
_LANG_META: dict[str, dict] = {
    "en": {"name": "English",     "code": "EN", "color": "#6366f1"},
    "zh": {"name": "中文",         "code": "ZH", "color": "#10b981"},
    "ja": {"name": "日本語",       "code": "JA", "color": "#f59e0b"},
    "ko": {"name": "한국어",       "code": "KO", "color": "#3b82f6"},
    "vi": {"name": "Tiếng Việt",  "code": "VI", "color": "#ec4899"},
}

_LEVEL_CLASS: dict[str, str] = {
    "CRITICAL": "tag-critical",
    "HIGH":     "tag-high",
    "MEDIUM":   "tag-medium",
    "LOW":      "tag-low",
}


def _time_ago(dt_str: str | None, now: datetime) -> str:
    """Return a short Chinese relative-time string for an ISO-8601 timestamp."""
    if not dt_str:
        return "未知时间"
    try:
        dt = datetime.fromisoformat(dt_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        mins = int((now - dt.astimezone(timezone.utc)).total_seconds() / 60)
        if mins < 1:
            return "刚刚"
        if mins < 60:
            return f"{mins} 分钟前"
        if mins < 1440:
            return f"{mins // 60} 小时前"
        return f"{mins // 1440} 天前"
    except Exception:
        return dt_str[:10] if len(dt_str) >= 10 else "未知时间"


def build_dashboard_json(conn: sqlite3.Connection) -> dict:
    """
    Query raw_articles and return the complete dashboard data payload.

    Schema of the returned dict:
        generated_at     ISO-8601 timestamp
        overview         today_total, positive_pct, negative_pct,
                         neutral_pct, active_alerts
        sentiment_trend  list of 24 hourly dicts {hour, positive,
                         negative, neutral}  – oldest to newest
        alerts           up to 5 most recent negative articles with
                         level/icon/class, title, source, score,
                         link, time_ago
        language_volumes list of {name, code, value, color} dicts
    """
    from alerting.telegram_alerter import (  # noqa: PLC0415
        alert_level, fetch_negative_articles,
    )

    now       = datetime.now(timezone.utc)
    today_pfx = now.strftime("%Y-%m-%d")          # "2025-03-03"
    cutoff_24 = (now - timedelta(hours=24)).isoformat()

    # ── Overview ───────────────────────────────────────────────────────────────
    today_total = conn.execute(
        "SELECT COUNT(*) FROM raw_articles WHERE collected_at LIKE ?",
        (today_pfx + "%",),
    ).fetchone()[0]

    label_rows   = conn.execute(
        "SELECT sentiment_label, COUNT(*) FROM raw_articles "
        "WHERE  sentiment_label IS NOT NULL "
        "GROUP  BY sentiment_label"
    ).fetchall()
    label_counts: dict[str, int] = {r[0]: r[1] for r in label_rows}
    total_lbl    = sum(label_counts.values()) or 1  # guard /0

    def pct(label: str) -> float:
        return round(label_counts.get(label, 0) / total_lbl * 100, 1)

    active_alerts = conn.execute(
        "SELECT COUNT(*) FROM raw_articles "
        "WHERE  sentiment_label = 'negative' AND collected_at LIKE ?",
        (today_pfx + "%",),
    ).fetchone()[0]

    # ── Sentiment trend – last 24 hourly slots ─────────────────────────────────
    # Fetch every relevant row in a single query, then pivot in Python.
    hourly_raw = conn.execute(
        """
        SELECT SUBSTR(collected_at, 1, 13) AS hkey,
               sentiment_label,
               COUNT(*)                    AS cnt
        FROM   raw_articles
        WHERE  collected_at       >= ?
          AND  sentiment_label IS NOT NULL
        GROUP  BY hkey, sentiment_label
        """,
        (cutoff_24,),
    ).fetchall()

    # Nested dict:  "2025-03-03T14" → {"positive": 12, "negative": 3, ...}
    hour_map: dict[str, dict[str, int]] = {}
    for hkey, lbl, cnt in hourly_raw:
        hour_map.setdefault(hkey, {})[lbl] = cnt

    trend: list[dict] = []
    for i in range(23, -1, -1):          # slot 23 = 23 h ago, slot 0 = now
        slot  = now - timedelta(hours=i)
        hkey  = slot.strftime("%Y-%m-%dT%H")
        counts = hour_map.get(hkey, {})
        total_h = sum(counts.values())

        if total_h:
            p = round(counts.get("positive", 0) / total_h * 100, 1)
            n = round(counts.get("negative", 0) / total_h * 100, 1)
            u = round(max(0.0, 100 - p - n), 1)
        else:
            # Empty hour → fall back to the overall label distribution.
            p, n, u = pct("positive"), pct("negative"), pct("neutral")

        trend.append({
            "hour":     slot.strftime("%H:00"),
            "positive": p,
            "negative": n,
            "neutral":  u,
        })

    # ── Alerts – top 5 most recent negative articles ───────────────────────────
    neg_articles = fetch_negative_articles(conn, limit=5)
    alerts_out: list[dict] = []
    for a in neg_articles:
        icon, level = alert_level(a["sentiment_score"])
        alerts_out.append({
            "icon":        icon,
            "level":       level,
            "level_class": _LEVEL_CLASS.get(level, "tag-low"),
            "title":       a["title"],
            "source":      a["source"],
            "score":       round(a["sentiment_score"], 4),
            "link":        a["link"],
            "time_ago":    _time_ago(a.get("published_at"), now),
        })

    # ── Language volumes ───────────────────────────────────────────────────────
    lang_rows = conn.execute(
        "SELECT language, COUNT(*) FROM raw_articles "
        "GROUP  BY language ORDER BY COUNT(*) DESC"
    ).fetchall()
    lang_vols: list[dict] = []
    for lang_code, count in lang_rows:
        meta = _LANG_META.get(
            lang_code,
            {"name": lang_code, "code": lang_code.upper(), "color": "#94a3b8"},
        )
        lang_vols.append({
            "name":  meta["name"],
            "code":  meta["code"],
            "value": count,
            "color": meta["color"],
        })

    return {
        "generated_at":     now.isoformat(),
        "overview": {
            "today_total":   today_total,
            "positive_pct":  pct("positive"),
            "negative_pct":  pct("negative"),
            "neutral_pct":   pct("neutral"),
            "active_alerts": active_alerts,
        },
        "sentiment_trend":  trend,
        "alerts":           alerts_out,
        "language_volumes": lang_vols,
    }


def write_json(data: dict, path: Path = JSON_PATH) -> None:
    """Serialise *data* to *path* as UTF-8 JSON, creating directories as needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("Dashboard JSON written → %s", path.resolve())


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    t_start = time.perf_counter()

    print(f"\n{_SEP2}")
    print("  TRON Sentinel  ·  数据采集与分析流水线")
    print(f"  {datetime.now().strftime('%Y-%m-%d  %H:%M:%S')}")
    print(_SEP2)

    step_ok: dict[str, bool] = {}

    # ── Step 1: RSS collection ─────────────────────────────────────────────────
    val, ok = run_step(1, "RSS 新闻采集", do_collect)
    step_ok["collect"] = ok
    if ok:
        print(f"     新增文章 : {val} 条")

    # ── Step 2: Sentiment analysis ─────────────────────────────────────────────
    if DB_PATH.exists():
        val, ok = run_step(2, "VADER 情绪分析", do_analyze)
        step_ok["analyze"] = ok
        if ok:
            print(f"     本次分析 : {val} 条")
    else:
        print(f"\n  ⚠  数据库未找到，跳过第 2 步（{DB_PATH}）")
        step_ok["analyze"] = False

    # ── Step 3: Telegram alerting ──────────────────────────────────────────────
    if DB_PATH.exists():
        val, ok = run_step(3, "Telegram 预警发送", do_alert)
        step_ok["alert"] = ok
        if ok:
            print(f"     已发送   : {val} 条")
    else:
        print(f"\n  ⚠  数据库未找到，跳过第 3 步（{DB_PATH}）")
        step_ok["alert"] = False

    # ── Dashboard JSON ─────────────────────────────────────────────────────────
    print(f"\n{_SEP}")
    print("  生成 Dashboard 数据 (dashboard/data.json)")
    print(_SEP)
    if DB_PATH.exists():
        t0 = time.perf_counter()
        try:
            conn = sqlite3.connect(DB_PATH)
            data = build_dashboard_json(conn)
            conn.close()
            write_json(data)
            elapsed = time.perf_counter() - t0
            ov = data["overview"]
            print(f"  ✓  完成  ({elapsed:.2f}s)")
            print(f"     今日声量 : {ov['today_total']} 条")
            print(
                f"     情绪分布 : 正面 {ov['positive_pct']}%  "
                f"负面 {ov['negative_pct']}%  "
                f"中性 {ov['neutral_pct']}%"
            )
            print(f"     活跃预警 : {ov['active_alerts']} 条")
            print(f"     预警列表 : {len(data['alerts'])} 条写入 JSON")
        except Exception as exc:
            print(f"  ✗  JSON 生成失败: {exc}")
            logger.exception("JSON generation failed")
    else:
        print("  ⚠  数据库未找到，跳过 JSON 生成")

    # ── Summary ────────────────────────────────────────────────────────────────
    elapsed_total = time.perf_counter() - t_start
    n_ok  = sum(step_ok.values())
    n_all = len(step_ok)
    status = "✓ 全部成功" if n_ok == n_all else f"⚠ {n_ok}/{n_all} 步骤成功"

    print(f"\n{_SEP2}")
    print(f"  {status}  ·  总耗时 {elapsed_total:.1f}s")
    print(_SEP2)

    if JSON_PATH.exists():
        print(f"\n  ▶  查看 Dashboard（需要 HTTP 服务器，因浏览器 fetch 限制）：")
        print(f"     cd {ROOT.resolve()}")
        print(f"     python -m http.server 8080")
        print(f"     → 浏览器访问 http://localhost:8080/dashboard/")
    print()


if __name__ == "__main__":
    main()

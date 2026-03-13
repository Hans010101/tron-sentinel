"""
main.py
~~~~~~~
TRON Sentinel – full data pipeline orchestrator.

Executes in order:
    Step 1   RSS collection          collectors/rss_collector.py
    Step 2   Apify Twitter/X         collectors/apify_collector.py
    Step 3   Apify Google Search     collectors/apify_collector.py
    Step 4   Apify YouTube           collectors/apify_collector.py
    Step 5   Apify Reddit            collectors/apify_collector.py
    Step 6   Apify TikTok            collectors/apify_collector.py
    Step 7   Apify Weibo             collectors/apify_collector.py
    Step 8   Bilibili Search         collectors/bilibili_collector.py
    Step 9   CoinGecko market data   collectors/coingecko_collector.py
    Step 10  DeFiLlama TVL data      collectors/defillama_collector.py
    Step 11  Baidu News RSS          collectors/baidu_collector.py
    Step 12  CryptoPanic API         collectors/crypto_panic_collector.py
    Step 13  Sentiment Analysis      analyzers/sentiment_analyzer.py
    Step 14  LLM Deep Analysis       analyzers/llm_analyzer.py

Then queries the populated SQLite database and writes
dashboard/data.json so the live dashboard can display real data.

Usage:
    python main.py
"""

import json
import logging
import os
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Load .env file for local development (Cloud Run uses Secret Manager)
_env_path = Path(__file__).parent / ".env"
if _env_path.exists():
    _raw = _env_path.read_bytes()
    # Handle UTF-16 LE BOM (common on Windows)
    _text = _raw.decode("utf-16") if _raw[:2] == b"\xff\xfe" else _raw.decode("utf-8-sig")
    for line in _text.splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, val = line.partition("=")
            os.environ.setdefault(key.strip(), val.strip())

# ── Paths ──────────────────────────────────────────────────────────────────────
# In the Cloud Run container __file__ is /app/main.py, so:
#   ROOT      → /app
#   DB_PATH   → /app/data/sentinel.db
#   JSON_PATH → /app/dashboard/data.json   (served by entrypoint.py /data.json)

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

_SEP        = "─" * 58
_SEP2       = "═" * 58
_TOTAL_STEPS = 15         # update when adding / removing pipeline steps


def run_step(num: int, label: str, fn, *args, **kwargs):
    """
    Call fn(*args, **kwargs), print a progress header and elapsed time.

    Returns ``(return_value, success: bool)``.  Exceptions are caught,
    logged, and reported inline so the pipeline continues with the
    remaining steps.
    """
    print(f"\n{_SEP}")
    print(f"  [{num}/{_TOTAL_STEPS}]  {label}")
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

def do_collect_rss() -> int:
    """RSS collector → returns count of newly inserted articles."""
    from collectors.rss_collector import init_db, collect_all  # noqa: PLC0415
    conn = init_db(DB_PATH)
    try:
        return collect_all(conn)
    finally:
        conn.close()


def do_collect_apify_twitter() -> int:
    """Apify Twitter/X collector → returns count of newly inserted tweets."""
    from collectors.apify_collector import open_db, collect_twitter  # noqa: PLC0415
    conn = open_db(DB_PATH)
    try:
        return collect_twitter(conn)
    finally:
        conn.close()


def do_collect_apify_google() -> int:
    """Apify Google Search News collector → returns count of newly inserted articles."""
    from collectors.apify_collector import open_db, collect_google_news  # noqa: PLC0415
    conn = open_db(DB_PATH)
    try:
        return collect_google_news(conn)
    finally:
        conn.close()


def do_collect_apify_youtube() -> int:
    """Apify YouTube collector → returns count of newly inserted videos."""
    from collectors.apify_collector import open_db, collect_youtube  # noqa: PLC0415
    conn = open_db(DB_PATH)
    try:
        return collect_youtube(conn)
    finally:
        conn.close()


def do_collect_apify_reddit() -> int:
    """Apify Reddit collector → returns count of newly inserted posts."""
    from collectors.apify_collector import open_db, collect_reddit  # noqa: PLC0415
    conn = open_db(DB_PATH)
    try:
        return collect_reddit(conn)
    finally:
        conn.close()


def do_collect_apify_tiktok() -> int:
    """Apify TikTok collector → returns count of newly inserted videos."""
    from collectors.apify_collector import open_db, collect_tiktok  # noqa: PLC0415
    conn = open_db(DB_PATH)
    try:
        return collect_tiktok(conn)
    finally:
        conn.close()


def do_collect_apify_weibo() -> int:
    """Apify Weibo collector → returns count of newly inserted posts."""
    from collectors.apify_collector import open_db, collect_weibo  # noqa: PLC0415
    conn = open_db(DB_PATH)
    try:
        return collect_weibo(conn)
    finally:
        conn.close()


def do_collect_bilibili() -> int:
    """Bilibili search collector → returns count of newly inserted videos."""
    from collectors.bilibili_collector import open_db, collect_bilibili  # noqa: PLC0415
    conn = open_db(DB_PATH)
    try:
        return collect_bilibili(conn)
    finally:
        conn.close()


def do_collect_coingecko() -> bool:
    """CoinGecko market-data collector → returns True on success."""
    from collectors.coingecko_collector import open_db, collect  # noqa: PLC0415
    conn = open_db(DB_PATH)
    try:
        return collect(conn)
    finally:
        conn.close()


def do_collect_defillama() -> bool:
    """DeFiLlama TVL collector → returns True on success."""
    from collectors.defillama_collector import open_db, collect  # noqa: PLC0415
    conn = open_db(DB_PATH)
    try:
        return collect(conn)
    finally:
        conn.close()


def do_collect_baidu() -> int:
    """Baidu News RSS collector → returns count of newly inserted articles."""
    from collectors.baidu_collector import open_db, collect_baidu  # noqa: PLC0415
    conn = open_db(DB_PATH)
    try:
        return collect_baidu(conn)
    finally:
        conn.close()


def do_collect_crypto_panic() -> int:
    """CryptoPanic API collector → returns count of newly inserted articles."""
    from collectors.crypto_panic_collector import open_db, collect_crypto_panic  # noqa: PLC0415
    conn = open_db(DB_PATH)
    try:
        return collect_crypto_panic(conn)
    finally:
        conn.close()


def do_analyze_sentiment() -> int:
    """Keyword-based sentiment analyzer → returns count of articles analyzed."""
    from analyzers.sentiment_analyzer import open_db, analyze_sentiment  # noqa: PLC0415
    conn = open_db(DB_PATH)
    try:
        return analyze_sentiment(conn)
    finally:
        conn.close()


def do_llm_analyze() -> int:
    """DashScope LLM analyzer → returns count of articles analysed."""
    api_key = os.environ.get("DASHSCOPE_API_KEY", "").strip()
    if not api_key:
        logger.info("DASHSCOPE_API_KEY not set – skipping LLM analysis.")
        return 0
    from analyzers.llm_analyzer import analyze_articles  # noqa: PLC0415
    return analyze_articles(DB_PATH)


def do_send_daily_reports() -> int:
    """Generate and send daily reports via Feishu webhook → returns count sent."""
    webhook_url = os.environ.get("FEISHU_WEBHOOK_URL", "").strip()
    if not webhook_url:
        logger.info("FEISHU_WEBHOOK_URL not set – skipping daily reports.")
        return 0
    from reporters.daily_report import generate_and_send_all_reports  # noqa: PLC0415
    return generate_and_send_all_reports(webhook_url)


# ── Dashboard JSON builder ─────────────────────────────────────────────────────

_LANG_META: dict[str, dict] = {
    "en": {"name": "English",    "code": "EN", "color": "#6366f1"},
    "zh": {"name": "中文",        "code": "ZH", "color": "#10b981"},
    "ja": {"name": "日本語",      "code": "JA", "color": "#f59e0b"},
    "ko": {"name": "한국어",      "code": "KO", "color": "#3b82f6"},
    "vi": {"name": "Tiếng Việt", "code": "VI", "color": "#ec4899"},
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


def _latest_market(conn: sqlite3.Connection, source: str) -> dict:
    """Return the most-recently collected market_data row for *source*."""
    row = conn.execute(
        "SELECT price_usd, change_24h, market_cap, volume_24h, tvl, "
        "       collected_at, extra_json "
        "FROM market_data WHERE source = ? "
        "ORDER BY collected_at DESC LIMIT 1",
        (source,),
    ).fetchone()
    if not row:
        return {}
    price, change, mcap, vol, tvl, ts, extra_json = row
    return {
        "price_usd":    price,
        "change_24h":   change,
        "market_cap":   mcap,
        "volume_24h":   vol,
        "tvl":          tvl,
        "collected_at": ts,
        "extra_json":   extra_json,
    }


def _ensure_sentiment_columns(conn: sqlite3.Connection) -> None:
    """Add sentiment_label and sentiment_score columns if they don't exist yet."""
    existing = {row[1] for row in conn.execute("PRAGMA table_info(raw_articles)")}
    if "sentiment_label" not in existing:
        conn.execute("ALTER TABLE raw_articles ADD COLUMN sentiment_label TEXT")
    if "sentiment_score" not in existing:
        conn.execute("ALTER TABLE raw_articles ADD COLUMN sentiment_score REAL")
    conn.commit()


def build_dashboard_json(conn: sqlite3.Connection) -> dict:
    """
    Query raw_articles and market_data and return the full dashboard payload.

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
        market           TRX price, change, market_cap, volume (CoinGecko)
                         + tvl (DeFiLlama)
    """
    now       = datetime.now(timezone.utc)
    today_pfx = now.strftime("%Y-%m-%d")
    cutoff_24 = (now - timedelta(hours=24)).isoformat()

    # Ensure sentiment columns exist (added by sentiment analyzer when it runs)
    _ensure_sentiment_columns(conn)

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
    total_lbl    = sum(label_counts.values()) or 1

    def pct(label: str) -> float:
        return round(label_counts.get(label, 0) / total_lbl * 100, 1)

    active_alerts = conn.execute(
        "SELECT COUNT(*) FROM raw_articles "
        "WHERE  sentiment_label = 'negative' AND collected_at LIKE ?",
        (today_pfx + "%",),
    ).fetchone()[0]

    # ── Sentiment trend – last 24 hourly slots ─────────────────────────────────
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

    hour_map: dict[str, dict[str, int]] = {}
    for hkey, lbl, cnt in hourly_raw:
        hour_map.setdefault(hkey, {})[lbl] = cnt

    trend: list[dict] = []
    for i in range(23, -1, -1):
        slot   = now - timedelta(hours=i)
        hkey   = slot.strftime("%Y-%m-%dT%H")
        counts = hour_map.get(hkey, {})
        total_h = sum(counts.values())
        if total_h:
            p = round(counts.get("positive", 0) / total_h * 100, 1)
            n = round(counts.get("negative", 0) / total_h * 100, 1)
            u = round(max(0.0, 100 - p - n), 1)
        else:
            p, n, u = pct("positive"), pct("negative"), pct("neutral")
        trend.append({"hour": slot.strftime("%H:00"), "positive": p,
                      "negative": n, "neutral": u})

    # ── Alerts – top 5 most recent articles (by recency) ──────────────────────
    alert_rows = conn.execute(
        "SELECT title, source, link, published_at, sentiment_score "
        "FROM raw_articles "
        "ORDER BY collected_at DESC LIMIT 5"
    ).fetchall()
    alerts_out: list[dict] = []
    for title, source, link, pub_at, score in alert_rows:
        score = score or 0.0
        if score < -0.5:
            icon, level = "!!!", "CRITICAL"
        elif score < -0.2:
            icon, level = "!!", "HIGH"
        elif score < 0:
            icon, level = "!", "MEDIUM"
        else:
            icon, level = "i", "LOW"
        alerts_out.append({
            "icon":        icon,
            "level":       level,
            "level_class": _LEVEL_CLASS.get(level, "tag-low"),
            "title":       title,
            "source":      source,
            "score":       round(score, 4),
            "link":        link,
            "time_ago":    _time_ago(pub_at, now),
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
        lang_vols.append({"name": meta["name"], "code": meta["code"],
                          "value": count,       "color": meta["color"]})

    # ── Market data (CoinGecko + DeFiLlama) ────────────────────────────────────
    cg = _latest_market(conn, "coingecko")
    dl = _latest_market(conn, "defillama")

    # Extract 7-day price history stored in CoinGecko's extra_json field.
    price_history: list = []
    cg_extra_raw = cg.get("extra_json")
    if cg_extra_raw:
        try:
            cg_extra = json.loads(cg_extra_raw)
            price_history = cg_extra.get("price_history", [])
        except Exception:
            pass

    market: dict = {
        "price_usd":      cg.get("price_usd"),
        "change_24h":     cg.get("change_24h"),
        "market_cap":     cg.get("market_cap"),
        "volume_24h":     cg.get("volume_24h"),
        "tvl":            dl.get("tvl"),
        "coingecko_at":   cg.get("collected_at"),
        "defillama_at":   dl.get("collected_at"),
        "price_history":  price_history,
    }

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
        "market":           market,
    }


def write_json(data: dict, path: Path = JSON_PATH) -> None:
    """
    Serialise *data* to *path* as UTF-8 JSON, creating directories as needed.

    When the environment variable ``GCS_BUCKET`` is set (injected by Cloud Run
    via deploy.sh), also uploads the JSON to that GCS bucket as ``data.json``
    so that the Cloud Storage–hosted dashboard can fetch real data.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    json_str = json.dumps(data, ensure_ascii=False, indent=2)
    path.write_text(json_str, encoding="utf-8")
    logger.info("Dashboard JSON written → %s", path.resolve())

    # ── Optional GCS upload (active when running on Cloud Run) ────────────────
    bucket_name = os.environ.get("GCS_BUCKET", "").strip()
    if not bucket_name:
        return
    try:
        from google.cloud import storage as gcs  # noqa: PLC0415
        client = gcs.Client()
        blob   = client.bucket(bucket_name).blob("data.json")
        blob.upload_from_string(
            json_str.encode("utf-8"),
            content_type="application/json",
        )
        # Make the object publicly readable so the browser can fetch it.
        blob.make_public()
        logger.info("Dashboard JSON uploaded → gs://%s/data.json", bucket_name)
    except Exception as exc:
        logger.warning(
            "GCS upload skipped (bucket=%s): %s. "
            "Local dashboard/data.json is still available.",
            bucket_name, exc,
        )


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    t_start = time.perf_counter()

    print(f"\n{_SEP2}")
    print("  TRON Sentinel  ·  数据采集与分析流水线")
    print(f"  {datetime.now().strftime('%Y-%m-%d  %H:%M:%S')}")
    print(_SEP2)

    step_ok: dict[str, bool] = {}

    # ── Step 1: RSS collection ─────────────────────────────────────────────────
    val, ok = run_step(1, "RSS 新闻采集（24源：CoinDesk / TheBlock / Blockworks / 金色财经 / PANews 等）",
                       do_collect_rss)
    step_ok["rss"] = ok
    if ok:
        print(f"     新增文章 : {val} 条")

    # ── Step 2: Apify Twitter/X ────────────────────────────────────────────────
    val, ok = run_step(2, "Apify Twitter/X 采集（Justin Sun / 孙宇晨 / TRON）",
                       do_collect_apify_twitter)
    step_ok["apify_twitter"] = ok
    if ok:
        print(f"     新增推文 : {val} 条")

    # ── Step 3: Apify Google Search News ───────────────────────────────────────
    val, ok = run_step(3, "Apify Google 新闻采集（Justin Sun / TRON / 波场）",
                       do_collect_apify_google)
    step_ok["apify_google"] = ok
    if ok:
        print(f"     新增文章 : {val} 条")

    # ── Step 4: Apify YouTube ──────────────────────────────────────────────────
    val, ok = run_step(4, "Apify YouTube 视频采集（Justin Sun / TRON / 波场）",
                       do_collect_apify_youtube)
    step_ok["apify_youtube"] = ok
    if ok:
        print(f"     新增视频 : {val} 条")

    # ── Step 5: Apify Reddit ───────────────────────────────────────────────────
    val, ok = run_step(5, "Apify Reddit 帖子采集（TRON TRX / Justin Sun）",
                       do_collect_apify_reddit)
    step_ok["apify_reddit"] = ok
    if ok:
        print(f"     新增帖子 : {val} 条")

    # ── Step 6: Apify TikTok ──────────────────────────────────────────────────
    val, ok = run_step(6, "Apify TikTok 视频采集（TRON / Justin Sun / 波场）",
                       do_collect_apify_tiktok)
    step_ok["apify_tiktok"] = ok
    if ok:
        print(f"     新增视频 : {val} 条")

    # ── Step 7: Apify Weibo ───────────────────────────────────────────────────
    val, ok = run_step(7, "Apify 微博采集（波场 / 孙宇晨 相关微博）",
                       do_collect_apify_weibo)
    step_ok["apify_weibo"] = ok
    if ok:
        print(f"     新增微博 : {val} 条")

    # ── Step 8: Bilibili ──────────────────────────────────────────────────────
    val, ok = run_step(8, "Bilibili 视频搜索（孙宇晨 / 波场TRON）",
                       do_collect_bilibili)
    step_ok["bilibili"] = ok
    if ok:
        print(f"     新增视频 : {val} 条")

    # ── Step 9: CoinGecko market data ─────────────────────────────────────────
    val, ok = run_step(9, "CoinGecko TRX 市场数据", do_collect_coingecko)
    step_ok["coingecko"] = ok
    if ok and val:
        try:
            conn_tmp = sqlite3.connect(DB_PATH)
            row = conn_tmp.execute(
                "SELECT price_usd, change_24h FROM market_data "
                "WHERE source='coingecko' ORDER BY collected_at DESC LIMIT 1"
            ).fetchone()
            conn_tmp.close()
            if row:
                price, change = row
                sign = "+" if (change or 0) >= 0 else ""
                print(f"     TRX 价格 : ${price:.5f}  ({sign}{change:.2f}%)")
        except Exception:
            pass

    # ── Step 10: DeFiLlama TVL ────────────────────────────────────────────────
    val, ok = run_step(10, "DeFiLlama TRON TVL 数据", do_collect_defillama)
    step_ok["defillama"] = ok
    if ok and val:
        try:
            conn_tmp = sqlite3.connect(DB_PATH)
            row = conn_tmp.execute(
                "SELECT tvl FROM market_data "
                "WHERE source='defillama' ORDER BY collected_at DESC LIMIT 1"
            ).fetchone()
            conn_tmp.close()
            if row:
                print(f"     TRON TVL : ${row[0] / 1e9:.2f}B")
        except Exception:
            pass

    # ── Step 11: Baidu News ─────────────────────────────────────────────────
    val, ok = run_step(11, "百度新闻采集（波场 / 孙宇晨）",
                       do_collect_baidu)
    step_ok["baidu"] = ok
    if ok:
        print(f"     新增文章 : {val} 条")

    # ── Step 12: CryptoPanic ────────────────────────────────────────────────
    val, ok = run_step(12, "CryptoPanic TRX 新闻采集",
                       do_collect_crypto_panic)
    step_ok["crypto_panic"] = ok
    if ok:
        print(f"     新增文章 : {val} 条")

    # ── Step 13: Sentiment Analysis ───────────────────────────────────────
    val, ok = run_step(13, "情绪分析（关键词分类）",
                       do_analyze_sentiment)
    step_ok["sentiment"] = ok
    if ok:
        print(f"     已分析   : {val} 条")

    # ── Step 14: LLM Deep Analysis (Deepseek) ─────────────────────────────
    val, ok = run_step(14, "LLM 深度分析（Deepseek API）",
                       do_llm_analyze)
    step_ok["llm_analyze"] = ok
    if ok:
        print(f"     已分析   : {val} 篇")

    # ── Step 15: Daily Reports (Feishu Webhook) ───────────────────────────
    val, ok = run_step(15, "生成并发送日报（飞书 Webhook）",
                       do_send_daily_reports)
    step_ok["daily_reports"] = ok
    if ok:
        print(f"     已发送   : {val}/3 份日报")

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
            mk = data.get("market", {})
            print(f"  ✓  完成  ({elapsed:.2f}s)")
            print(f"     今日声量 : {ov['today_total']} 条")
            print(
                f"     情绪分布 : 正面 {ov['positive_pct']}%  "
                f"负面 {ov['negative_pct']}%  "
                f"中性 {ov['neutral_pct']}%"
            )
            print(f"     活跃预警 : {ov['active_alerts']} 条")
            print(f"     预警列表 : {len(data['alerts'])} 条写入 JSON")
            if mk.get("price_usd"):
                print(f"     TRX 价格 : ${mk['price_usd']:.5f}")
            if mk.get("tvl"):
                print(f"     TRON TVL : ${mk['tvl'] / 1e9:.2f}B")
        except Exception as exc:
            print(f"  ✗  JSON 生成失败: {exc}")
            logger.exception("JSON generation failed")
    else:
        print("  ⚠  数据库未找到，跳过 JSON 生成")

    # ── Summary ────────────────────────────────────────────────────────────────
    elapsed_total = time.perf_counter() - t_start
    n_ok   = sum(step_ok.values())
    n_all  = len(step_ok)
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

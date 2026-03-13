"""
reporters/daily_report.py
~~~~~~~~~~~~~~~~~~~~~~~~~
Daily report generator for TRON Sentinel.

Produces three report types from the past 24 hours of data:
    1. AI 资讯日报  – News / video articles grouped by source
    2. AI 热点日报  – Platform highlights with engagement data
    3. 风险预警日报  – Negative-sentiment articles ranked by score

Each report is rendered as a Markdown string suitable for Feishu cards.

Usage:
    python -m reporters.daily_report          # print reports to stdout
    python -m reporters.daily_report --send   # also send via Feishu webhook
"""

import logging
import re
import sqlite3
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent.parent / "data" / "sentinel.db"

# ── Source classification ────────────────────────────────────────────────────

_STAR_SOURCES = {
    "GoogleNews_JustinSun", "apify_twitter", "apify_weibo",
}
_NEWS_SOURCES = {
    "CoinDesk", "CoinTelegraph", "Decrypt", "TheBlock", "Blockworks",
    "BitcoinMagazine", "DLNews", "Protos", "TheDefiant",
    "BlockBeats", "JinSeCaiJing", "PANews", "ShenChaoTechFlow",
    "Bitpush", "8BTC", "BlockTempo",
    "GoogleNews_TRON", "Reuters_Tech", "BBC_Crypto", "Guardian_Crypto",
    "SCMP_Crypto", "Forbes_Crypto",
    "baidu_news", "crypto_panic",
    "apify_google",
}
_VIDEO_SOURCES = {
    "apify_youtube", "apify_tiktok", "bilibili",
}
_SOCIAL_SOURCES = {
    "apify_twitter", "apify_reddit", "apify_weibo",
    "apify_tiktok",
}

_SOURCE_EMOJI: dict[str, str] = {
    "apify_twitter":  "🐦",
    "apify_youtube":  "📺",
    "apify_reddit":   "💬",
    "apify_tiktok":   "🎵",
    "apify_weibo":    "🌐",
    "apify_google":   "🔍",
    "bilibili":       "📺",
    "baidu_news":     "📰",
    "crypto_panic":   "🔔",
}

# ── Helpers ──────────────────────────────────────────────────────────────────


def _open_db(db_path: Path = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _cutoff_iso() -> str:
    """Return ISO-8601 timestamp for 24 hours ago (UTC)."""
    return (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()


def _today_str() -> str:
    """Return today's date as YYYY-MM-DD in UTC+8."""
    utc8 = timezone(timedelta(hours=8))
    return datetime.now(utc8).strftime("%Y-%m-%d")


def _extract_engagement(summary: str) -> dict[str, int]:
    """
    Parse engagement metrics from the summary field.

    Collectors embed metrics like "Likes: 42 | Retweets: 5" or
    "Score: 128 | Comments: 34" in the summary text.
    """
    metrics: dict[str, int] = {}
    patterns = {
        "likes":    r"Likes?:\s*(\d+)",
        "retweets": r"Retweets?:\s*(\d+)",
        "comments": r"Comments?:\s*(\d+)",
        "views":    r"Views?:\s*(\d+)",
        "plays":    r"Plays?:\s*(\d+)",
        "score":    r"Score:\s*(\d+)",
        "danmaku":  r"Danmaku:\s*(\d+)",
        "reposts":  r"Reposts?:\s*(\d+)",
    }
    for key, pat in patterns.items():
        m = re.search(pat, summary or "", re.IGNORECASE)
        if m:
            metrics[key] = int(m.group(1))
    return metrics


def _engagement_str(metrics: dict[str, int]) -> str:
    """Format engagement metrics as a short inline string."""
    parts: list[str] = []
    if "likes" in metrics:
        parts.append(f"👍{metrics['likes']}")
    if "retweets" in metrics:
        parts.append(f"🔁{metrics['retweets']}")
    if "reposts" in metrics:
        parts.append(f"🔁{metrics['reposts']}")
    if "comments" in metrics:
        parts.append(f"💬{metrics['comments']}")
    if "views" in metrics:
        parts.append(f"👁{metrics['views']}")
    if "plays" in metrics:
        parts.append(f"▶{metrics['plays']}")
    if "score" in metrics:
        parts.append(f"⬆{metrics['score']}")
    if "danmaku" in metrics:
        parts.append(f"弹{metrics['danmaku']}")
    return " ".join(parts)


def _truncate(text: str, max_len: int = 60) -> str:
    return text if len(text) <= max_len else text[: max_len - 1] + "…"


# ── Report 1: AI 资讯日报 ────────────────────────────────────────────────────


def generate_news_report(conn: sqlite3.Connection) -> str:
    """
    AI 资讯日报 – News and video articles grouped by category.

    Sections:
        🌟 明星公司动态  (Justin Sun / TRON official sources)
        📰 新闻汇总      (crypto media & mainstream media)
        🕶 大佬动态      (social media posts from key figures)
        🌊 YouTube更新   (YouTube / Bilibili / TikTok videos)
    """
    cutoff = _cutoff_iso()
    rows = conn.execute(
        "SELECT title, link, source, summary, published_at "
        "FROM raw_articles WHERE collected_at >= ? "
        "ORDER BY published_at DESC",
        (cutoff,),
    ).fetchall()

    star: dict[str, list] = defaultdict(list)
    news: dict[str, list] = defaultdict(list)
    kol:  dict[str, list] = defaultdict(list)
    video: dict[str, list] = defaultdict(list)

    for r in rows:
        src = r["source"]
        item = dict(r)
        if src in _STAR_SOURCES:
            star[src].append(item)
        elif src in _VIDEO_SOURCES:
            video[src].append(item)
        elif src in _SOCIAL_SOURCES:
            kol[src].append(item)
        elif src in _NEWS_SOURCES:
            news[src].append(item)
        else:
            news[src].append(item)

    lines: list[str] = [
        f"# 📋 AI 资讯日报  {_today_str()}",
        f"过去 24 小时共收录 **{len(rows)}** 条资讯",
        "",
    ]

    def _render_group(title: str, groups: dict[str, list]) -> None:
        if not groups:
            return
        lines.append(f"## {title}")
        lines.append("")
        for src, items in sorted(groups.items()):
            emoji = _SOURCE_EMOJI.get(src, "📌")
            lines.append(f"**{emoji} {src}** ({len(items)}条)")
            for it in items[:8]:
                t = _truncate(it["title"])
                link = it["link"]
                lines.append(f"  - [{t}]({link})")
            if len(items) > 8:
                lines.append(f"  - …及其他 {len(items) - 8} 条")
            lines.append("")

    _render_group("🌟 明星公司动态", star)
    _render_group("📰 新闻汇总", news)
    _render_group("🕶 大佬动态", kol)
    _render_group("🌊 视频更新", video)

    if not rows:
        lines.append("暂无数据")

    return "\n".join(lines)


# ── Report 2: AI 热点日报 ────────────────────────────────────────────────────


def generate_hotspot_report(conn: sqlite3.Connection) -> str:
    """
    AI 热点日报 – Platform highlights with engagement metrics.

    Sections per platform: Reddit / YouTube / Twitter / TikTok / Weibo / Bilibili
    """
    cutoff = _cutoff_iso()

    platform_map: dict[str, tuple[str, str]] = {
        "apify_reddit":  ("🔥 Reddit 热点",   "reddit"),
        "apify_youtube": ("🔥 YouTube 热点",  "youtube"),
        "apify_twitter": ("🔥 Twitter 热点",  "twitter"),
        "apify_tiktok":  ("🎵 TikTok 热点",   "tiktok"),
        "apify_weibo":   ("🌐 微博热点",      "weibo"),
        "bilibili":      ("📺 Bilibili 热点", "bilibili"),
    }

    lines: list[str] = [
        f"# 🔥 AI 热点日报  {_today_str()}",
        "",
    ]

    total_items = 0

    for source, (section_title, _) in platform_map.items():
        rows = conn.execute(
            "SELECT title, link, summary, published_at "
            "FROM raw_articles WHERE source = ? AND collected_at >= ? "
            "ORDER BY published_at DESC",
            (source, cutoff),
        ).fetchall()

        if not rows:
            continue

        total_items += len(rows)
        lines.append(f"## {section_title} ({len(rows)}条)")
        lines.append("")

        for r in rows[:10]:
            t = _truncate(r["title"])
            engagement = _extract_engagement(r["summary"])
            eng_str = _engagement_str(engagement)
            link = r["link"]
            if eng_str:
                lines.append(f"- [{t}]({link})")
                lines.append(f"  {eng_str}")
            else:
                lines.append(f"- [{t}]({link})")

        if len(rows) > 10:
            lines.append(f"- …及其他 {len(rows) - 10} 条")
        lines.append("")

    if total_items == 0:
        lines.append("暂无社交媒体热点数据")

    lines.insert(1, f"过去 24 小时各平台共 **{total_items}** 条热点")

    return "\n".join(lines)


# ── Report 3: 风险预警日报 ───────────────────────────────────────────────────


def generate_risk_report(conn: sqlite3.Connection) -> str:
    """
    风险预警日报 – Negative-sentiment articles sorted by score (most negative first).
    """
    cutoff = _cutoff_iso()

    # Check if sentiment columns exist
    cols = {row[1] for row in conn.execute("PRAGMA table_info(raw_articles)")}
    if "sentiment_label" not in cols:
        return (
            f"# ⚠️ 风险预警日报  {_today_str()}\n\n"
            "情绪分析尚未运行，暂无风险预警数据。"
        )

    rows = conn.execute(
        "SELECT title, link, source, sentiment_score, published_at "
        "FROM raw_articles "
        "WHERE sentiment_label = 'negative' AND collected_at >= ? "
        "ORDER BY sentiment_score ASC "
        "LIMIT 30",
        (cutoff,),
    ).fetchall()

    lines: list[str] = [
        f"# ⚠️ 风险预警日报  {_today_str()}",
        f"过去 24 小时共检测到 **{len(rows)}** 条负面舆情",
        "",
    ]

    if not rows:
        lines.append("✅ 过去 24 小时无负面舆情，一切正常！")
        return "\n".join(lines)

    # Severity grouping
    critical: list = []  # score < -0.5
    high: list = []      # score < -0.2
    medium: list = []    # score < 0

    for r in rows:
        score = r["sentiment_score"] or 0.0
        if score < -0.5:
            critical.append(r)
        elif score < -0.2:
            high.append(r)
        else:
            medium.append(r)

    def _render_risk_group(label: str, items: list) -> None:
        if not items:
            return
        lines.append(f"## {label} ({len(items)}条)")
        lines.append("")
        for r in items:
            t = _truncate(r["title"], 55)
            score = r["sentiment_score"] or 0.0
            src = r["source"]
            link = r["link"]
            emoji = _SOURCE_EMOJI.get(src, "📌")
            lines.append(f"- {emoji} [{t}]({link})")
            lines.append(f"  来源: {src} | 情绪分: {score:.2f}")
        lines.append("")

    _render_risk_group("🚨 严重风险", critical)
    _render_risk_group("⚠️ 高风险", high)
    _render_risk_group("💡 中等风险", medium)

    return "\n".join(lines)


# ── Combined send ────────────────────────────────────────────────────────────


def generate_and_send_all_reports(webhook_url: Optional[str] = None) -> int:
    """
    Generate all three daily reports and send them via Feishu webhook.

    Args:
        webhook_url: Feishu webhook URL.  Falls back to FEISHU_WEBHOOK_URL env.

    Returns:
        Number of reports successfully sent (0-3).
    """
    from alerting.webhook_notifier import send_feishu_webhook  # noqa: PLC0415

    if not DB_PATH.exists():
        logger.warning("Database not found at %s – skipping daily reports.", DB_PATH)
        return 0

    conn = _open_db()
    sent = 0

    try:
        reports = [
            ("📋 TRON Sentinel AI 资讯日报", generate_news_report(conn), "blue"),
            ("🔥 TRON Sentinel AI 热点日报", generate_hotspot_report(conn), "orange"),
            ("⚠️ TRON Sentinel 风险预警日报", generate_risk_report(conn), "red"),
        ]

        for title, content, color in reports:
            logger.info("Generating report: %s", title)
            ok = send_feishu_webhook(webhook_url, title, content,
                                     header_color=color)
            if ok:
                sent += 1
                logger.info("Report sent: %s", title)
            else:
                logger.warning("Report send failed or skipped: %s", title)

    finally:
        conn.close()

    logger.info("Daily reports done: %d/3 sent.", sent)
    return sent


# ── Entry point ──────────────────────────────────────────────────────────────


def main() -> None:
    """Print all three reports to stdout; optionally send via --send flag."""
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
    )

    if not DB_PATH.exists():
        print(f"Database not found: {DB_PATH.resolve()}")
        return

    conn = _open_db()
    try:
        sep = "=" * 60

        print(f"\n{sep}")
        report1 = generate_news_report(conn)
        print(report1)

        print(f"\n{sep}")
        report2 = generate_hotspot_report(conn)
        print(report2)

        print(f"\n{sep}")
        report3 = generate_risk_report(conn)
        print(report3)

        print(f"\n{sep}")

        if "--send" in sys.argv:
            print("\nSending reports via Feishu webhook...")
            conn.close()
            sent = generate_and_send_all_reports()
            print(f"Done: {sent}/3 reports sent.")
            return

    finally:
        conn.close()


if __name__ == "__main__":
    main()

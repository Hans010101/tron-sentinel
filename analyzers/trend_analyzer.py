"""
analyzers/trend_analyzer.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~
Trend analysis for TRON Sentinel.

Analyzes 7-day sentiment and volume trends, extracts hot keywords from
the last 24 hours, detects volume/sentiment anomalies, and writes all
results to the ``trend_data`` table in the SQLite database.

Usage:
    from analyzers.trend_analyzer import analyze_trends
    result = analyze_trends(db_path)
"""

import json
import logging
import re
import sqlite3
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

# ── Stop words ────────────────────────────────────────────────────────────────

_EN_STOP: set[str] = {
    "the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for",
    "of", "with", "by", "from", "as", "is", "was", "are", "were", "be",
    "been", "has", "have", "had", "will", "would", "could", "should",
    "may", "might", "do", "does", "did", "not", "this", "that", "it",
    "its", "he", "she", "they", "we", "you", "me", "him", "her",
    "us", "them", "what", "which", "who", "how", "when", "where", "why",
    "new", "says", "said", "can", "more", "than", "up", "over", "after",
    "about", "into", "also", "just", "if", "their", "all", "one", "two",
    "three", "our", "out", "no", "so", "my", "your", "too", "now", "then",
    "here", "there", "via", "per", "vs", "re", "yet", "get", "got", "use",
    "used", "say", "make", "made", "first", "last", "now", "see", "way",
    "take", "time", "year", "day", "week", "amid", "back", "still",
}

_ZH_STOP: set[str] = {
    "的", "了", "和", "是", "在", "有", "与", "对", "中", "为", "以", "及",
    "等", "将", "被", "从", "由", "但", "也", "不", "这", "那", "一", "之",
    "月", "日", "年", "个", "来", "去", "上", "下", "出", "到", "就", "都",
    "而", "后", "其", "他", "她", "们", "我", "你", "此", "该", "已",
}

_STOP_WORDS: set[str] = _EN_STOP | _ZH_STOP

# ── Platform source mapping (mirrors main.py) ─────────────────────────────────

_PLATFORM_SOURCES: dict[str, list[str]] = {
    "Twitter/X": ["apify_twitter"],
    "Reddit":    ["apify_reddit"],
    "YouTube":   ["apify_youtube"],
    "TikTok":    ["apify_tiktok"],
    "微博":      ["apify_weibo"],
    "B站":       ["bilibili"],
    "RSS新闻":   [
        "CoinDesk", "CoinTelegraph", "Decrypt", "TheBlock", "Blockworks",
        "BitcoinMagazine", "DLNews", "Protos", "TheDefiant",
        "BlockBeats", "JinSeCaiJing", "PANews", "ShenChaoTechFlow",
        "Bitpush", "8BTC", "BlockTempo", "GoogleNews_TRON",
        "GoogleNews_JustinSun", "Reuters_Tech", "BBC_Crypto",
        "Guardian_Crypto", "SCMP_Crypto", "Forbes_Crypto",
        "baidu_news", "crypto_panic", "apify_google",
    ],
}

# ── Schema ────────────────────────────────────────────────────────────────────

_CREATE_TREND_TABLE = """
CREATE TABLE IF NOT EXISTS trend_data (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    date              TEXT NOT NULL,
    metric_name       TEXT NOT NULL,
    metric_value_json TEXT NOT NULL,
    UNIQUE(date, metric_name)
)
"""


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(_CREATE_TREND_TABLE)
    conn.commit()


# ── Helpers ───────────────────────────────────────────────────────────────────

def _extract_keywords(titles: list[str]) -> list[dict]:
    """Return top-20 keywords from article titles (English words + Chinese bigrams)."""
    counter: Counter = Counter()
    for title in titles:
        if not title:
            continue
        # English: 3+ char words, lowercased
        for word in re.findall(r"[a-zA-Z]{3,}", title.lower()):
            if word not in _STOP_WORDS:
                counter[word] += 1
        # Chinese: sliding bigrams from runs of CJK characters
        for chunk in re.findall(r"[\u4e00-\u9fff]{2,}", title):
            for j in range(len(chunk) - 1):
                bigram = chunk[j : j + 2]
                if bigram not in _ZH_STOP:
                    counter[bigram] += 1
    return [{"word": w, "count": c} for w, c in counter.most_common(20)]


# ── Main function ─────────────────────────────────────────────────────────────

def analyze_trends(db_path) -> dict:
    """
    Analyze 7-day sentiment & volume trends.

    Returns a dict with keys:
        daily_volume      list[{date, count}]          – 7 days, oldest first
        daily_sentiment   list[{date, pos, neu, neg}]  – 7 days, oldest first
        hot_keywords      list[{word, count}]           – top 20, last 24 h
        anomalies         list[{type, pct, detail}]     – empty if none detected
        platform_trends   list[{platform, daily, avg_7d}]
    """
    db_path = Path(db_path)
    conn = sqlite3.connect(db_path)
    try:
        _ensure_schema(conn)

        now   = datetime.now(timezone.utc)
        today = now.date()
        # Oldest → newest so charts render left-to-right
        dates = [(today - timedelta(days=i)) for i in range(6, -1, -1)]

        # ── Daily volume ──────────────────────────────────────────────────────
        daily_volume: list[dict] = []
        for d in dates:
            pfx   = d.isoformat()
            count = conn.execute(
                "SELECT COUNT(*) FROM raw_articles WHERE collected_at LIKE ?",
                (pfx + "%",),
            ).fetchone()[0]
            daily_volume.append({"date": pfx, "count": count})

        # ── Daily sentiment ───────────────────────────────────────────────────
        daily_sentiment: list[dict] = []
        for d in dates:
            pfx  = d.isoformat()
            rows = conn.execute(
                "SELECT sentiment_label, COUNT(*) FROM raw_articles "
                "WHERE collected_at LIKE ? AND sentiment_label IS NOT NULL "
                "GROUP BY sentiment_label",
                (pfx + "%",),
            ).fetchall()
            c = {r[0]: r[1] for r in rows}
            daily_sentiment.append({
                "date":     pfx,
                "positive": c.get("positive", 0),
                "neutral":  c.get("neutral",  0),
                "negative": c.get("negative", 0),
            })

        # ── Hot keywords (last 24 h) ──────────────────────────────────────────
        cutoff_24 = (now - timedelta(hours=24)).isoformat()
        titles = [
            row[0]
            for row in conn.execute(
                "SELECT title FROM raw_articles WHERE collected_at >= ?",
                (cutoff_24,),
            ).fetchall()
        ]
        hot_keywords = _extract_keywords(titles)

        # ── Anomaly detection ─────────────────────────────────────────────────
        anomalies: list[dict] = []

        # Volume surge: today > 2× past-6-day average
        today_vol     = daily_volume[-1]["count"]
        past_6_vols   = [d["count"] for d in daily_volume[:-1]]
        avg_past_6_vol = sum(past_6_vols) / max(len(past_6_vols), 1)

        if avg_past_6_vol > 0 and today_vol > avg_past_6_vol * 2:
            pct = round((today_vol / avg_past_6_vol - 1) * 100, 1)
            anomalies.append({
                "type":   "声量激增",
                "pct":    pct,
                "detail": (
                    f"今日声量 {today_vol} 条，"
                    f"超过过去6天均值 {avg_past_6_vol:.0f} 条的 {pct}%"
                ),
            })

        # Negative surge: today's negative ratio > 1.5× past-6-day average
        today_sent       = daily_sentiment[-1]
        today_sent_total = max(
            today_sent["positive"] + today_sent["neutral"] + today_sent["negative"], 1
        )
        today_neg_ratio  = today_sent["negative"] / today_sent_total

        past_6_neg_ratios = []
        for ds in daily_sentiment[:-1]:
            tot = max(ds["positive"] + ds["neutral"] + ds["negative"], 1)
            past_6_neg_ratios.append(ds["negative"] / tot)
        avg_past_6_neg = sum(past_6_neg_ratios) / max(len(past_6_neg_ratios), 1)

        if avg_past_6_neg > 0 and today_neg_ratio > avg_past_6_neg * 1.5:
            pct = round((today_neg_ratio / avg_past_6_neg - 1) * 100, 1)
            anomalies.append({
                "type":   "负面激增",
                "pct":    pct,
                "detail": (
                    f"今日负面比例 {today_neg_ratio:.1%}，"
                    f"超过过去6天均值 {avg_past_6_neg:.1%} 的 {pct}%"
                ),
            })

        # ── Platform trends ───────────────────────────────────────────────────
        platform_trends: list[dict] = []
        for platform_name, sources in _PLATFORM_SOURCES.items():
            placeholders  = ",".join(["?"] * len(sources))
            daily_counts: list[dict] = []
            for d in dates:
                pfx   = d.isoformat()
                count = conn.execute(
                    f"SELECT COUNT(*) FROM raw_articles "
                    f"WHERE source IN ({placeholders}) AND collected_at LIKE ?",
                    sources + [pfx + "%"],
                ).fetchone()[0]
                daily_counts.append({"date": pfx, "count": count})
            total_7d = sum(d["count"] for d in daily_counts)
            platform_trends.append({
                "platform": platform_name,
                "daily":    daily_counts,
                "avg_7d":   round(total_7d / 7, 1),
            })

        # ── Assemble & persist ────────────────────────────────────────────────
        result: dict = {
            "daily_volume":    daily_volume,
            "daily_sentiment": daily_sentiment,
            "hot_keywords":    hot_keywords,
            "anomalies":       anomalies,
            "platform_trends": platform_trends,
        }

        today_str = today.isoformat()
        for metric_name, value in result.items():
            conn.execute(
                "INSERT OR REPLACE INTO trend_data "
                "    (date, metric_name, metric_value_json) "
                "VALUES (?, ?, ?)",
                (today_str, metric_name, json.dumps(value, ensure_ascii=False)),
            )
        conn.commit()

        logger.info(
            "Trend analysis complete – %d anomaly(ies) detected.", len(anomalies)
        )
        return result

    finally:
        conn.close()

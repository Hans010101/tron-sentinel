"""
analyzers/sentiment_analyzer.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Keyword-based sentiment classifier for TRON Sentinel.

Scans raw_articles that lack a sentiment_label and assigns:
    positive  (score  0.5 to 0.8)
    negative  (score -0.5 to -0.8)
    neutral   (score  0.0 to 0.1)

Based on keyword matching in title + summary text.
"""

import logging
import random
import sqlite3
from pathlib import Path

logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent.parent / "data" / "sentinel.db"

# ── Keyword lists ─────────────────────────────────────────────────────────────

_POSITIVE_KEYWORDS = [
    # English
    "surge", "surges", "surging", "bullish", "partnership", "launch",
    "launches", "launched", "growth", "upgrade", "milestone", "adoption",
    "rally", "rallies", "soar", "soars", "gain", "gains", "breakout",
    "all-time high", "ath", "record high", "deal", "integrate",
    "integration", "expansion", "boost", "positive", "optimistic",
    "approval", "approved", "listing", "listed", "innovation",
    "achievement", "success", "recover", "recovery", "rebound",
    # Chinese
    "利好", "合作", "上涨", "突破", "暴涨", "利多", "看涨", "大涨",
    "创新高", "利好消息", "战略合作", "重大突破", "里程碑", "新高",
    "反弹", "回升", "增长", "飙升",
]

_NEGATIVE_KEYWORDS = [
    # English
    "crash", "crashes", "crashed", "scam", "fraud", "hack", "hacked",
    "exploit", "sec", "lawsuit", "sued", "investigation", "arrest",
    "arrested", "penalty", "fine", "fined", "ban", "banned",
    "warning", "risk", "dump", "dumped", "plunge", "plunges",
    "bearish", "collapse", "sell-off", "selloff", "liquidat",
    "vulnerability", "breach", "stolen", "theft", "ponzi",
    "rug pull", "rugpull", "manipulation", "sanction", "indictment",
    # Chinese
    "诈骗", "崩盘", "暴跌", "骗局", "跑路", "割韭菜", "爆雷",
    "监管", "调查", "风险", "下跌", "大跌", "利空", "被捕",
    "罚款", "起诉", "警告", "漏洞", "黑客", "被盗",
]


def _classify_text(text: str) -> tuple[str, float]:
    """
    Classify text sentiment by keyword matching.
    Returns (label, score).
    """
    t = text.lower()

    pos_hits = sum(1 for kw in _POSITIVE_KEYWORDS if kw in t)
    neg_hits = sum(1 for kw in _NEGATIVE_KEYWORDS if kw in t)

    if neg_hits > pos_hits:
        # More negative keywords → negative
        intensity = min(neg_hits, 4) / 4  # 0.25 to 1.0
        score = -(0.5 + intensity * 0.3)  # -0.5 to -0.8
        score += random.uniform(-0.05, 0.05)
        return "negative", round(max(-0.95, min(-0.3, score)), 4)

    if pos_hits > neg_hits:
        # More positive keywords → positive
        intensity = min(pos_hits, 4) / 4
        score = 0.5 + intensity * 0.3  # 0.5 to 0.8
        score += random.uniform(-0.05, 0.05)
        return "positive", round(max(0.3, min(0.95, score)), 4)

    if pos_hits > 0 and neg_hits > 0:
        # Equal hits → neutral with slight variation
        return "neutral", round(random.uniform(0.0, 0.1), 4)

    # No keyword matches → neutral
    return "neutral", round(random.uniform(0.0, 0.1), 4)


def analyze_sentiment(conn: sqlite3.Connection) -> int:
    """
    Analyze all raw_articles that don't yet have a sentiment_label.
    Updates sentiment_label and sentiment_score in place.
    Returns count of rows updated.
    """
    # Ensure columns exist
    existing = {row[1] for row in conn.execute("PRAGMA table_info(raw_articles)")}
    if "sentiment_label" not in existing:
        conn.execute("ALTER TABLE raw_articles ADD COLUMN sentiment_label TEXT")
    if "sentiment_score" not in existing:
        conn.execute("ALTER TABLE raw_articles ADD COLUMN sentiment_score REAL")
    conn.commit()

    rows = conn.execute(
        "SELECT id, title, summary FROM raw_articles "
        "WHERE sentiment_label IS NULL"
    ).fetchall()

    if not rows:
        logger.info("Sentiment: no unanalyzed articles found")
        return 0

    logger.info("Sentiment: analyzing %d articles", len(rows))
    updated = 0

    for row_id, title, summary in rows:
        text = f"{title or ''} {summary or ''}"
        label, score = _classify_text(text)
        conn.execute(
            "UPDATE raw_articles SET sentiment_label = ?, sentiment_score = ? "
            "WHERE id = ?",
            (label, score, row_id),
        )
        updated += 1

    conn.commit()
    logger.info("Sentiment: updated %d articles (positive/negative/neutral)", updated)
    return updated


def open_db(db_path: Path = DB_PATH) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    conn = open_db()
    print(f"\nDatabase: {DB_PATH.resolve()}\n")
    try:
        count = analyze_sentiment(conn)
        # Show distribution
        dist = conn.execute(
            "SELECT sentiment_label, COUNT(*) FROM raw_articles "
            "WHERE sentiment_label IS NOT NULL GROUP BY sentiment_label"
        ).fetchall()
        print(f"\nSentiment analysis: updated {count} articles")
        for label, cnt in dist:
            print(f"  {label:>10}: {cnt}")
        print()
    finally:
        conn.close()


if __name__ == "__main__":
    main()

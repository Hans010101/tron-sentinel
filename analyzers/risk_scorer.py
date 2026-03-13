"""
analyzers/risk_scorer.py
~~~~~~~~~~~~~~~~~~~~~~~~
Composite risk scoring for TRON Sentinel articles.

Computes a 0-100 risk score for each article based on:
    - Sentiment score (base score)
    - TRON keyword relevance (multiplier)
    - Source authority weight (multiplier)
    - Engagement volume (multiplier)
    - LLM risk level override (when available)

Usage:
    python -m analyzers.risk_scorer
"""

import logging
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent.parent / "data" / "sentinel.db"

# ── Constants ────────────────────────────────────────────────────────────────

_TRON_KEYWORDS = re.compile(
    r"tron|trx|justin\s*sun|孙宇晨|波场|sunpump|usdd|bittorrent|tron\s*network|tron\s*foundation",
    re.IGNORECASE,
)

_MAINSTREAM_SOURCES = {
    "Reuters_Tech", "BBC_Crypto", "Guardian_Crypto", "SCMP_Crypto",
    "Forbes_Crypto",
}

_CRYPTO_MEDIA_SOURCES = {
    "CoinDesk", "CoinTelegraph", "Decrypt", "TheBlock", "Blockworks",
    "BitcoinMagazine", "DLNews", "Protos", "TheDefiant",
    "BlockBeats", "JinSeCaiJing", "PANews", "ShenChaoTechFlow",
    "Bitpush", "8BTC", "BlockTempo", "crypto_panic",
}

_LLM_RISK_SCORES = {
    "critical": 90,
    "high":     75,
    "medium":   50,
    "low":      20,
}

_ENGAGEMENT_RE = re.compile(
    r"(?:Likes?|Comments?|Retweets?|Views?|Plays?|Score|Reposts?):\s*(\d+)",
    re.IGNORECASE,
)

# ── Database ─────────────────────────────────────────────────────────────────


def open_db(db_path: Path = DB_PATH) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _ensure_columns(conn: sqlite3.Connection) -> None:
    """Add risk_score and alert_sent columns if missing."""
    existing = {row[1] for row in conn.execute("PRAGMA table_info(raw_articles)")}
    if "risk_score" not in existing:
        conn.execute("ALTER TABLE raw_articles ADD COLUMN risk_score INTEGER")
    if "alert_sent" not in existing:
        conn.execute("ALTER TABLE raw_articles ADD COLUMN alert_sent INTEGER DEFAULT 0")
    conn.commit()


# ── Scoring logic ────────────────────────────────────────────────────────────


def _base_score(sentiment_score: float | None) -> float:
    """Map sentiment_score (-1..1) to a base risk score (10..60)."""
    if sentiment_score is None:
        return 30.0
    # Linear mapping: -1 → 60, 0 → 30, 1 → 10
    clamped = max(-1.0, min(1.0, sentiment_score))
    return 35.0 - 25.0 * clamped


def _tron_multiplier(title: str, summary: str | None) -> float:
    """1.5 if TRON-related keywords found, else 1.0."""
    text = f"{title} {summary or ''}"
    return 1.5 if _TRON_KEYWORDS.search(text) else 1.0


def _source_multiplier(source: str) -> float:
    """Mainstream 1.3, crypto media 1.1, social/other 1.0."""
    if source in _MAINSTREAM_SOURCES:
        return 1.3
    if source in _CRYPTO_MEDIA_SOURCES:
        return 1.1
    return 1.0


def _engagement_multiplier(summary: str | None) -> float:
    """1.2 if any single engagement metric > 1000."""
    if not summary:
        return 1.0
    for m in _ENGAGEMENT_RE.finditer(summary):
        if int(m.group(1)) > 1000:
            return 1.2
    return 1.0


def _compute_risk_score(
    sentiment_score: float | None,
    title: str,
    summary: str | None,
    source: str,
    llm_risk_level: str | None,
) -> int:
    """Return a 0-100 integer risk score."""
    # LLM override takes priority
    if llm_risk_level and llm_risk_level in _LLM_RISK_SCORES:
        base = float(_LLM_RISK_SCORES[llm_risk_level])
    else:
        base = _base_score(sentiment_score)

    score = base
    score *= _tron_multiplier(title, summary)
    score *= _source_multiplier(source)
    score *= _engagement_multiplier(summary)

    return max(0, min(100, round(score)))


# ── Main scoring function ───────────────────────────────────────────────────


def score_articles(db_path: Path = DB_PATH) -> list[dict]:
    """
    Score all articles from the last 24 hours that don't have a risk_score yet.

    Returns a list of critical articles (risk_score >= 80) as dicts with
    keys: id, title, source, risk_score, link, llm_summary_zh.
    """
    conn = open_db(db_path)
    try:
        _ensure_columns(conn)

        cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()

        # Check which optional columns exist
        cols = {row[1] for row in conn.execute("PRAGMA table_info(raw_articles)")}
        has_sentiment = "sentiment_score" in cols
        has_llm_risk = "llm_risk_level" in cols
        has_llm_summary = "llm_summary_zh" in cols

        select_cols = "id, title, summary, source, link"
        if has_sentiment:
            select_cols += ", sentiment_score"
        if has_llm_risk:
            select_cols += ", llm_risk_level"
        if has_llm_summary:
            select_cols += ", llm_summary_zh"

        rows = conn.execute(
            f"SELECT {select_cols} FROM raw_articles "
            "WHERE (risk_score IS NULL) AND collected_at >= ? ",
            (cutoff,),
        ).fetchall()

        if not rows:
            logger.info("No articles pending risk scoring.")
            return []

        logger.info("Risk scoring: %d articles to process.", len(rows))

        critical_articles: list[dict] = []
        scored = 0

        for row in rows:
            idx = 0
            art_id = row[idx]; idx += 1
            title = row[idx]; idx += 1
            summary = row[idx]; idx += 1
            source = row[idx]; idx += 1
            link = row[idx]; idx += 1

            sentiment_score = row[idx] if has_sentiment else None
            if has_sentiment:
                idx += 1
            llm_risk_level = row[idx] if has_llm_risk else None
            if has_llm_risk:
                idx += 1
            llm_summary_zh = row[idx] if has_llm_summary else None

            risk = _compute_risk_score(
                sentiment_score, title, summary, source, llm_risk_level,
            )

            conn.execute(
                "UPDATE raw_articles SET risk_score = ? WHERE id = ?",
                (risk, art_id),
            )
            scored += 1

            if risk >= 80:
                critical_articles.append({
                    "id": art_id,
                    "title": title,
                    "source": source,
                    "risk_score": risk,
                    "link": link,
                    "llm_summary_zh": llm_summary_zh,
                })

        conn.commit()
        logger.info(
            "Risk scoring done: %d scored, %d critical (>=80).",
            scored, len(critical_articles),
        )
        return critical_articles

    finally:
        conn.close()


# ── Entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
    )
    critical = score_articles()
    print(f"\nDone: {len(critical)} critical articles found.")
    for art in critical:
        print(f"  [{art['risk_score']}] {art['title'][:60]}")

"""
analyzers/sentiment_analyzer.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
VADER-based sentiment analyzer for raw_articles.

Reads every row whose sentiment_score is NULL, scores the concatenated
title + summary with VADER's compound metric, then writes the result
back to the same row.

Scoring thresholds (per VADER paper / vaderSentiment defaults):
    compound >  0.05  →  "positive"
    compound < -0.05  →  "negative"
    otherwise         →  "neutral"

Note on Chinese content (BlockBeats):
    VADER is an English lexicon.  Chinese-language articles receive a
    compound score of 0.0 and are therefore labelled "neutral".  A
    multilingual model should be added in a follow-up pass for those rows.

Prerequisites:
    pip install vaderSentiment
    Run collectors/rss_collector.py first to populate raw_articles.

Usage:
    python analyzers/sentiment_analyzer.py
    python -m analyzers.sentiment_analyzer
"""

import logging
import sqlite3
from pathlib import Path

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

logger = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────

DB_PATH = Path(__file__).parent.parent / "data" / "sentinel.db"

_BATCH_SIZE   = 200     # rows fetched and committed per round-trip
_POS_THRESHOLD =  0.05  # VADER paper recommendation
_NEG_THRESHOLD = -0.05

# ── SQL ────────────────────────────────────────────────────────────────────────

# Fetch one batch of unanalysed rows.
_SELECT_PENDING = """
SELECT id, title, summary
FROM   raw_articles
WHERE  sentiment_score IS NULL
LIMIT  ?
"""

# Write both result fields back in a single UPDATE per row.
_UPDATE_ROW = """
UPDATE raw_articles
SET    sentiment_score = ?,
       sentiment_label = ?
WHERE  id = ?
"""

# Aggregate for the summary report.
_SUMMARY_QUERY = """
SELECT sentiment_label,
       COUNT(*)                     AS cnt,
       ROUND(AVG(sentiment_score), 4) AS avg_score
FROM   raw_articles
WHERE  sentiment_label IS NOT NULL
GROUP  BY sentiment_label
ORDER  BY cnt DESC
"""

# ── Helpers ────────────────────────────────────────────────────────────────────

def _label(score: float) -> str:
    """Map a VADER compound score to a sentiment label string."""
    if score > _POS_THRESHOLD:
        return "positive"
    if score < _NEG_THRESHOLD:
        return "negative"
    return "neutral"


def _analysis_text(title: str, summary: str | None) -> str:
    """
    Build the string passed to VADER.

    Combining title and summary gives the lexicon more signal than the
    title alone, particularly for longer English articles.
    """
    parts = [title] if title else []
    if summary:
        parts.append(summary)
    return " ".join(parts)


# ── Database ───────────────────────────────────────────────────────────────────

def _ensure_columns(conn: sqlite3.Connection) -> None:
    """
    Add sentiment_score and sentiment_label columns if they are absent.

    SQLite does not support ALTER TABLE … ADD COLUMN IF NOT EXISTS, so we
    inspect PRAGMA table_info first to avoid a duplicate-column error.
    """
    existing = {row[1] for row in conn.execute("PRAGMA table_info(raw_articles)")}

    if "sentiment_score" not in existing:
        conn.execute("ALTER TABLE raw_articles ADD COLUMN sentiment_score REAL")
        logger.debug("Schema: added column sentiment_score")

    if "sentiment_label" not in existing:
        conn.execute("ALTER TABLE raw_articles ADD COLUMN sentiment_label TEXT")
        logger.debug("Schema: added column sentiment_label")

    conn.commit()


def open_db(db_path: Path = DB_PATH) -> sqlite3.Connection:
    """
    Open the SQLite database and migrate the schema if needed.

    Raises FileNotFoundError when the database does not exist yet, which
    signals that rss_collector should be run first.
    """
    if not db_path.exists():
        raise FileNotFoundError(
            f"Database not found: {db_path}\n"
            "Run  python collectors/rss_collector.py  first."
        )
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    _ensure_columns(conn)
    return conn


# ── Core analysis ──────────────────────────────────────────────────────────────

def analyze_pending(conn: sqlite3.Connection) -> int:
    """
    Score all raw_articles rows where sentiment_score IS NULL.

    Rows are processed in batches of ``_BATCH_SIZE`` and committed after
    each batch, so progress is preserved even on an interrupted run.

    Returns the total number of rows updated.
    """
    analyzer = SentimentIntensityAnalyzer()
    cur      = conn.cursor()
    total    = 0

    while True:
        rows = conn.execute(_SELECT_PENDING, (_BATCH_SIZE,)).fetchall()
        if not rows:
            break

        batch: list[tuple[float, str, int]] = []
        for row_id, title, summary in rows:
            text   = _analysis_text(title or "", summary)
            score  = analyzer.polarity_scores(text)["compound"]
            batch.append((score, _label(score), row_id))

        cur.executemany(_UPDATE_ROW, batch)
        conn.commit()
        total += len(batch)
        logger.info("Analysed and committed %d rows (running total: %d)", len(batch), total)

    return total


# ── Entry point ────────────────────────────────────────────────────────────────

def main() -> None:
    """
    Standalone runner.  Analyses all pending articles and prints a summary
    of the sentiment distribution across the entire raw_articles table.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    conn = open_db()
    print(f"\n数据库位置: {DB_PATH.resolve()}\n")

    try:
        pending = conn.execute(
            "SELECT COUNT(*) FROM raw_articles WHERE sentiment_score IS NULL"
        ).fetchone()[0]

        print(f"待分析文章: {pending} 条")

        if pending == 0:
            print("所有文章均已完成情绪分析。")
        else:
            updated = analyze_pending(conn)
            print(f"本次分析完成: {updated} 条")

        # ── Distribution report ────────────────────────────────────────────
        rows        = conn.execute(_SUMMARY_QUERY).fetchall()
        total_label = sum(r[1] for r in rows)

        if total_label == 0:
            print("\n数据库中暂无已标注文章。")
            return

        _ZH   = {"positive": "正面", "negative": "负面", "neutral": "中性"}
        _ICON = {"positive": "🟢",   "negative": "🔴",   "neutral": "🟡"}

        sep = "─" * 54
        print(f"\n{sep}")
        print(f"  已标注总量 : {total_label:>6} 条")
        print(f"{sep}")
        print(f"\n  {'情绪':<6} {'条数':>6}   {'占比':>6}   {'平均分':>8}   分布")

        for label, cnt, avg in rows:
            pct  = cnt / total_label * 100
            bar  = "█" * round(pct / 2)
            zh   = _ZH.get(label, label)
            icon = _ICON.get(label, " ")
            print(f"  {icon} {zh:<4} {cnt:>6} 条  {pct:5.1f}%  {avg:>+.4f}   {bar}")

        print()

    finally:
        conn.close()


if __name__ == "__main__":
    main()

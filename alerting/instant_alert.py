"""
alerting/instant_alert.py
~~~~~~~~~~~~~~~~~~~~~~~~~
Instant alert sender for critical risk articles.

Checks raw_articles for recent high-risk items (risk_score >= 80)
that haven't been alerted yet, and sends each one as a red Feishu
card via the webhook.

Usage:
    python -m alerting.instant_alert
"""

import logging
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent.parent / "data" / "sentinel.db"

_RISK_THRESHOLD = 80


def _open_db(db_path: Path = DB_PATH) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def send_critical_alerts(db_path: Path = DB_PATH) -> int:
    """
    Find articles from the last hour with risk_score >= 80 and alert_sent = 0,
    send each via Feishu webhook, and mark them as alerted.

    Returns the number of alerts sent.
    """
    webhook_url = os.environ.get("FEISHU_WEBHOOK_URL", "").strip()
    if not webhook_url:
        logger.info("FEISHU_WEBHOOK_URL not set – skipping instant alerts.")
        return 0

    conn = _open_db(db_path)
    try:
        # Check columns exist
        cols = {row[1] for row in conn.execute("PRAGMA table_info(raw_articles)")}
        if "risk_score" not in cols or "alert_sent" not in cols:
            logger.info("risk_score/alert_sent columns not found – skipping.")
            return 0

        has_llm_summary = "llm_summary_zh" in cols
        has_llm_risk = "llm_risk_level" in cols

        extra = ""
        if has_llm_summary:
            extra += ", llm_summary_zh"
        if has_llm_risk:
            extra += ", llm_risk_level"

        cutoff = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()

        rows = conn.execute(
            "SELECT id, title, source, risk_score, link, "
            "       sentiment_score"
            f"{extra} "
            "FROM raw_articles "
            "WHERE risk_score >= ? "
            "  AND (alert_sent IS NULL OR alert_sent = 0) "
            "  AND collected_at >= ? "
            "ORDER BY risk_score DESC",
            (_RISK_THRESHOLD, cutoff),
        ).fetchall()

        if not rows:
            logger.info("No critical articles pending alert.")
            return 0

        from alerting.webhook_notifier import send_feishu_webhook  # noqa: PLC0415

        sent = 0
        for r in rows:
            title = r["title"]
            source = r["source"]
            score = r["risk_score"]
            link = r["link"]
            sentiment = r["sentiment_score"]

            llm_summary = r["llm_summary_zh"] if has_llm_summary else None
            llm_risk = r["llm_risk_level"] if has_llm_risk else None

            # Build alert card content
            lines = [
                f"**风险分数: {score}/100**",
                "",
                f"**标题:** {title}",
                f"**来源:** {source}",
            ]
            if llm_risk:
                lines.append(f"**风险等级:** {llm_risk.upper()}")
            if sentiment is not None:
                lines.append(f"**情绪分数:** {sentiment:.2f}")
            if llm_summary:
                lines.append("")
                lines.append(f"**AI摘要:** {llm_summary}")
            lines.append("")
            lines.append(f"[查看原文]({link})")

            content = "\n".join(lines)

            ok = send_feishu_webhook(
                webhook_url,
                f"🚨 TRON舆情预警 [风险{score}分]",
                content,
                header_color="red",
            )

            if ok:
                conn.execute(
                    "UPDATE raw_articles SET alert_sent = 1 WHERE id = ?",
                    (r["id"],),
                )
                conn.commit()
                sent += 1
                logger.info("Alert sent for article %d: %s", r["id"], title[:50])
            else:
                logger.warning("Alert failed for article %d", r["id"])

        logger.info("Instant alerts done: %d/%d sent.", sent, len(rows))
        return sent

    finally:
        conn.close()


# ── Entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
    )
    count = send_critical_alerts()
    print(f"Done: {count} alerts sent.")

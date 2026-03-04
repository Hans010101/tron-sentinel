"""
alerting/telegram_alerter.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Reads negative articles from raw_articles and dispatches alert messages
via the Telegram Bot API.

Alert levels (by sentiment_score):
    score < -0.7  →  🔴 CRITICAL
    score < -0.5  →  🟠 HIGH
    score < -0.3  →  🟡 MEDIUM
    score < -0.05 →  🟢 LOW   (negative but mild)

Configuration (environment variables):
    TELEGRAM_BOT_TOKEN   –  token from @BotFather
    TELEGRAM_CHAT_ID     –  target chat / channel ID

Dry-run mode:
    When TELEGRAM_BOT_TOKEN is not set, every alert is printed to stdout
    instead of being sent to Telegram.  No exception is raised.

Usage:
    python alerting/telegram_alerter.py
    python -m alerting.telegram_alerter
"""

import json
import logging
import os
import sqlite3
import time
import urllib.error
import urllib.request
from pathlib import Path

logger = logging.getLogger(__name__)

# ── Paths & endpoints ──────────────────────────────────────────────────────────

DB_PATH      = Path(__file__).parent.parent / "data" / "sentinel.db"
_TELEGRAM_URL = "https://api.telegram.org/bot{token}/sendMessage"

# Telegram allows ~1 message / second to the same chat before throttling.
_INTER_MSG_DELAY = 0.5  # seconds between successive sends

# ── Alert level mapping ────────────────────────────────────────────────────────
# Each tuple: (score_threshold, icon, label).
# The first entry whose threshold is *greater than* the score wins.
_LEVELS: list[tuple[float, str, str]] = [
    (-0.7, "🔴", "CRITICAL"),
    (-0.5, "🟠", "HIGH"),
    (-0.3, "🟡", "MEDIUM"),
    (-0.05, "🟢", "LOW"),          # catches remaining negatives (< -0.05)
]


def alert_level(score: float) -> tuple[str, str]:
    """
    Return ``(icon, label)`` for a negative sentiment score.

    >>> alert_level(-0.85)
    ('🔴', 'CRITICAL')
    >>> alert_level(-0.55)
    ('🟠', 'HIGH')
    >>> alert_level(-0.35)
    ('🟡', 'MEDIUM')
    >>> alert_level(-0.10)
    ('🟢', 'LOW')
    """
    for threshold, icon, label in _LEVELS:
        if score < threshold:
            return icon, label
    # Fallback – should not be reached for genuinely negative scores.
    return "🟢", "LOW"


# ── Message formatting ─────────────────────────────────────────────────────────

def build_message(article: dict) -> str:
    """
    Compose the plain-text alert body for one article.

    Example output::

        🔴 TRON Sentinel 预警 — CRITICAL

        标题：TRX Price Plunges 18% Amid Market Panic
        来源：CoinDesk
        情绪分：-0.8421
        链接：https://www.coindesk.com/…
    """
    icon, level = alert_level(article["sentiment_score"])
    return "\n".join([
        f"{icon} TRON Sentinel 预警 — {level}",
        "",
        f"标题：{article['title']}",
        f"来源：{article['source']}",
        f"情绪分：{article['sentiment_score']:+.4f}",
        f"链接：{article['link']}",
    ])


# ── Telegram transport ─────────────────────────────────────────────────────────

class TelegramAlerter:
    """
    Dispatches alert messages to a Telegram chat.

    When *token* is absent (and TELEGRAM_BOT_TOKEN is not set), the
    instance silently enters **dry-run mode**: ``send_alert`` prints
    the formatted message to stdout and returns ``True`` so callers
    do not need to branch on configuration state.
    """

    def __init__(
        self,
        token:   str | None = None,
        chat_id: str | None = None,
    ) -> None:
        self.token   = token   or os.environ.get("TELEGRAM_BOT_TOKEN", "")
        self.chat_id = chat_id or os.environ.get("TELEGRAM_CHAT_ID",   "")
        self.dry_run = not bool(self.token)

        if self.dry_run:
            logger.warning(
                "TELEGRAM_BOT_TOKEN not configured – dry-run mode active "
                "(alerts will be printed to stdout)"
            )

    # ── Internal HTTP call ────────────────────────────────────────────────────

    def _post(self, text: str) -> bool:
        """
        Call Telegram's sendMessage endpoint.

        Returns True on success.  Any HTTP or network error is logged and
        False is returned so the caller can decide how to proceed.
        """
        url = _TELEGRAM_URL.format(token=self.token)
        payload = json.dumps({
            "chat_id": self.chat_id,
            "text":    text,
            # No parse_mode – plain text avoids entity-escaping issues
            # in article titles that may contain <, >, & characters.
        }).encode("utf-8")

        req = urllib.request.Request(
            url,
            data    = payload,
            headers = {"Content-Type": "application/json; charset=utf-8"},
            method  = "POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                result = json.loads(resp.read().decode("utf-8"))
                if result.get("ok"):
                    logger.debug(
                        "Telegram: sent message_id=%s",
                        result["result"].get("message_id"),
                    )
                    return True
                logger.error("Telegram API rejected message: %s", result.get("description"))
                return False

        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            logger.error("Telegram HTTP %d: %s", exc.code, body)
        except urllib.error.URLError as exc:
            logger.error("Telegram network error: %s", exc.reason)
        except Exception as exc:                          # pragma: no cover
            logger.error("Telegram unexpected error: %s", exc)

        return False

    # ── Public API ────────────────────────────────────────────────────────────

    def send_alert(self, article: dict) -> bool:
        """
        Format and dispatch one alert.

        In dry-run mode the message is printed to stdout between separator
        lines.  Returns True when the message was delivered (or printed).
        """
        text = build_message(article)

        if self.dry_run:
            sep = "─" * 56
            print(f"\n{sep}\n{text}\n{sep}")
            return True

        ok = self._post(text)
        if ok:
            icon, label = alert_level(article["sentiment_score"])
            logger.info(
                "Alert dispatched  %s %-9s  score=%.4f  %s",
                icon, label, article["sentiment_score"],
                article["title"][:60],
            )
        return ok

    def send_alerts(
        self,
        articles: list[dict],
        delay: float = _INTER_MSG_DELAY,
    ) -> int:
        """
        Send alerts for each article in *articles*.

        Inserts a *delay*-second pause between successive messages to
        stay within Telegram's per-chat rate limit.

        Returns the count of successfully sent (or printed) messages.
        """
        sent = 0
        for i, article in enumerate(articles):
            if i:
                time.sleep(delay)
            if self.send_alert(article):
                sent += 1
        return sent


# ── Database helpers ───────────────────────────────────────────────────────────

def fetch_negative_articles(
    conn:  sqlite3.Connection,
    limit: int = 5,
) -> list[dict]:
    """
    Return up to *limit* negative articles from raw_articles.

    Ordered by ``collected_at DESC`` (newest first), then by
    ``sentiment_score ASC`` (most negative first within the same
    collection batch) so the most actionable alerts surface first.
    """
    rows = conn.execute(
        """
        SELECT id, title, link, source, sentiment_score, sentiment_label, published_at
        FROM   raw_articles
        WHERE  sentiment_label = 'negative'
        ORDER  BY collected_at DESC, sentiment_score ASC
        LIMIT  ?
        """,
        (limit,),
    ).fetchall()

    keys = ("id", "title", "link", "source", "sentiment_score", "sentiment_label", "published_at")
    return [dict(zip(keys, row)) for row in rows]


def open_db(db_path: Path = DB_PATH) -> sqlite3.Connection:
    """Open the SQLite database; raises FileNotFoundError if absent."""
    if not db_path.exists():
        raise FileNotFoundError(
            f"Database not found: {db_path}\n"
            "Run the following first:\n"
            "  python collectors/rss_collector.py\n"
            "  python analyzers/sentiment_analyzer.py"
        )
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


# ── Entry point ────────────────────────────────────────────────────────────────

def main() -> None:
    """
    Simulate sending alerts for the 5 most recent negative articles.

    Without TELEGRAM_BOT_TOKEN set this runs in dry-run mode and prints
    the formatted messages to stdout – useful for local testing.
    """
    logging.basicConfig(
        level  = logging.INFO,
        format = "%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
        datefmt= "%H:%M:%S",
    )

    alerter = TelegramAlerter()
    conn    = open_db()

    try:
        articles = fetch_negative_articles(conn, limit=5)

        if not articles:
            print(
                "\n数据库中暂无负面文章。\n"
                "请先运行:\n"
                "  python collectors/rss_collector.py\n"
                "  python analyzers/sentiment_analyzer.py\n"
            )
            return

        mode = (
            "干运行（控制台输出）"
            if alerter.dry_run
            else f"Telegram  chat_id={alerter.chat_id}"
        )
        print(f"\n发送模式  : {mode}")
        print(f"待发送文章 : {len(articles)} 条\n")

        sent = alerter.send_alerts(articles)

        # ── Summary ────────────────────────────────────────────────────────
        sep = "─" * 56
        print(f"\n{sep}")
        print(f"  成功发送 : {sent} / {len(articles)} 条")
        print(f"{sep}")

        # Level breakdown
        level_counts: dict[str, tuple[str, int]] = {}
        for a in articles:
            icon, label = alert_level(a["sentiment_score"])
            prev_cnt = level_counts.get(label, (icon, 0))[1]
            level_counts[label] = (icon, prev_cnt + 1)

        if level_counts:
            print("\n预警等级分布：")
            for _, icon, label in _LEVELS:
                if label in level_counts:
                    _, cnt = level_counts[label]
                    print(f"  {icon} {label:<10} {cnt} 条")
        print()

    finally:
        conn.close()


if __name__ == "__main__":
    main()

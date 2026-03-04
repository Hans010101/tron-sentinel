"""
collectors/telegram_collector.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Telegram public-channel collector using the Telethon MTProto library.

Fetches the latest 100 messages from each configured public channel and
stores them in the raw_articles table (same schema as rss_collector).

Environment variables:
    TELEGRAM_API_ID    – Integer app-ID from https://my.telegram.org
    TELEGRAM_API_HASH  – App-hash from https://my.telegram.org
    TELEGRAM_BOT_TOKEN – (optional) Bot token from @BotFather.
                          If set the client authenticates as a bot,
                          which avoids interactive phone login.
                          If unset, a saved user session is required
                          (see "First-time setup" below).

First-time setup (user-account mode, one-time interactive):
    python -m collectors.telegram_collector
    # Enter your phone number and the OTP code when prompted.
    # The session is saved to  data/telegram_session.session
    # and reused automatically on all subsequent runs.

Bot-token mode:
    export TELEGRAM_BOT_TOKEN=...   # same var used by telegram_alerter
    python -m collectors.telegram_collector
    # No phone / OTP needed, but the bot must be a member of private
    # channels.  All four channels below are public, so this works.

Usage:
    python -m collectors.telegram_collector   # from project root
    python collectors/telegram_collector.py   # direct run
"""

import asyncio
import logging
import os
import sqlite3
from datetime import timezone
from pathlib import Path

logger = logging.getLogger(__name__)

# ── Paths ─────────────────────────────────────────────────────────────────────

DB_PATH      = Path(__file__).parent.parent / "data" / "sentinel.db"
SESSION_PATH = Path(__file__).parent.parent / "data" / "telegram_session"

# ── Configuration ─────────────────────────────────────────────────────────────

CHANNELS: list[dict] = [
    {"username": "tronfoundation", "language": "en"},
    {"username": "justinsuntron",  "language": "en"},
    {"username": "CoinDesk",       "language": "en"},
    {"username": "cointelegraph",  "language": "en"},
]

MESSAGES_PER_CHANNEL = 100
_SUMMARY_MAX_LEN     = 500

# ── SQL ───────────────────────────────────────────────────────────────────────

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS raw_articles (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    title        TEXT NOT NULL,
    link         TEXT NOT NULL UNIQUE,
    published_at TEXT,
    source       TEXT NOT NULL,
    summary      TEXT,
    language     TEXT NOT NULL DEFAULT 'en',
    collected_at TEXT NOT NULL
)
"""

_CREATE_INDEX = """
CREATE INDEX IF NOT EXISTS idx_raw_articles_source
    ON raw_articles (source)
"""

_INSERT = """
INSERT OR IGNORE INTO raw_articles
    (title, link, published_at, source, summary, language, collected_at)
VALUES
    (:title, :link, :published_at, :source, :summary, :language, :collected_at)
"""

# ── Database ──────────────────────────────────────────────────────────────────


def open_db(db_path: Path = DB_PATH) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute(_CREATE_TABLE)
    conn.execute(_CREATE_INDEX)
    conn.commit()
    return conn


# ── Async core ────────────────────────────────────────────────────────────────


async def _fetch_channel(client, ch_cfg: dict) -> list[dict]:
    """Fetch up to MESSAGES_PER_CHANNEL messages from one public channel."""
    from datetime import datetime  # noqa: PLC0415

    username = ch_cfg["username"]
    language = ch_cfg["language"]
    source   = f"tg_{username.lower()}"
    now_utc  = datetime.now(tz=timezone.utc).isoformat()

    articles: list[dict] = []
    try:
        entity   = await client.get_entity(f"@{username}")
        messages = await client.get_messages(entity, limit=MESSAGES_PER_CHANNEL)
    except Exception as exc:
        logger.error("[TG @%s] Could not fetch: %s", username, exc)
        return articles

    for msg in messages:
        text = (msg.text or msg.message or "").strip()
        if not text:
            continue

        # First non-empty line (≤80 chars) becomes the title.
        first_line = next((l.strip() for l in text.splitlines() if l.strip()), "")
        title = (first_line or text)[:80]
        if not title:
            continue

        # Canonical link:  https://t.me/channel/message_id
        link = f"https://t.me/{username}/{msg.id}"

        if msg.date:
            published_at = msg.date.astimezone(timezone.utc).isoformat()
        else:
            published_at = now_utc

        articles.append({
            "title":        title,
            "link":         link,
            "published_at": published_at,
            "source":       source,
            "summary":      text[:_SUMMARY_MAX_LEN],
            "language":     language,
            "collected_at": now_utc,
        })

    logger.info("[TG @%s] Retrieved %d messages", username, len(articles))
    return articles


async def _collect_async(conn: sqlite3.Connection) -> int:
    """Authenticate, fetch all channels, persist to DB. Returns new-row count."""
    api_id_str = os.getenv("TELEGRAM_API_ID",   "")
    api_hash   = os.getenv("TELEGRAM_API_HASH",  "")
    bot_token  = os.getenv("TELEGRAM_BOT_TOKEN", "")

    if not api_id_str or not api_hash:
        logger.warning(
            "TELEGRAM_API_ID / TELEGRAM_API_HASH not set. "
            "Skipping Telegram channel collection."
        )
        return 0

    try:
        api_id = int(api_id_str)
    except ValueError:
        logger.error("TELEGRAM_API_ID must be an integer (got %r).", api_id_str)
        return 0

    try:
        from telethon import TelegramClient  # noqa: PLC0415
    except ImportError:
        logger.error("telethon is not installed. Run: pip install telethon")
        return 0

    SESSION_PATH.parent.mkdir(parents=True, exist_ok=True)
    client = TelegramClient(str(SESSION_PATH), api_id, api_hash)

    total_new = 0
    cur       = conn.cursor()

    async with client:
        if bot_token:
            # Bot-token auth – no phone/OTP needed, works for public channels.
            await client.start(bot_token=bot_token)
            logger.info("Telegram: authenticated via bot token.")
        else:
            # User-session auth – requires existing session file.
            if not await client.is_user_authorized():
                logger.error(
                    "No valid Telegram session found at %s.session\n"
                    "Run 'python -m collectors.telegram_collector' once "
                    "interactively to complete phone login, then retry.",
                    SESSION_PATH,
                )
                return 0
            logger.info("Telegram: authenticated via saved user session.")

        for ch_cfg in CHANNELS:
            articles  = await _fetch_channel(client, ch_cfg)
            new_in_ch = 0
            try:
                for article in articles:
                    cur.execute(_INSERT, article)
                    new_in_ch += cur.rowcount
                conn.commit()
                logger.info(
                    "[TG @%s] Inserted %d new message(s)",
                    ch_cfg["username"], new_in_ch,
                )
            except sqlite3.Error as exc:
                conn.rollback()
                logger.error("[TG @%s] DB error: %s", ch_cfg["username"], exc)

            total_new += new_in_ch

    return total_new


# ── Public API ────────────────────────────────────────────────────────────────


def collect_all(conn: sqlite3.Connection) -> int:
    """Synchronous wrapper – run the async collector and return new-row count."""
    return asyncio.run(_collect_async(conn))


# ── Entry point ───────────────────────────────────────────────────────────────


def main() -> None:
    logging.basicConfig(
        level  = logging.INFO,
        format = "%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
        datefmt= "%H:%M:%S",
    )

    conn = open_db()
    print(f"\n数据库位置: {DB_PATH.resolve()}\n")

    try:
        new_count = collect_all(conn)
        tg_sources = [f"'tg_{c['username'].lower()}'" for c in CHANNELS]
        placeholders = ",".join(tg_sources)
        total = conn.execute(
            f"SELECT COUNT(*) FROM raw_articles WHERE source IN ({placeholders})"
        ).fetchone()[0]

        sep = "─" * 52
        print(f"\n{sep}")
        print(f"  Telegram 本次新增 : {new_count:>5} 条")
        print(f"  Telegram 总存量   : {total:>5} 条")
        print(f"{sep}\n")

        # Per-channel breakdown
        rows = conn.execute(
            f"SELECT source, COUNT(*) FROM raw_articles "
            f"WHERE source IN ({placeholders}) "
            f"GROUP BY source ORDER BY COUNT(*) DESC"
        ).fetchall()
        for src, cnt in rows:
            print(f"  {src:<25} {cnt:>5} 条")
        print()
    finally:
        conn.close()


if __name__ == "__main__":
    main()

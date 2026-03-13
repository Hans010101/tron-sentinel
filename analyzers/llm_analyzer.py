"""
analyzers/llm_analyzer.py
~~~~~~~~~~~~~~~~~~~~~~~~~
LLM analyzer for TRON Sentinel articles (Alibaba DashScope).

Calls the DashScope-compatible chat completion API to classify, summarise,
and risk-assess articles collected in the raw_articles table.

Environment variable:
    DASHSCOPE_API_KEY  – Required.  Obtain from https://dashscope.console.aliyun.com

Usage:
    python -m analyzers.llm_analyzer          # analyse pending articles
"""

import json
import logging
import os
import sqlite3
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent.parent / "data" / "sentinel.db"

_API_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions"
_MODEL = "deepseek-v3"

_MAX_BATCH = 50       # max articles per run
_CALL_DELAY = 0.5     # seconds between API calls

# ── LLM columns added to raw_articles ────────────────────────────────────────

_LLM_COLUMNS = {
    "llm_category":        "TEXT",
    "llm_sector":          "TEXT",
    "llm_risk_level":      "TEXT",
    "llm_summary_zh":      "TEXT",
    "llm_category_reason": "TEXT",
    "llm_analyzed":        "INTEGER DEFAULT 0",
}

# ── System prompt ────────────────────────────────────────────────────────────

_SYSTEM_PROMPT = """\
你是 TRON Network 的专业舆情分析师。你的任务是分析与加密货币相关的新闻、社交媒体帖子和视频内容。

对于每篇文章，请严格返回以下 JSON 格式（不要返回任何其他内容）：

{
  "category": "新闻|干货|视频|讨论|公告|广告",
  "sector": "明星公司动态|大佬动态|行业新闻|社区讨论|技术更新|监管政策",
  "sentiment": "positive|neutral|negative",
  "sentiment_score": 0.0,
  "risk_level": "low|medium|high|critical",
  "summary_zh": "中文摘要（100-150字）",
  "category_reason": "分类理由（一句话）"
}

分析要求：
1. 对涉及 TRON/TRX/Justin Sun/孙宇晨/波场 的内容重点关注
2. sentiment_score 取值范围 -1.0 到 1.0（负面为负值，正面为正值）
3. risk_level 评级标准（仅对涉及 TRON/Justin Sun 的负面内容评估）：
   - critical: 安全事件（黑客攻击/资金损失）、重大 FUD、监管行动（SEC/DOJ起诉）
   - high: 负面报道、KOL 攻击/批评、合作终止
   - medium: 争议讨论、社区质疑、竞品对比
   - low: 日常信息、中性报道、技术讨论
4. summary_zh 要突出与 TRON 相关的核心信息，使用中文撰写
5. 如果内容与 TRON/Justin Sun 无直接关系，risk_level 设为 "low"
6. 严格返回 JSON，不要包含 markdown 代码块标记或任何其他文本"""

# ── Database helpers ─────────────────────────────────────────────────────────


def open_db(db_path: Path = DB_PATH) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _ensure_llm_columns(conn: sqlite3.Connection) -> None:
    """Add LLM-specific columns to raw_articles if they don't exist."""
    existing = {row[1] for row in conn.execute("PRAGMA table_info(raw_articles)")}
    for col_name, col_type in _LLM_COLUMNS.items():
        if col_name not in existing:
            conn.execute(
                f"ALTER TABLE raw_articles ADD COLUMN {col_name} {col_type}"
            )
    conn.commit()


def _fetch_pending(conn: sqlite3.Connection, limit: int = _MAX_BATCH) -> list[tuple]:
    """
    Return up to *limit* articles from the last 24 hours that haven't
    been LLM-analysed yet.
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    return conn.execute(
        "SELECT id, title, summary, source, language "
        "FROM raw_articles "
        "WHERE (llm_analyzed IS NULL OR llm_analyzed = 0) "
        "  AND collected_at >= ? "
        "ORDER BY collected_at DESC "
        "LIMIT ?",
        (cutoff, limit),
    ).fetchall()


def _update_article(conn: sqlite3.Connection, article_id: int,
                    result: dict) -> None:
    """Write LLM analysis results back into raw_articles."""
    conn.execute(
        "UPDATE raw_articles SET "
        "  llm_category = ?, "
        "  llm_sector = ?, "
        "  llm_risk_level = ?, "
        "  llm_summary_zh = ?, "
        "  llm_category_reason = ?, "
        "  llm_analyzed = 1, "
        "  sentiment_label = ?, "
        "  sentiment_score = ? "
        "WHERE id = ?",
        (
            result.get("category"),
            result.get("sector"),
            result.get("risk_level"),
            result.get("summary_zh"),
            result.get("category_reason"),
            result.get("sentiment"),
            result.get("sentiment_score"),
            article_id,
        ),
    )
    conn.commit()


# ── DashScope API call ────────────────────────────────────────────────────────


def _call_deepseek(api_key: str, title: str, summary: str,
                   source: str, language: str) -> Optional[dict]:
    """
    Send a single article to DashScope for analysis and return parsed JSON.

    Returns None on any failure (network, parse, API error).
    """
    user_content = (
        f"来源: {source}\n"
        f"语言: {language}\n"
        f"标题: {title}\n"
        f"摘要: {summary or '(无摘要)'}"
    )

    payload = {
        "model": _MODEL,
        "messages": [
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": user_content},
        ],
        "temperature": 0.1,
        "max_tokens": 800,
        "response_format": {"type": "json_object"},
    }

    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        _API_URL,
        data=body,
        headers={
            "Content-Type": "application/json; charset=utf-8",
            "Authorization": f"Bearer {api_key}",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            resp_data = json.loads(resp.read().decode("utf-8"))

        content = resp_data["choices"][0]["message"]["content"]

        # Strip markdown code fences if present
        content = content.strip()
        if content.startswith("```"):
            # Remove ```json ... ``` wrapping
            lines = content.split("\n")
            if lines[0].startswith("```"):
                lines = lines[1:]
            if lines and lines[-1].strip() == "```":
                lines = lines[:-1]
            content = "\n".join(lines)

        result = json.loads(content)

        # Validate required fields
        if "sentiment" not in result or "category" not in result:
            logger.warning("LLM response missing required fields: %s", content[:200])
            return None

        # Clamp sentiment_score to [-1.0, 1.0]
        score = float(result.get("sentiment_score", 0.0))
        result["sentiment_score"] = max(-1.0, min(1.0, score))

        return result

    except urllib.error.HTTPError as exc:
        logger.error("DashScope HTTP %d: %s", exc.code, exc.reason)
    except urllib.error.URLError as exc:
        logger.error("DashScope network error: %s", exc.reason)
    except (json.JSONDecodeError, KeyError, ValueError) as exc:
        logger.error("DashScope response parse error: %s", exc)
    except Exception as exc:
        logger.error("DashScope unexpected error: %s", exc)

    return None


# ── Main analysis function ───────────────────────────────────────────────────


def analyze_articles(db_path: Path = DB_PATH) -> int:
    """
    Analyse unprocessed articles from the last 24 hours using DashScope.

    Returns the number of articles successfully analysed.
    Skips silently if DASHSCOPE_API_KEY is not set.
    """
    api_key = os.environ.get("DASHSCOPE_API_KEY", "").strip()
    if not api_key:
        logger.info("DASHSCOPE_API_KEY not set – skipping LLM analysis.")
        return 0

    conn = open_db(db_path)
    try:
        _ensure_llm_columns(conn)
        pending = _fetch_pending(conn)

        if not pending:
            logger.info("No articles pending LLM analysis.")
            return 0

        total = len(pending)
        logger.info("LLM analysis: %d articles to process (max %d).",
                     total, _MAX_BATCH)

        analysed = 0
        for i, (art_id, title, summary, source, language) in enumerate(pending, 1):
            logger.info("[%d/%d] Analysing: %.50s…", i, total,
                        title if len(title) > 50 else title)

            result = _call_deepseek(api_key, title, summary or "",
                                    source, language)
            if result:
                _update_article(conn, art_id, result)
                analysed += 1
                logger.debug("  → %s / %s / risk=%s",
                             result.get("category"),
                             result.get("sentiment"),
                             result.get("risk_level"))
            else:
                logger.warning("[%d/%d] Analysis failed, skipping article %d.",
                               i, total, art_id)

            # Rate-limit between calls (skip delay after last item)
            if i < total:
                time.sleep(_CALL_DELAY)

        logger.info("LLM analysis complete: %d/%d articles analysed.",
                     analysed, total)
        return analysed

    finally:
        conn.close()


# ── Entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
    )
    count = analyze_articles()
    print(f"\nDone: {count} articles analysed by LLM.")

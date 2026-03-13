"""
alerting/webhook_notifier.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Feishu (飞书) custom bot webhook notifier.

Sends rich-text interactive cards to a Feishu group chat via the
custom bot webhook API.

Usage:
    from alerting.webhook_notifier import send_feishu_webhook
    send_feishu_webhook(webhook_url, "日报标题", "Markdown 内容")

Environment variable:
    FEISHU_WEBHOOK_URL   – The webhook URL from the Feishu bot configuration.
"""

import json
import logging
import os
import re
import urllib.error
import urllib.request
from typing import Optional

logger = logging.getLogger(__name__)


def _get_webhook_url() -> Optional[str]:
    """Return the Feishu webhook URL from the environment, or None."""
    url = os.environ.get("FEISHU_WEBHOOK_URL", "").strip()
    return url or None


def _markdown_to_feishu_elements(md_text: str) -> list[dict]:
    """
    Convert a Markdown string into Feishu card element blocks.

    Supported conversions:
      - **bold**  → bold tag
      - [text](url) → link tag
      - # / ## / ### headings → markdown element
      - Plain paragraphs → markdown element
      - Emoji prefixed lines preserved as-is

    Feishu interactive cards support a subset of Markdown in their
    "markdown" element type, so we pass most content through directly
    and split on section dividers.
    """
    elements: list[dict] = []

    lines = md_text.strip().split("\n")
    current_block: list[str] = []

    def flush_block() -> None:
        if current_block:
            text = "\n".join(current_block).strip()
            if text:
                elements.append({
                    "tag": "markdown",
                    "content": text,
                })
            current_block.clear()

    for line in lines:
        stripped = line.strip()
        # Horizontal rule → divider
        if re.match(r"^-{3,}$|^_{3,}$|^\*{3,}$", stripped):
            flush_block()
            elements.append({"tag": "hr"})
            continue
        current_block.append(line)

    flush_block()
    return elements


def _build_card_payload(title: str, content: str,
                        header_color: str = "red") -> dict:
    """
    Build a Feishu interactive card (msg_type: interactive) payload.

    Args:
        title:        Card header title text.
        content:      Markdown-formatted body content.
        header_color: Template color – red, blue, green, orange, etc.
    """
    elements = _markdown_to_feishu_elements(content)

    return {
        "msg_type": "interactive",
        "card": {
            "header": {
                "title": {
                    "tag": "plain_text",
                    "content": title,
                },
                "template": header_color,
            },
            "elements": elements,
        },
    }


def send_feishu_webhook(
    webhook_url: Optional[str],
    title: str,
    content: str,
    header_color: str = "red",
) -> bool:
    """
    Send a rich-text card message to a Feishu group chat webhook.

    Args:
        webhook_url:  Full webhook URL.  Falls back to FEISHU_WEBHOOK_URL env
                      var when *None* is passed.
        title:        Card header title.
        content:      Markdown body text.
        header_color: Feishu card header colour (red / blue / green / orange).

    Returns:
        True on success, False on failure.  Failures are logged but never
        raise, so the caller's pipeline is not interrupted.
    """
    url = webhook_url or _get_webhook_url()
    if not url:
        logger.warning("Feishu webhook URL not configured – skipping send.")
        return False

    payload = _build_card_payload(title, content, header_color)
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")

    req = urllib.request.Request(
        url,
        data=body,
        headers={
            "Content-Type": "application/json; charset=utf-8",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            resp_body = json.loads(resp.read().decode("utf-8"))

        # Feishu returns {"code": 0, "msg": "success"} on success
        code = resp_body.get("code", -1)
        if code != 0:
            logger.error(
                "Feishu webhook returned error: code=%s msg=%s",
                code, resp_body.get("msg", "unknown"),
            )
            return False

        logger.info("Feishu webhook sent successfully: %s", title)
        return True

    except urllib.error.HTTPError as exc:
        logger.error(
            "Feishu webhook HTTP %d: %s (title=%s)",
            exc.code, exc.reason, title,
        )
    except urllib.error.URLError as exc:
        logger.error(
            "Feishu webhook network error: %s (title=%s)",
            exc.reason, title,
        )
    except Exception as exc:
        logger.error(
            "Feishu webhook unexpected error: %s (title=%s)",
            exc, title,
        )

    return False


# ── Convenience ───────────────────────────────────────────────────────────────

def send_if_configured(title: str, content: str,
                       header_color: str = "red") -> bool:
    """Send a message only if FEISHU_WEBHOOK_URL is set.  Returns success."""
    url = _get_webhook_url()
    if not url:
        return False
    return send_feishu_webhook(url, title, content, header_color)


# ── CLI test ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
    )
    url = _get_webhook_url()
    if not url:
        print("Set FEISHU_WEBHOOK_URL to test.")
    else:
        ok = send_feishu_webhook(
            url,
            "TRON Sentinel 测试消息",
            "**飞书 Webhook 连通测试**\n\n"
            "- 时间: 测试发送\n"
            "- 状态: 正常\n\n"
            "---\n\n"
            "Powered by TRON Sentinel",
        )
        print("OK" if ok else "FAILED")

"""
scripts/diagnose.py
~~~~~~~~~~~~~~~~~~~
TRON Sentinel全系统诊断工具。

检查项：
  1. SQLite数据库状态（记录数、来源分布、时间范围）
  2. RSS源可达性（逐一HTTP测试）
  3. TwitterAPI.io可达性
  4. Apify API可达性
  5. 环境变量配置情况（是/否，不显示值）
  6. 最近一次流水线执行日志片段

独立运行：
    python scripts/diagnose.py

由 entrypoint.py 的 GET /diagnose 路由调用，返回纯文本。
"""

import io
import os
import sqlite3
import sys
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT    = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "sentinel.db"
YAML_PATH      = ROOT / "config" / "rss_sources.yaml"
LOG_PATH       = ROOT / "data" / "pipeline.log"   # written by some deployments
RSS_TIMEOUT    = 10   # seconds per RSS source
API_TIMEOUT    = 15   # seconds for API tests

SEP  = "─" * 60
SEP2 = "═" * 60


# ── Helpers ────────────────────────────────────────────────────────────────────

def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _fmt_dt(ts: str | None) -> str:
    if not ts:
        return "N/A"
    return ts[:19].replace("T", " ") + " UTC"


def _load_rss_sources() -> list[dict]:
    """Return enabled RSS sources from config/rss_sources.yaml."""
    if not YAML_PATH.exists():
        return []
    try:
        import yaml  # pyyaml
        with YAML_PATH.open(encoding="utf-8") as fh:
            cfg = yaml.safe_load(fh)
        entries = cfg.get("sources", []) if isinstance(cfg, dict) else []
        return [e for e in entries if e.get("enabled", True) and e.get("url")]
    except Exception:
        # Fallback: naive regex parse without pyyaml
        import re
        text = YAML_PATH.read_text(encoding="utf-8")
        result, cur = [], {}
        for line in text.splitlines():
            m_name = re.match(r"\s+-?\s*name:\s*(.+)", line)
            m_url  = re.match(r"\s+url:\s*(.+)", line)
            m_en   = re.match(r"\s+enabled:\s*(true|false)", line)
            if m_name:
                if cur.get("url"):
                    result.append(cur)
                cur = {"name": m_name.group(1).strip(), "enabled": True}
            elif m_url and cur:
                cur["url"] = m_url.group(1).strip()
            elif m_en and cur:
                cur["enabled"] = m_en.group(1).strip() == "true"
        if cur.get("url"):
            result.append(cur)
        return [e for e in result if e.get("enabled", True)]


# ── Section 1: Database ────────────────────────────────────────────────────────

def _check_database(out: io.StringIO) -> None:
    out.write(f"\n{SEP}\n")
    out.write("  [1] 数据库状态  (data/sentinel.db)\n")
    out.write(f"{SEP}\n")

    if not DB_PATH.exists():
        out.write("  ✗  数据库文件不存在\n")
        return

    db_mb = round(DB_PATH.stat().st_size / 1_048_576, 2)
    out.write(f"  文件大小   : {db_mb} MB\n")

    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("PRAGMA journal_mode=WAL")
        now = _now_utc()

        # Total
        total = conn.execute("SELECT COUNT(*) FROM raw_articles").fetchone()[0]
        out.write(f"  总记录数   : {total:,} 条\n")

        # Last 24h
        cut_24h = (now - timedelta(hours=24)).isoformat()
        cnt_24h = conn.execute(
            "SELECT COUNT(*) FROM raw_articles WHERE collected_at >= ?", (cut_24h,)
        ).fetchone()[0]
        out.write(f"  近24h新增  : {cnt_24h:,} 条\n")

        # Newest / oldest
        row_new = conn.execute(
            "SELECT collected_at FROM raw_articles ORDER BY collected_at DESC LIMIT 1"
        ).fetchone()
        row_old = conn.execute(
            "SELECT collected_at FROM raw_articles ORDER BY collected_at ASC LIMIT 1"
        ).fetchone()
        out.write(f"  最新记录   : {_fmt_dt(row_new[0] if row_new else None)}\n")
        out.write(f"  最旧记录   : {_fmt_dt(row_old[0] if row_old else None)}\n")

        # Last 7 days daily breakdown
        out.write("\n  近7天每日记录数：\n")
        for d in range(6, -1, -1):
            day_start = (now - timedelta(days=d)).strftime("%Y-%m-%d")
            day_end   = (now - timedelta(days=d - 1)).strftime("%Y-%m-%d") if d > 0 else (now + timedelta(days=1)).strftime("%Y-%m-%d")
            cnt = conn.execute(
                "SELECT COUNT(*) FROM raw_articles "
                "WHERE collected_at >= ? AND collected_at < ?",
                (day_start, day_end),
            ).fetchone()[0]
            bar = "█" * min(cnt // 10, 40) if cnt else ""
            out.write(f"    {day_start}  {cnt:>6,} 条  {bar}\n")

        # By source (top 40)
        out.write("\n  按来源统计（所有来源）：\n")
        rows = conn.execute(
            "SELECT source, COUNT(*) AS cnt FROM raw_articles "
            "GROUP BY source ORDER BY cnt DESC"
        ).fetchall()
        for src, cnt in rows:
            out.write(f"    {src:<40}  {cnt:>7,} 条\n")

        conn.close()

    except Exception as exc:
        out.write(f"  ✗  数据库查询失败: {exc}\n")


# ── Section 2: RSS reachability ────────────────────────────────────────────────

def _check_rss(out: io.StringIO) -> None:
    out.write(f"\n{SEP}\n")
    out.write("  [2] RSS源可达性测试\n")
    out.write(f"{SEP}\n")

    sources = _load_rss_sources()
    if not sources:
        out.write("  ⚠  未找到启用的RSS源（config/rss_sources.yaml）\n")
        return

    out.write(f"  共 {len(sources)} 个启用的RSS源\n\n")
    ok_cnt = err_cnt = timeout_cnt = 0

    for src in sources:
        name = src.get("name") or src.get("source") or "?"
        url  = src.get("url", "")
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "TRON-Sentinel/1.0"})
            with urllib.request.urlopen(req, timeout=RSS_TIMEOUT) as resp:
                status = resp.status
            mark = "✓" if status == 200 else "⚠"
            out.write(f"  {mark} [{status}]  {name}\n")
            ok_cnt += 1
        except urllib.error.URLError as exc:
            reason = str(exc.reason)
            if "timed out" in reason.lower() or "timeout" in reason.lower():
                out.write(f"  ✗ [TIMEOUT]  {name}\n")
                timeout_cnt += 1
            else:
                out.write(f"  ✗ [ERR]  {name}  →  {reason[:80]}\n")
                err_cnt += 1
        except Exception as exc:
            out.write(f"  ✗ [ERR]  {name}  →  {str(exc)[:80]}\n")
            err_cnt += 1

    out.write(f"\n  汇总: 成功 {ok_cnt}  /  失败 {err_cnt}  /  超时 {timeout_cnt}\n")


# ── Section 3: Twitter API ─────────────────────────────────────────────────────

def _check_twitter(out: io.StringIO) -> None:
    out.write(f"\n{SEP}\n")
    out.write("  [3] TwitterAPI.io 可达性\n")
    out.write(f"{SEP}\n")

    api_key = os.environ.get("TWITTERAPI_KEY", "").strip()
    if not api_key:
        out.write("  –  TWITTERAPI_KEY 未配置，跳过测试\n")
        return

    import urllib.parse
    query  = "TRON TRX"
    params = urllib.parse.urlencode({"query": query, "queryType": "Latest"})
    url    = f"https://api.twitterapi.io/twitter/tweet/advanced_search?{params}"
    try:
        req = urllib.request.Request(
            url,
            headers={"X-API-Key": api_key, "Accept": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=API_TIMEOUT) as resp:
            import json
            data = json.loads(resp.read().decode("utf-8"))
        tweets = data.get("tweets") or data.get("data") or []
        out.write(f"  ✓  连接成功，返回推文数: {len(tweets)}\n")
    except urllib.error.HTTPError as exc:
        out.write(f"  ✗  HTTP {exc.code}: {exc.reason}\n")
    except urllib.error.URLError as exc:
        out.write(f"  ✗  连接失败: {exc.reason}\n")
    except Exception as exc:
        out.write(f"  ✗  错误: {exc}\n")


# ── Section 4: Apify API ───────────────────────────────────────────────────────

def _check_apify(out: io.StringIO) -> None:
    out.write(f"\n{SEP}\n")
    out.write("  [4] Apify API 可达性\n")
    out.write(f"{SEP}\n")

    token = os.environ.get("APIFY_API_TOKEN", "").strip()
    if not token:
        out.write("  –  APIFY_API_TOKEN 未配置，跳过测试\n")
        return

    import urllib.parse
    url = f"https://api.apify.com/v2/users/me?token={urllib.parse.quote(token)}"
    try:
        with urllib.request.urlopen(url, timeout=API_TIMEOUT) as resp:
            import json
            data = json.loads(resp.read().decode("utf-8"))
        username = data.get("data", {}).get("username") or data.get("username") or "?"
        plan     = data.get("data", {}).get("plan", {}).get("id") or "?"
        out.write(f"  ✓  连接成功  username={username}  plan={plan}\n")
    except urllib.error.HTTPError as exc:
        out.write(f"  ✗  HTTP {exc.code}: {exc.reason}\n")
    except urllib.error.URLError as exc:
        out.write(f"  ✗  连接失败: {exc.reason}\n")
    except Exception as exc:
        out.write(f"  ✗  错误: {exc}\n")


# ── Section 5: Environment variables ──────────────────────────────────────────

def _check_env(out: io.StringIO) -> None:
    out.write(f"\n{SEP}\n")
    out.write("  [5] 环境变量配置\n")
    out.write(f"{SEP}\n")

    vars_to_check = [
        ("TWITTERAPI_KEY",    "TwitterAPI.io 采集"),
        ("APIFY_API_TOKEN",   "Apify 多平台采集"),
        ("DASHSCOPE_API_KEY", "DashScope LLM分析"),
        ("FEISHU_WEBHOOK_URL","飞书告警/日报推送"),
        ("GCS_BUCKET",        "GCS Dashboard上传"),
    ]
    for var, desc in vars_to_check:
        val = os.environ.get(var, "").strip()
        status = "✓  已配置" if val else "✗  未配置"
        out.write(f"  {status:<12}  {var:<22}  ({desc})\n")


# ── Section 6: Pipeline log ────────────────────────────────────────────────────

def _check_log(out: io.StringIO) -> None:
    out.write(f"\n{SEP}\n")
    out.write("  [6] 最近流水线执行日志\n")
    out.write(f"{SEP}\n")

    if not LOG_PATH.exists():
        out.write(f"  –  日志文件不存在 ({LOG_PATH.name})\n")
        out.write("     （Cloud Run 日志可通过 gcloud logging 查看）\n")
        return

    try:
        lines = LOG_PATH.read_text(encoding="utf-8", errors="replace").splitlines()
        tail  = lines[-50:] if len(lines) > 50 else lines
        out.write(f"  文件路径: {LOG_PATH}\n")
        out.write(f"  显示最后 {len(tail)} 行（共 {len(lines)} 行）：\n\n")
        for line in tail:
            out.write(f"    {line}\n")
    except Exception as exc:
        out.write(f"  ✗  读取日志失败: {exc}\n")


# ── Public API ─────────────────────────────────────────────────────────────────

def run_diagnosis() -> str:
    """
    Execute all diagnostic checks and return the full report as a string.

    Called by entrypoint.py for the GET /diagnose HTTP endpoint.
    """
    out = io.StringIO()
    now = _now_utc()

    out.write(f"\n{SEP2}\n")
    out.write("  TRON Sentinel  ·  系统诊断报告\n")
    out.write(f"  {now.strftime('%Y-%m-%d  %H:%M:%S')} UTC\n")
    out.write(f"{SEP2}\n")

    _check_database(out)
    _check_rss(out)
    _check_twitter(out)
    _check_apify(out)
    _check_env(out)
    _check_log(out)

    out.write(f"\n{SEP2}\n")
    out.write(f"  诊断完成  {_now_utc().strftime('%H:%M:%S')} UTC\n")
    out.write(f"{SEP2}\n\n")

    return out.getvalue()


# ── CLI entry point ────────────────────────────────────────────────────────────

def main() -> None:
    report = run_diagnosis()
    print(report)
    # Exit non-zero if DB is missing (useful for CI smoke tests)
    sys.exit(0 if DB_PATH.exists() else 1)


if __name__ == "__main__":
    main()

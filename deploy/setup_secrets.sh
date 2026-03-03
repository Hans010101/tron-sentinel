#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# deploy/setup_secrets.sh
# TRON Sentinel – 将所有 API 密钥写入 Google Secret Manager
#
# 运行时会逐一提示输入每个密钥的值。
# 对于已存在的密钥，会追加一个新版本（旧版本保留，Cloud Run 始终读取 :latest）。
# 标记为 [可选] 的密钥可直接按 Enter 跳过。
#
# 用法：
#   cd /path/to/TRON-Sentinel
#   bash deploy/setup_secrets.sh
# ──────────────────────────────────────────────────────────────────────────────

set -euo pipefail

# ── 颜色输出 ───────────────────────────────────────────────────────────────────
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'
BOLD='\033[1m'; DIM='\033[2m'; NC='\033[0m'

log()  { echo -e "${CYAN}[$(date +%H:%M:%S)]${NC} $*"; }
ok()   { echo -e "${GREEN}  ✓${NC} $*"; }
skip() { echo -e "${YELLOW}  –${NC} $* ${DIM}（已跳过）${NC}"; }

# ── 项目检测 ───────────────────────────────────────────────────────────────────
PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null)}"
[[ -z "$PROJECT_ID" ]] && {
    echo "未检测到 GCP 项目 ID。请先运行：gcloud config set project YOUR_PROJECT_ID"
    exit 1
}

echo ""
echo -e "${BOLD}══════════════════════════════════════════════════════════════${NC}"
echo -e "${BOLD}  TRON Sentinel – API 密钥配置（Google Secret Manager）${NC}"
echo -e "${BOLD}══════════════════════════════════════════════════════════════${NC}"
echo "  项目 ID : $PROJECT_ID"
echo ""
echo -e "  ${DIM}[必填] 字段若留空将导致相应采集器无法正常运行${NC}"
echo -e "  ${DIM}[可选] 字段直接按 Enter 跳过，不影响其他功能${NC}"
echo ""

# ── 核心函数 ───────────────────────────────────────────────────────────────────

# 确保 Secret Manager API 已启用
gcloud services enable secretmanager.googleapis.com \
    --project="$PROJECT_ID" --quiet

# 写入或更新密钥
# 用法：upsert_secret SECRET_NAME "secret value"
upsert_secret() {
    local name="$1"
    local value="$2"

    if gcloud secrets describe "$name" \
            --project="$PROJECT_ID" &>/dev/null 2>&1; then
        # 密钥已存在 → 追加新版本
        printf '%s' "$value" \
            | gcloud secrets versions add "$name" \
                --data-file=- \
                --project="$PROJECT_ID" --quiet
        ok "已更新：$name"
    else
        # 密钥不存在 → 创建
        printf '%s' "$value" \
            | gcloud secrets create "$name" \
                --data-file=- \
                --replication-policy=automatic \
                --project="$PROJECT_ID" --quiet
        ok "已创建：$name"
    fi
}

# 交互式读取密钥（支持跳过）
# 用法：read_secret VAR_NAME "提示文字" required|optional
read_secret() {
    local var_name="$1"
    local prompt="$2"
    local required="${3:-required}"

    local suffix=""
    [[ "$required" == "optional" ]] && suffix=" ${DIM}[可选，Enter 跳过]${NC}"

    echo -e "  ${BOLD}${var_name}${NC}${suffix}"
    echo -e "  ${DIM}${prompt}${NC}"
    read -r -s -p "  → 输入值：" value
    echo ""   # 换行（read -s 不换行）

    if [[ -z "$value" ]]; then
        skip "$var_name"
    else
        upsert_secret "$var_name" "$value"
    fi
    echo ""
}

# ── Telegram（必填）────────────────────────────────────────────────────────────
echo -e "${BOLD}── Telegram ──────────────────────────────────────────────────${NC}"
echo ""

read_secret "TELEGRAM_API_ID" \
    "整数 App ID，从 https://my.telegram.org/apps 获取" \
    required

read_secret "TELEGRAM_API_HASH" \
    "32 位十六进制字符串，与 API_ID 来自同一页面" \
    required

read_secret "TELEGRAM_BOT_TOKEN" \
    "Bot Token，从 Telegram @BotFather 的 /newbot 命令获取
  格式：123456789:AABBCC...
  用途：Telegram 预警发送 + 频道采集（可替代用户 Session）" \
    required

read_secret "TELEGRAM_CHAT_ID" \
    "预警消息发送到的聊天 / 频道 ID
  个人聊天 ID：与 @userinfobot 对话获取
  频道 ID：通常以 -100 开头（如 -1001234567890）" \
    required

# ── Twitter / X（可选）────────────────────────────────────────────────────────
echo -e "${BOLD}── Twitter / X（可选，Twitter 采集器预留）────────────────────${NC}"
echo ""

read_secret "TWITTER_BEARER_TOKEN" \
    "Bearer Token，从 https://developer.twitter.com 的 App 详情页获取
  Twitter 采集器尚未激活，此密钥留空不影响当前功能" \
    optional

# ── Reddit（可选）─────────────────────────────────────────────────────────────
echo -e "${BOLD}── Reddit（可选，Reddit 采集器预留）─────────────────────────${NC}"
echo ""

read_secret "REDDIT_CLIENT_ID" \
    "Reddit App Client ID，从 https://www.reddit.com/prefs/apps 获取" \
    optional

read_secret "REDDIT_CLIENT_SECRET" \
    "Reddit App Client Secret，与 Client ID 来自同一页面" \
    optional

# ── 完成 ───────────────────────────────────────────────────────────────────────
echo -e "${BOLD}══════════════════════════════════════════════════════════════${NC}"
echo ""
log "密钥配置完成，查看已存储的密钥列表："
gcloud secrets list --project="$PROJECT_ID" \
    --filter="name:TELEGRAM OR name:TWITTER OR name:REDDIT" \
    --format="table(name,createTime)"
echo ""
echo "  下一步：运行 bash deploy/deploy.sh 执行部署"
echo ""

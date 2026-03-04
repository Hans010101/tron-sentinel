#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# deploy/deploy.sh
# TRON Sentinel – 一键部署到 Google Cloud
#
# 执行内容（按顺序）：
#   1. 启用所需 GCP API
#   2. 创建专用服务账号并授权
#   3. gcloud builds submit  – 构建镜像并推送到 Artifact Registry
#   4. gcloud run deploy     – 部署 Cloud Run 后端服务（常驻运行）
#   5. gcloud storage        – 创建 GCS 存储桶，部署仪表盘静态网站
#
# 用法：
#   cd /path/to/TRON-Sentinel
#   bash deploy/deploy.sh
#
# 可选环境变量（覆盖默认值）：
#   PROJECT_ID   – GCP 项目 ID（默认：gcloud config 当前项目）
#   REGION       – 部署区域（默认：asia-east1，台湾，适合亚太用户）
#   SERVICE_NAME – Cloud Run 服务名（默认：tron-sentinel）
# ──────────────────────────────────────────────────────────────────────────────

set -euo pipefail

# ── 颜色输出 ───────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

log()  { echo -e "${CYAN}[$(date +%H:%M:%S)]${NC} $*"; }
ok()   { echo -e "${GREEN}  ✓${NC} $*"; }
warn() { echo -e "${YELLOW}  ⚠${NC} $*"; }
die()  { echo -e "${RED}  ✗ 错误：${NC}$*" >&2; exit 1; }

# ── 配置 ───────────────────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null)}"
REGION="${REGION:-asia-east1}"
SERVICE_NAME="${SERVICE_NAME:-tron-sentinel}"

# 派生名称
SA_NAME="${SERVICE_NAME}-sa"
SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
BUCKET="${PROJECT_ID}-tron-sentinel"
# Artifact Registry 仓库（比旧版 Container Registry 更推荐）
AR_REPO="tron-sentinel-repo"
IMAGE="$REGION-docker.pkg.dev/${PROJECT_ID}/${AR_REPO}/${SERVICE_NAME}:latest"

# ── 前置检查 ───────────────────────────────────────────────────────────────────
[[ -z "$PROJECT_ID" ]] && die "未检测到 GCP 项目 ID。\n  请先运行：gcloud config set project YOUR_PROJECT_ID"

echo ""
echo -e "${BOLD}══════════════════════════════════════════════════════════════${NC}"
echo -e "${BOLD}  TRON Sentinel – Google Cloud 一键部署${NC}"
echo -e "${BOLD}══════════════════════════════════════════════════════════════${NC}"
echo "  项目 ID  : $PROJECT_ID"
echo "  区域     : $REGION"
echo "  服务名   : $SERVICE_NAME"
echo "  镜像     : $IMAGE"
echo "  GCS 桶   : $BUCKET"
echo ""

read -r -p "  确认以上配置并继续部署？[y/N] " confirm
[[ "$confirm" =~ ^[Yy]$ ]] || { echo "已取消。"; exit 0; }
echo ""

# ── 步骤 1：启用 GCP API ───────────────────────────────────────────────────────
log "步骤 1/5  启用必要的 GCP API…"
gcloud services enable \
    cloudbuild.googleapis.com \
    run.googleapis.com \
    secretmanager.googleapis.com \
    storage.googleapis.com \
    artifactregistry.googleapis.com \
    iam.googleapis.com \
    --project="$PROJECT_ID" --quiet
ok "API 启用完成"

# ── 步骤 2：服务账号与 IAM ─────────────────────────────────────────────────────
log "步骤 2/5  配置服务账号…"

if ! gcloud iam service-accounts describe "$SA_EMAIL" \
        --project="$PROJECT_ID" &>/dev/null 2>&1; then
    gcloud iam service-accounts create "$SA_NAME" \
        --display-name="TRON Sentinel Service Account" \
        --project="$PROJECT_ID"
    ok "服务账号已创建：$SA_EMAIL"
else
    ok "服务账号已存在：$SA_EMAIL"
fi

# 授权：读取 Secret Manager
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="roles/secretmanager.secretAccessor" \
    --condition=None --quiet

# 授权：写入 GCS（用于上传 data.json）
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="roles/storage.objectAdmin" \
    --condition=None --quiet

ok "IAM 权限配置完成"

# ── 步骤 3：构建 Docker 镜像 ───────────────────────────────────────────────────
log "步骤 3/5  构建并推送 Docker 镜像…"

# 确保 Artifact Registry 仓库存在
if ! gcloud artifacts repositories describe "$AR_REPO" \
        --location="$REGION" --project="$PROJECT_ID" &>/dev/null 2>&1; then
    gcloud artifacts repositories create "$AR_REPO" \
        --repository-format=docker \
        --location="$REGION" \
        --description="TRON Sentinel container images" \
        --project="$PROJECT_ID"
    ok "Artifact Registry 仓库已创建"
fi

# 配置 Docker 认证
gcloud auth configure-docker "${REGION}-docker.pkg.dev" --quiet

# 提交构建（使用项目根目录的 Dockerfile）
cd "$PROJECT_ROOT"
gcloud builds submit \
    --tag "$IMAGE" \
    --project="$PROJECT_ID" \
    --timeout=20m \
    .
ok "镜像构建并推送完成：$IMAGE"

# ── 步骤 4：部署 Cloud Run 服务 ────────────────────────────────────────────────
log "步骤 4/5  部署 Cloud Run 服务…"

# 动态构建 --set-secrets 参数（仅包含实际存在的密钥）
SECRET_PAIRS=()
for secret_name in \
    TELEGRAM_API_ID TELEGRAM_API_HASH \
    TELEGRAM_BOT_TOKEN TELEGRAM_CHAT_ID; do
    if gcloud secrets describe "$secret_name" \
            --project="$PROJECT_ID" &>/dev/null 2>&1; then
        SECRET_PAIRS+=("${secret_name}=${secret_name}:latest")
    else
        warn "Secret '$secret_name' 未找到，跳过注入"
    fi
done

# 组合成逗号分隔字符串
SECRETS_FLAG=""
if [[ ${#SECRET_PAIRS[@]} -gt 0 ]]; then
    SECRETS_FLAG=$(printf '%s,' "${SECRET_PAIRS[@]}")
    SECRETS_FLAG="${SECRETS_FLAG%,}"   # 去掉末尾逗号
fi

# 构建 gcloud run deploy 命令
DEPLOY_ARGS=(
    "$SERVICE_NAME"
    --image "$IMAGE"
    --region "$REGION"
    --platform managed
    --service-account "$SA_EMAIL"
    --min-instances 1
    --max-instances 1
    --memory 512Mi
    --cpu 1
    --timeout 300
    --concurrency 1
    --set-env-vars "GCS_BUCKET=${BUCKET}"
    --no-allow-unauthenticated
    --project "$PROJECT_ID"
    --quiet
)
[[ -n "$SECRETS_FLAG" ]] && DEPLOY_ARGS+=(--set-secrets "$SECRETS_FLAG")

gcloud run deploy "${DEPLOY_ARGS[@]}"

SERVICE_URL=$(gcloud run services describe "$SERVICE_NAME" \
    --region "$REGION" --project "$PROJECT_ID" \
    --format="value(status.url)")
ok "Cloud Run 服务已部署：$SERVICE_URL"

# ── 步骤 5：GCS 静态网站（仪表盘前端） ────────────────────────────────────────
log "步骤 5/5  配置 GCS 静态网站…"

# 创建存储桶（已存在时跳过）
if ! gcloud storage buckets describe "gs://${BUCKET}" \
        --project="$PROJECT_ID" &>/dev/null 2>&1; then
    gcloud storage buckets create "gs://${BUCKET}" \
        --location="$REGION" \
        --project="$PROJECT_ID" \
        --uniform-bucket-level-access
    ok "GCS 存储桶已创建：gs://${BUCKET}"
else
    ok "GCS 存储桶已存在：gs://${BUCKET}"
fi

# 配置静态网站（主页 + 404 均指向 index.html）
gcloud storage buckets update "gs://${BUCKET}" \
    --web-main-page-suffix=index.html \
    --web-error-page=index.html

# 配置 CORS（允许浏览器从相同存储桶 fetch data.json）
cat > /tmp/tron_sentinel_cors.json << 'CORS_EOF'
[
  {
    "origin": ["*"],
    "method": ["GET", "HEAD"],
    "responseHeader": ["Content-Type", "Cache-Control"],
    "maxAgeSeconds": 300
  }
]
CORS_EOF
gcloud storage buckets update "gs://${BUCKET}" \
    --cors-file=/tmp/tron_sentinel_cors.json
rm -f /tmp/tron_sentinel_cors.json

# 设为公开可读（allUsers 查看对象）
gcloud storage buckets add-iam-policy-binding "gs://${BUCKET}" \
    --member="allUsers" \
    --role="roles/storage.objectViewer" \
    --project="$PROJECT_ID"

# 上传仪表盘 HTML（注意：data.json 由 Cloud Run 定时写入）
gcloud storage cp "${PROJECT_ROOT}/dashboard/index.html" \
    "gs://${BUCKET}/index.html" \
    --cache-control="no-cache, max-age=0"
ok "仪表盘 HTML 已上传"

# ── 完成，打印摘要 ─────────────────────────────────────────────────────────────
DASHBOARD_URL="https://storage.googleapis.com/${BUCKET}/index.html"

echo ""
echo -e "${BOLD}══════════════════════════════════════════════════════════════${NC}"
echo -e "${BOLD}  ✅  部署完成！${NC}"
echo -e "${BOLD}══════════════════════════════════════════════════════════════${NC}"
echo -e "  后端服务  : ${CYAN}${SERVICE_URL}${NC}"
echo -e "  仪表盘    : ${CYAN}${DASHBOARD_URL}${NC}"
echo ""
echo -e "  ${YELLOW}注意：${NC}首次部署后需等待第一次定时任务运行完毕（约 1 分钟），"
echo -e "        data.json 才会出现，仪表盘才显示真实数据。"
echo -e "  查看日志：gcloud run services logs read $SERVICE_NAME --region $REGION"
echo ""

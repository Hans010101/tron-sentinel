# TRON Sentinel – Google Cloud 部署指南

本指南介绍如何将 TRON Sentinel 部署到 Google Cloud Platform（GCP）。
整个部署在单一 GCP 项目内完成，无需其他云厂商。

---

## 部署架构

```
┌─────────────────────────────────────────────────────────────────┐
│                        Google Cloud Platform                    │
│                                                                 │
│  ┌──────────────────────┐      ┌───────────────────────────┐   │
│  │   Cloud Run (后端)    │──写──▶│  Cloud Storage (前端)     │   │
│  │                      │      │                           │   │
│  │  entrypoint.py       │      │  index.html  (仪表盘)     │   │
│  │  └─ scheduler.py     │      │  data.json   (数据文件)   │   │
│  │     └─ main.py       │      │                           │   │
│  │        ├─ RSS采集     │      └───────────┬───────────────┘   │
│  │        ├─ TG采集      │                  │ 公开访问           │
│  │        ├─ GNews采集   │                  ▼                   │
│  │        ├─ CoinGecko  │      浏览器 fetch('./data.json')      │
│  │        ├─ DeFiLlama  │                                       │
│  │        ├─ VADER分析  │                                       │
│  │        └─ TG预警     │                                       │
│  │                      │      ┌───────────────────────────┐   │
│  │  min-instances: 1    │◀─读──│  Secret Manager           │   │
│  │  memory: 512Mi       │      │  (API 密钥安全存储)        │   │
│  └──────────────────────┘      └───────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

| 组件 | 用途 | 运行方式 |
|------|------|----------|
| **Cloud Run** | 后端调度服务 | 常驻容器，每 30 分钟执行一次完整数据流水线 |
| **Cloud Storage** | 前端静态网站 | 托管 `index.html` + `data.json`，全球 CDN |
| **Secret Manager** | 密钥管理 | 所有 API Token 加密存储，Cloud Run 启动时注入 |
| **Artifact Registry** | 镜像仓库 | 存储 Docker 镜像（替代旧版 Container Registry） |
| **Cloud Build** | CI/CD | 在云端构建 Docker 镜像（无需本地 Docker） |

---

## 前置条件

### 1. 安装 gcloud CLI

```bash
# macOS（Homebrew）
brew install --cask google-cloud-sdk

# Linux（官方安装脚本）
curl https://sdk.cloud.google.com | bash
exec -l $SHELL

# Windows
# 下载安装包：https://cloud.google.com/sdk/docs/install
```

验证安装：
```bash
gcloud version   # 应显示 Google Cloud SDK 版本
```

### 2. 登录 Google Cloud

```bash
# 登录账号
gcloud auth login

# 登录用于应用凭证（Cloud Run 本地测试时用到）
gcloud auth application-default login
```

### 3. 创建或选择 GCP 项目

```bash
# 列出已有项目
gcloud projects list

# 设置当前项目（替换 YOUR_PROJECT_ID）
gcloud config set project YOUR_PROJECT_ID

# 若需要创建新项目
gcloud projects create YOUR_PROJECT_ID --name="TRON Sentinel"
gcloud config set project YOUR_PROJECT_ID
```

> **重要**：Cloud Run 需要启用计费。请在 [GCP 控制台](https://console.cloud.google.com/billing) 为项目绑定付款方式。
> 正常使用量在免费额度内几乎零费用（详见下方费用估算）。

### 4. 准备 API 密钥（部署前）

| 密钥 | 获取地址 | 是否必填 |
|------|----------|----------|
| `TELEGRAM_API_ID` | https://my.telegram.org/apps | **必填** |
| `TELEGRAM_API_HASH` | https://my.telegram.org/apps | **必填** |
| `TELEGRAM_BOT_TOKEN` | Telegram @BotFather → `/newbot` | **必填** |
| `TELEGRAM_CHAT_ID` | 与 @userinfobot 对话获取 | **必填** |
| `TWITTER_BEARER_TOKEN` | https://developer.twitter.com | 可选 |
| `REDDIT_CLIENT_ID` | https://www.reddit.com/prefs/apps | 可选 |
| `REDDIT_CLIENT_SECRET` | 同上 | 可选 |

---

## 第一步：配置 API 密钥

在项目根目录运行：

```bash
cd /path/to/TRON-Sentinel
bash deploy/setup_secrets.sh
```

脚本会逐一提示输入每个密钥值，**输入时不会在终端显示**（安全隐藏）。

**示例交互：**
```
  TELEGRAM_API_ID
  整数 App ID，从 https://my.telegram.org/apps 获取
  → 输入值：（在此输入，不显示）

  ✓ 已创建：TELEGRAM_API_ID

  TELEGRAM_API_HASH
  ...
```

可选密钥（Twitter、Reddit）直接按 **Enter** 跳过，不影响当前采集功能。

---

## 第二步：一键部署

```bash
cd /path/to/TRON-Sentinel
bash deploy/deploy.sh
```

脚本会确认配置后自动完成：
1. 启用必要的 GCP API（约 1 分钟）
2. 创建专用服务账号并配置 IAM 权限
3. 在云端构建 Docker 镜像（约 5-8 分钟）
4. 部署 Cloud Run 服务（约 1-2 分钟）
5. 创建 GCS 存储桶并上传仪表盘 HTML

**自定义区域（可选）：**
```bash
# 部署到新加坡（更接近东南亚用户）
REGION=asia-southeast1 bash deploy/deploy.sh

# 部署到美国东部
REGION=us-east1 bash deploy/deploy.sh
```

---

## 第三步：访问仪表盘

部署完成后终端会输出两个 URL：

```
══════════════════════════════════════════════════════════════
  ✅  部署完成！
══════════════════════════════════════════════════════════════
  后端服务  : https://tron-sentinel-xxxx-de.a.run.app
  仪表盘    : https://storage.googleapis.com/PROJECT_ID-tron-sentinel/index.html
```

- **后端服务 URL**：Cloud Run 的健康检查端点，可直接访问验证容器是否在线
- **仪表盘 URL**：公开可访问的静态网站

> **首次数据延迟**：容器启动后会立即执行第一次流水线（约 1-3 分钟），
> 之后 `data.json` 写入 GCS，刷新仪表盘即可看到真实数据。

---

## 更新部署

修改代码后，只需重新运行部署脚本：

```bash
bash deploy/deploy.sh
```

脚本是幂等的，可以安全地多次运行。

**仅更新仪表盘 HTML（不重建镜像）：**
```bash
PROJECT_ID=$(gcloud config get-value project)
gcloud storage cp dashboard/index.html \
    "gs://${PROJECT_ID}-tron-sentinel/index.html" \
    --cache-control="no-cache, max-age=0"
```

---

## 费用估算

以下估算基于 Google Cloud 2025 年定价，适用于个人/小团队日常运行。

| 服务 | 用量（典型） | 月费用 |
|------|-------------|--------|
| **Cloud Run** – 最小实例 1 | 730 小时/月（常驻） | ~$5–8 |
| **Cloud Build** – 镜像构建 | 5 次构建 × 5 分钟 | 免费（120 分钟/天） |
| **Cloud Storage** | <1 MB 数据 + 少量请求 | <$0.01 |
| **Secret Manager** | <1000 次访问/月 | 免费（6 个密钥版本） |
| **Artifact Registry** | <1 GB 镜像存储 | ~$0.10 |
| **合计** | | **约 $5–10 /月** |

> **节省费用提示**：
> - 如果不需要 24/7 运行，可以设 `--min-instances=0`，Cold Start 约 10-15 秒
> - 每月前 200 万次 Cloud Run 请求和 360,000 CPU 秒免费
> - 中国大陆访问 GCS 静态网站可能需要 VPN

---

## 查看运行日志

```bash
# 查看最新 50 行日志
gcloud run services logs read tron-sentinel \
    --region=asia-east1 --limit=50

# 实时追踪日志（类似 tail -f）
gcloud run services logs tail tron-sentinel \
    --region=asia-east1

# 在 GCP 控制台查看（可视化）
# https://console.cloud.google.com/run/detail/asia-east1/tron-sentinel/logs
```

---

## 常见报错与解决方法

### ❌ `ERROR: (gcloud.config.get-value) ...未设置项目`

```bash
gcloud config set project YOUR_PROJECT_ID
```

---

### ❌ `Permission denied: ...requires billing`

Cloud Run 需要已启用计费的账号。

1. 前往 https://console.cloud.google.com/billing
2. 创建或绑定付款账号到当前项目

---

### ❌ `ERROR: (gcloud.builds.submit) INVALID_ARGUMENT: ...`

原因：Artifact Registry 仓库不存在，或区域不匹配。

```bash
# 手动创建仓库
gcloud artifacts repositories create tron-sentinel-repo \
    --repository-format=docker \
    --location=asia-east1 \
    --description="TRON Sentinel images"
```

---

### ❌ Cloud Run 容器启动后立即重启

原因：容器启动时未能监听 PORT。

```bash
# 查看详细错误
gcloud run services logs read tron-sentinel --region=asia-east1 --limit=20

# 常见原因：
# 1. Python 依赖安装失败（检查 requirements-prod.txt 中的包版本）
# 2. 代码语法错误（本地运行 python -m py_compile main.py 检查）
# 3. 密钥未配置（TELEGRAM_API_ID 等为空时某些模块会报错）
```

---

### ❌ `Secret [TELEGRAM_API_ID] not found`

先运行 `setup_secrets.sh` 配置密钥，再执行 `deploy.sh`。

---

### ❌ 仪表盘页面显示"模拟数据"

`data.json` 尚未生成，等待第一次定时任务执行完毕（容器启动后约 1-3 分钟）。

可查看日志确认：
```bash
gcloud run services logs tail tron-sentinel --region=asia-east1
# 出现 "Dashboard JSON uploaded → gs://..." 即为成功
```

---

### ❌ `TELEGRAM_API_HASH` 或 `TELEGRAM_API_ID` 无效

- API ID 必须是整数（如 `12345678`），不含引号
- API Hash 是 32 位十六进制字符串
- 来源：https://my.telegram.org/apps（需要手机验证登录）

---

### ❌ `gsutil: command not found`

`gcloud storage` 命令已包含在新版 gcloud SDK 中，无需单独安装 `gsutil`。
如果系统提示找不到命令，请升级 gcloud：

```bash
gcloud components update
```

---

## 清理（删除所有资源）

```bash
PROJECT_ID=$(gcloud config get-value project)
REGION=asia-east1
SERVICE_NAME=tron-sentinel
BUCKET="${PROJECT_ID}-tron-sentinel"

# 删除 Cloud Run 服务
gcloud run services delete $SERVICE_NAME --region=$REGION --quiet

# 删除 GCS 存储桶（及其中的所有文件）
gcloud storage rm -r "gs://${BUCKET}"

# 删除 Artifact Registry 仓库
gcloud artifacts repositories delete tron-sentinel-repo \
    --location=$REGION --quiet

# 删除 Secret Manager 密钥
for secret in TELEGRAM_API_ID TELEGRAM_API_HASH \
              TELEGRAM_BOT_TOKEN TELEGRAM_CHAT_ID \
              TWITTER_BEARER_TOKEN REDDIT_CLIENT_ID REDDIT_CLIENT_SECRET; do
    gcloud secrets delete $secret --quiet 2>/dev/null || true
done

# 删除服务账号
gcloud iam service-accounts delete \
    "${SERVICE_NAME}-sa@${PROJECT_ID}.iam.gserviceaccount.com" --quiet

echo "清理完成。"
```

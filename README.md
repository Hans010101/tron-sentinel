# TRON Sentinel

**TRON Sentinel** 是针对 TRON 生态的**全网实时舆情监控系统**。
通过 18 步自动化流水线，从 12 个数据源采集内容，经 AI 情感分析、LLM 深度解读和风险评分后，推送至实时仪表盘并触发飞书告警。

---

## 目录

- [系统架构](#系统架构)
- [功能清单](#功能清单)
- [快速开始](#快速开始)
- [环境变量](#环境变量)
- [配置文件](#配置文件)
- [Dashboard](#dashboard)
- [健康检查](#健康检查)
- [日常运维](#日常运维)
- [目录结构](#目录结构)

---

## 系统架构

```
┌─────────────────────────────────────────────────────────────────────┐
│                    TRON Sentinel  18-Step Pipeline                   │
│                                                                     │
│  ┌──────────────── 采集层 (Step 1-12) ─────────────────────────┐   │
│  │  RSS News (24源)  Twitter/X   Google News   YouTube          │   │
│  │  Reddit    TikTok   Weibo   Bilibili   CoinGecko             │   │
│  │  DeFiLlama   Baidu News   CryptoPanic                        │   │
│  └───────────────────────────┬─────────────────────────────────┘   │
│                               │ raw_articles / market_data          │
│  ┌──────────────── 分析层 (Step 13-17) ────────────────────────┐   │
│  │  情感分析 → LLM深度分析 → 风险评分 → 即时告警 → 趋势分析   │   │
│  └───────────────────────────┬─────────────────────────────────┘   │
│                               │ sentiment_label / risk_score /      │
│                               │ llm_summary_zh / trend_data         │
│  ┌──────────────── 存储层 ─────────────────────────────────────┐   │
│  │              SQLite  data/sentinel.db  (WAL mode)            │   │
│  └───────────────────────────┬─────────────────────────────────┘   │
│                               │                                     │
│  ┌──── 展示层 ────┐   ┌───── 告警层 (Step 16, 18) ──────────┐   │
│  │ dashboard/     │   │  即时告警（风险≥80）  飞书日报推送    │   │
│  │ index.html     │   │  三份日报：中文/英文/市场             │   │
│  │ /data.json     │   └────────────────────────────────────────┘   │
│  └────────────────┘                                                 │
└─────────────────────────────────────────────────────────────────────┘
```

**Pipeline 步骤总览：**

| 步骤 | 名称 | 说明 |
|------|------|------|
| 1 | RSS 新闻采集 | 24 个 RSS 源（CoinDesk、金色财经等），由 `config/rss_sources.yaml` 管理 |
| 2 | Apify Twitter/X | Justin Sun、孙宇晨、TRON 相关推文 |
| 3 | Apify Google News | Justin Sun、TRON、波场新闻搜索 |
| 4 | Apify YouTube | 视频搜索采集 |
| 5 | Apify Reddit | TRON TRX、Justin Sun 帖子 |
| 6 | Apify TikTok | 短视频采集 |
| 7 | Apify 微博 | 波场、孙宇晨相关微博 |
| 8 | Bilibili | 视频搜索 |
| 9 | CoinGecko | TRX 市场数据（价格、24h 涨跌、市值、成交量） |
| 10 | DeFiLlama | TRON TVL 数据 |
| 11 | Baidu News | 百度新闻 RSS |
| 12 | CryptoPanic | TRX 专题新闻 API |
| 13 | 情感分析 | 关键词分类（正面/中性/负面） |
| 14 | LLM 深度分析 | DashScope Deepseek-v3：板块、摘要、风险级别 |
| 15 | 风险评分 | 综合 0-100 风险评分 |
| 16 | 即时告警 | 风险≥80 的文章推送飞书 Webhook |
| 17 | 趋势分析 | 7天声量/情感趋势、热词榜、异常检测 |
| 18 | 日报推送 | 三份飞书日报（中文/英文/市场） |
| 19 | 数据清理 | 删除30天前记录，VACUUM 压缩数据库 |

---

## 功能清单

### 采集层（12 个采集器）
- **RSS 新闻**：24 个权威加密媒体源，支持 YAML 配置化管理
- **社交媒体**：Twitter/X、Reddit、TikTok、微博（via Apify）
- **视频平台**：YouTube、Bilibili
- **搜索引擎**：Google News、Baidu News
- **专题 API**：CryptoPanic TRX 新闻、CoinGecko 市场数据、DeFiLlama TVL

### 分析层
- **情感分析**：关键词规则分类，0 延迟，无 API 成本
- **LLM 深度分析**：Deepseek-v3 对文章进行板块分类、中文摘要、风险定级
- **风险评分**：综合情感分数 × 来源权威性 × 互动量 × LLM 判断，0-100 分
- **趋势分析**：7 天声量/情感趋势、24 小时热词 Top 20、声量激增/负面激增异常检测

### 展示层
- 实时仪表盘（`dashboard/index.html`）
- 7 天趋势图（声量柱状图 + 情感折线图）
- 热词排行榜 Top 20
- 风险预警面板（实时更新）
- 平台分布图、情感24小时趋势、市场数据

### 告警层
- **即时告警**：风险评分 ≥ 80 触发飞书 Webhook 推送
- **日报**：每次流水线结束后生成并推送三份报告

---

## 快速开始

### 本地运行

```bash
# 1. 克隆仓库
git clone https://github.com/Hans010101/TRON-Sentinel.git
cd TRON-Sentinel

# 2. 安装依赖（建议 Python 3.11+）
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# 3. 配置环境变量
cp .env.example .env             # 如有；或直接编辑 .env
# 至少填写 DASHSCOPE_API_KEY 和 FEISHU_WEBHOOK_URL

# 4. 运行流水线（单次）
python main.py

# 5. 查看 Dashboard（需要 HTTP 服务器）
python -m http.server 8080
# 访问 http://localhost:8080/dashboard/
```

### Docker 运行

```bash
# 构建镜像
docker build -t tron-sentinel .

# 运行（挂载数据目录，传入环境变量）
docker run -d \
  -p 8080:8080 \
  -v $(pwd)/data:/app/data \
  -e DASHSCOPE_API_KEY=your_key \
  -e FEISHU_WEBHOOK_URL=your_webhook \
  --name sentinel \
  tron-sentinel

# 访问
# Dashboard: http://localhost:8080/
# 健康检查: http://localhost:8080/health
# 数据 API: http://localhost:8080/data.json
```

### Cloud Run 部署

项目已配置 CI/CD，推送到 `main` 分支后自动部署至 Google Cloud Run。

```bash
# 手动部署
gcloud run deploy tron-sentinel \
  --source . \
  --region asia-east1 \
  --set-env-vars DASHSCOPE_API_KEY=xxx,FEISHU_WEBHOOK_URL=xxx \
  --allow-unauthenticated
```

---

## 环境变量

| 变量名 | 必填 | 说明 |
|--------|------|------|
| `DASHSCOPE_API_KEY` | 推荐 | 阿里云 DashScope API Key，用于 LLM 深度分析（Step 14）。不设置则跳过 LLM 步骤 |
| `FEISHU_WEBHOOK_URL` | 推荐 | 飞书群机器人 Webhook URL，用于即时告警（Step 16）和日报推送（Step 18）。不设置则跳过告警 |
| `APIFY_API_TOKEN` | 推荐 | Apify 平台 Token，用于 Twitter/X、Google News、YouTube、Reddit、TikTok、微博等社交媒体采集 |
| `GCS_BUCKET` | 可选 | Google Cloud Storage Bucket 名称。设置后 `data.json` 会同步上传至 GCS，供静态托管 Dashboard 使用 |
| `PORT` | 可选 | HTTP 服务端口（默认 `8080`，Cloud Run 自动注入） |

本地开发可在项目根目录创建 `.env` 文件（自动读取）：

```env
DASHSCOPE_API_KEY=sk-xxx
FEISHU_WEBHOOK_URL=https://open.feishu.cn/open-apis/bot/v2/hook/xxx
APIFY_API_TOKEN=apify_api_xxx
```

---

## 配置文件

### `config/rss_sources.yaml` — RSS 源管理

管理所有 RSS 新闻源，支持热更新（重启流水线即生效）：

```yaml
sources:
  - name:     CoinDesk
    url:      https://feeds.feedburner.com/CoinDesk
    language: en
    category: 加密媒体
    enabled:  true

  - name:     金色财经
    url:      https://www.jinse.cn/rss
    language: zh
    category: 中文加密媒体
    enabled:  true
```

**常用操作：**
- **禁用某个源**：将 `enabled: true` 改为 `enabled: false`
- **添加新源**：在文件末尾追加一个 YAML 块，无需改代码
- **YAML 不存在**：自动回退到代码中的硬编码列表

### `config/keywords.yaml` — 关键词管理

控制 RSS 相关性过滤规则：

```yaml
primary_keywords:     # 标题含有任一词才采集
  - TRON
  - TRX
  - Justin Sun

noise_filters:        # 标题含有任一词则丢弃（优先级更高）
  - price prediction
  - sponsored
```

**常用操作：**
- 添加新监控词：在 `primary_keywords` 下追加
- 屏蔽噪音标题：在 `noise_filters` 下追加
- **YAML 不存在**：自动回退到代码中的硬编码关键词

---

## Dashboard

仪表盘通过 `entrypoint.py` 提供服务，实时展示：

| 面板 | 数据来源 |
|------|---------|
| 风险预警 | 近24h 风险评分 ≥ 60 的文章 |
| 舆情概览 | 今日声量、情感分布、活跃预警数 |
| 异常检测 | 声量激增 / 负面激增标签 |
| 7天趋势 | 声量柱状图 + 情感折线图 |
| 热词榜 | 近24h 标题高频词 Top 20 |
| AI 分析概览 | LLM 覆盖率、板块分布、内容分类 |
| 舆情地图 | 各平台文章数量及情感分布 |
| 最新动态 | 实时新闻列表，支持板块筛选 |
| 24h 情绪趋势 | 逐小时情感折线图 |
| 市场数据 | TRX 价格、24h 涨跌、市值、TVL |

数据每次流水线运行后自动更新（默认约30分钟）。

---

## 健康检查

```bash
# 命令行检查
python scripts/health_check.py

# JSON 格式（供机器读取）
python scripts/health_check.py --json

# HTTP 接口（服务运行时）
curl http://localhost:8080/health
```

返回示例：
```json
{
  "checked_at": "2025-01-15T08:30:00+00:00",
  "status": "healthy",
  "database": {
    "accessible": true,
    "total_articles": 15234,
    "articles_last_24h": 342,
    "last_collected_ago": "12m ago",
    "db_size_mb": 45.2
  },
  "collectors": {
    "rss": { "count_last_24h": 48, "last_success_ago": "28m ago" },
    "twitter": { "count_last_24h": 120, "last_success_ago": "28m ago" }
  },
  "llm": { "coverage_pct": 87.3, "analyzed": 13290, "total": 15234 }
}
```

**状态说明：**
- `healthy`：数据库可访问，近24小时有采集数据
- `degraded`：数据库可访问但近24小时无新数据
- `critical`：数据库不可访问或从未采集过数据

---

## 日常运维

### 添加 RSS 源

编辑 `config/rss_sources.yaml`，追加一条记录：

```yaml
  - name:     MyNewSource
    url:      https://example.com/feed.rss
    language: en
    category: 加密媒体
    enabled:  true
```

重启服务或等待下一次流水线运行，无需改代码。

### 调整监控关键词

编辑 `config/keywords.yaml`：

```yaml
primary_keywords:
  - TRON
  - TRX
  - 你要新增的词       # ← 追加这里

noise_filters:
  - price today
  - 你要屏蔽的噪音词   # ← 追加这里
```

### 查看运行日志

```bash
# 本地运行时日志直接输出到 stdout
python main.py 2>&1 | tee logs/run.log

# Cloud Run 查看日志
gcloud logs read --service tron-sentinel --limit 100
```

### 手动触发健康检查

```bash
python scripts/health_check.py
```

### 数据库维护

流水线每次运行的最后一步（Step 19）会自动：
- 删除 30 天前的 `raw_articles` 记录
- 删除 30 天前的 `trend_data` 记录
- 执行 `VACUUM` 压缩数据库文件

如需手动清理：

```bash
python -c "
import sqlite3
conn = sqlite3.connect('data/sentinel.db')
conn.execute(\"DELETE FROM raw_articles WHERE collected_at < date('now', '-30 days')\")
conn.execute(\"DELETE FROM trend_data WHERE date < date('now', '-30 days')\")
conn.execute('VACUUM')
conn.commit()
conn.close()
print('Done')
"
```

---

## 目录结构

```
TRON-Sentinel/
├── collectors/                  # 数据采集模块
│   ├── rss_collector.py         # RSS 新闻采集（24源，YAML配置化）
│   ├── apify_collector.py       # Twitter/X、Google、YouTube、Reddit、TikTok、微博
│   ├── bilibili_collector.py    # Bilibili 视频搜索
│   ├── coingecko_collector.py   # CoinGecko TRX 市场数据
│   ├── defillama_collector.py   # DeFiLlama TRON TVL
│   ├── baidu_collector.py       # 百度新闻 RSS
│   ├── crypto_panic_collector.py # CryptoPanic API
│   └── __init__.py
│
├── analyzers/                   # AI 分析模块
│   ├── sentiment_analyzer.py    # 关键词情感分类
│   ├── llm_analyzer.py          # DashScope Deepseek-v3 深度分析
│   ├── risk_scorer.py           # 综合风险评分（0-100）
│   ├── trend_analyzer.py        # 7天趋势分析 + 热词 + 异常检测
│   └── __init__.py
│
├── alerting/                    # 告警通知模块
│   ├── instant_alert.py         # 即时告警（风险≥80 → 飞书）
│   ├── alert_manager.py         # 告警规则引擎
│   ├── telegram_notifier.py     # Telegram Bot 推送
│   ├── email_notifier.py        # 邮件告警（SMTP）
│   └── webhook_notifier.py      # Webhook 推送
│
├── reporters/                   # 日报生成模块
│   └── daily_report.py          # 三份飞书日报（中/英/市场）
│
├── dashboard/                   # 前端仪表盘
│   ├── index.html               # 单页应用 SPA（纯 Vanilla JS）
│   └── data.json                # 流水线生成的数据文件（gitignored）
│
├── config/                      # 配置文件
│   ├── rss_sources.yaml         # RSS 源管理（24 个源）
│   ├── keywords.yaml            # 关键词管理（primary / noise_filters）
│   ├── settings.py              # Pydantic Settings 配置
│   ├── logging.yaml             # 日志配置
│   └── config.example.yaml      # 配置示例
│
├── scripts/                     # 运维工具脚本
│   └── health_check.py          # 系统健康检查（CLI + HTTP /health）
│
├── database/                    # 数据库层（SQLite，无 ORM 依赖）
│   ├── models.py
│   └── db_client.py
│
├── main.py                      # 流水线主入口（19步）
├── entrypoint.py                # Cloud Run HTTP 服务器
├── scheduler.py                 # APScheduler 定时任务
├── requirements.txt             # Python 依赖
├── Dockerfile                   # 容器构建
└── README.md                    # 本文档
```

**数据库表：**

| 表名 | 说明 |
|------|------|
| `raw_articles` | 所有采集的文章/推文/视频，含情感、LLM、风险评分字段 |
| `market_data` | CoinGecko & DeFiLlama 市场数据 |
| `trend_data` | 趋势分析结果（日声量、情感、热词、异常） |

---

## 许可证

MIT License

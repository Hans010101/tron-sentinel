# TRON Sentinel

**TRON Sentinel** 是一套针对 TRON 生态的加密货币**实时舆情监控系统**，通过多源数据采集、AI 情感分析与可视化仪表盘，帮助投资者和项目方及时感知市场情绪变化，并在关键阈值被触发时发出多渠道告警。

---

## 目录结构

```
TRON-Sentinel/
├── collectors/                  # 数据采集模块
│   ├── __init__.py
│   ├── twitter_collector.py     # Twitter/X 舆情采集（Twitter API v2）
│   ├── reddit_collector.py      # Reddit 讨论采集（PRAW）
│   ├── news_collector.py        # 加密新闻 RSS/API 采集
│   └── onchain_collector.py     # TRON 链上指标采集（TronGrid API）
│
├── analyzers/                   # AI 分析模块
│   ├── __init__.py
│   ├── sentiment_analyzer.py    # NLP 情感分类（HuggingFace Transformers）
│   ├── trend_analyzer.py        # 热点话题与关键词趋势检测
│   ├── risk_scorer.py           # 综合风险评分聚合器
│   └── llm_analyzer.py          # LLM 深度分析（Claude API）
│
├── database/                    # 数据存储模块
│   ├── __init__.py
│   ├── models.py                # SQLAlchemy ORM 数据模型
│   ├── db_client.py             # 数据库连接管理与查询封装
│   └── migrations/
│       └── 001_initial.sql      # 初始化 Schema 迁移脚本
│
├── alerting/                    # 告警通知模块
│   ├── __init__.py
│   ├── alert_manager.py         # 告警规则引擎与触发逻辑
│   ├── telegram_notifier.py     # Telegram Bot 推送
│   ├── email_notifier.py        # 邮件告警（SMTP）
│   └── webhook_notifier.py      # Webhook 推送（Slack / Discord 等）
│
├── dashboard/                   # 前端仪表盘模块
│   ├── __init__.py
│   ├── app.py                   # FastAPI 应用入口（含 WebSocket）
│   ├── api/
│   │   └── routes.py            # REST API 路由定义
│   └── static/
│       └── index.html           # 前端页面入口
│
├── config/                      # 配置模块
│   ├── __init__.py
│   ├── settings.py              # Pydantic Settings 配置加载器
│   ├── config.example.yaml      # 配置文件示例（请复制为 config.yaml）
│   └── logging.yaml             # 日志配置（RotatingFileHandler）
│
├── logs/                        # 运行日志（自动生成）
├── requirements.txt             # Python 依赖清单
└── README.md                    # 本文档
```

---

## 核心功能

| 模块 | 功能 |
|------|------|
| **collectors** | 多源数据采集：社交媒体、新闻、链上指标，支持定时轮询 |
| **analyzers** | 情感分析（RoBERTa）、热点趋势检测、LLM 深度解读（Claude） |
| **database** | PostgreSQL 持久化存储，SQLAlchemy ORM + Alembic 迁移 |
| **alerting** | 规则引擎驱动的多渠道告警：Telegram、Email、Webhook |
| **dashboard** | FastAPI + WebSocket 实时仪表盘，REST API 对外暴露数据 |
| **config** | YAML + 环境变量统一配置，Pydantic 类型校验 |

---

## 快速开始

### 1. 克隆仓库

```bash
git clone https://github.com/Hans010101/TRON-Sentinel.git
cd TRON-Sentinel
```

### 2. 安装依赖

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 3. 配置

```bash
cp config/config.example.yaml config/config.yaml
# 编辑 config.yaml，填入 API 密钥、数据库连接等信息
```

### 4. 初始化数据库

```bash
psql -U postgres -d tron_sentinel -f database/migrations/001_initial.sql
```

### 5. 启动服务

```bash
# 启动仪表盘服务
uvicorn dashboard.app:app --host 0.0.0.0 --port 8000 --reload
```

---

## 技术栈

- **语言**：Python 3.11+
- **Web 框架**：FastAPI + Uvicorn
- **数据库**：PostgreSQL（可选 TimescaleDB 时序扩展）+ Redis
- **NLP 模型**：`cardiffnlp/twitter-roberta-base-sentiment-latest`
- **LLM**：Anthropic Claude（`claude-sonnet-4-6`）
- **任务调度**：APScheduler / Celery
- **监控**：Prometheus + Structlog

---

## 告警阈值（默认值，可在 config.yaml 中调整）

| 规则 | 默认阈值 | 说明 |
|------|---------|------|
| 情感骤降 | `-0.3` | 综合分数单次下跌超过 0.3 |
| 负面占比 | `60%` | 采集窗口内负面内容超过 60% |
| 量级突增 | `3×` | 内容发布量超过 24h 均值的 3 倍 |

---

## 许可证

MIT License

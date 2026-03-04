# TRON Sentinel

**TRON Sentinel** 是针对 TRON 生态的加密货币**实时舆情监控系统**。
它自动抓取多个加密媒体的 RSS 新闻，用 VADER 进行情感分析，通过 Telegram 推送负面预警，并将结果呈现在可视化仪表盘中。

---

## 快速启动（3 步）

### 第 1 步：安装依赖

```bash
git clone https://github.com/Hans010101/TRON-Sentinel.git
cd TRON-Sentinel

python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 第 2 步：运行数据流水线

```bash
python main.py
```

该命令依次执行：
1. **RSS 采集** — 从 CoinDesk、Decrypt、CoinTelegraph、BlockBeats 抓取最新文章，存入 `data/sentinel.db`
2. **情感分析** — 用 VADER 对未打分文章评分（`-1.0 → +1.0`），标注 positive / neutral / negative
3. **Telegram 预警** — 将最新负面文章推送至 Telegram（未配置 token 时自动切换为控制台干运行模式）
4. **生成仪表盘数据** — 写出 `dashboard/data.json` 供前端读取

### 第 3 步：查看仪表盘

```bash
python -m http.server 8080
# 浏览器访问 http://localhost:8080/dashboard/
```

> **注意**：仪表盘通过 `fetch('./data.json')` 加载数据，需要 HTTP 服务器；直接用 `file://` 打开会回退到内置模拟数据。

---

## 定时运行（可选）

```bash
# 每 30 分钟自动执行一次完整流水线
python scheduler.py

# 只执行一次后退出
python scheduler.py --once
```

---

## 模块说明

| 文件 | 功能 |
|------|------|
| `main.py` | 流水线编排：按顺序执行采集 → 分析 → 预警 → JSON 生成，打印每步耗时 |
| `scheduler.py` | APScheduler 定时任务，每 30 分钟调用 `main.main()`，支持 `--once` 参数 |
| `collectors/rss_collector.py` | RSS 采集器：feedparser 抓取 4 个加密媒体源，去重后存入 SQLite |
| `analyzers/sentiment_analyzer.py` | VADER 情感分析器：批量处理未打分行，写回 `sentiment_score` 和 `sentiment_label` |
| `alerting/telegram_alerter.py` | Telegram 预警：4 级告警（🔴CRITICAL / 🟠HIGH / 🟡MEDIUM / 🟢LOW），支持干运行 |
| `dashboard/index.html` | 单文件静态仪表盘：概览卡片、24h 情感趋势图、预警列表、语言分布图 |
| `dashboard/data.json` | 由 `main.py` 每次运行后自动生成，仪表盘通过 fetch 加载（git 忽略） |
| `data/sentinel.db` | SQLite 数据库（git 忽略，首次运行自动创建） |
| `config/config.example.yaml` | 配置文件示例，复制为 `config.yaml` 后填入密钥 |

---

## 环境变量

| 变量 | 说明 | 默认值 |
|------|------|--------|
| `TELEGRAM_BOT_TOKEN` | Telegram Bot 令牌（从 @BotFather 获取） | 空（干运行模式） |
| `TELEGRAM_CHAT_ID` | 接收预警的 Telegram 聊天 / 频道 ID | 空 |

未设置 `TELEGRAM_BOT_TOKEN` 时，预警内容将打印到控制台，不会抛出异常。

---

## 告警级别

| 级别 | 情感分阈值 | 含义 |
|------|-----------|------|
| 🔴 CRITICAL | `< -0.70` | 极度负面，需立即关注 |
| 🟠 HIGH | `< -0.50` | 高度负面 |
| 🟡 MEDIUM | `< -0.30` | 中度负面 |
| 🟢 LOW | `< -0.05` | 轻度负面 |

---

## RSS 数据源

| 媒体 | 语言 | 地址 |
|------|------|------|
| CoinDesk | EN | `https://www.coindesk.com/arc/outboundfeeds/rss/` |
| Decrypt | EN | `https://decrypt.co/feed` |
| CoinTelegraph | EN | `https://cointelegraph.com/rss` |
| BlockBeats | ZH | `https://www.theblockbeats.info/rss` |

---

## 技术栈

- **语言**：Python 3.11+
- **数据库**：SQLite（WAL 模式，零配置）
- **NLP**：VADER (`vaderSentiment 3.3.2`)
- **RSS 解析**：feedparser 6.0.x
- **任务调度**：APScheduler 3.10.x
- **仪表盘**：原生 HTML + CSS + Canvas（无框架依赖）

---

## 许可证

MIT License

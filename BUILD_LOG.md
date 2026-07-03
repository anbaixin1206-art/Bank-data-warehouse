# 银行数据仓库项目 — 构建日志

> 日期：2026-06-15  
> 状态：离线数仓全链路 + Spring Boot 驾驶舱已就绪

---

## 一、今日成果总览

| 模块 | 状态 | 说明 |
|------|------|------|
| 基础服务 | ✅ | MySQL + HDFS + Hive Metastore + HiveServer2 + Redis + Kafka + Zookeeper |
| 数仓 DDL | ✅ | ODS 8 张 + DWD 11 张 (Hub/Sat/Link) + DWS 1 张 + ADS 1 张 |
| 模拟数据 | ✅ | 10.5 万行（1000 客户 / 2000 账户 / 5 万交易 / 3 万支付 / 300 贷款 / 500 信用卡） |
| ODS→DWD→DWS→ADS | ✅ | 全链路 PySpark ETL 验证通过 |
| Maven 工程 | ✅ | Spring Boot 2.7.18 + Hive JDBC |
| REST API | ✅ | `/api/kpi` 等 10 个端点，从 JSON 文件读取数据 |
| 驾驶舱 UI | ✅ | macOS 毛玻璃风格，5 个标签页，ECharts 图表 |
| 数据导出 | ✅ | PySpark → JSON → Spring Boot → 前端 |

---

## 二、运行的服务

| 服务 | 端口 | 启动方式 |
|------|------|---------|
| MySQL 8.0.46 | 3306 | `docker compose up -d` |
| Redis 7 | 6379 | `docker run -d redis:7-alpine` |
| HDFS NameNode | 9000 | `start-dfs.sh` |
| HDFS DataNode | 9866 | `start-dfs.sh` |
| Hive Metastore | 9083 | `hive --service metastore` |
| HiveServer2 | 10000 | `hive --service hiveserver2` |
| Zookeeper | 2181 | Kafka 内嵌 |
| Kafka Broker | 9092 | `kafka-server-start.sh` |

### Kafka Topics

```
bank.dq.alert
bank.ods.cc.t_cc_transaction
bank.ods.core.t_transaction
bank.ods.ebank.t_ebank_transaction
bank.ods.pay.t_payment_flow
```

---

## 三、数仓数据流

```
源系统(模拟) → STG(HDFS CSV) → ODS(Hive ORC) → DWD(Data Vault)
                                                    ├── Hub (客户/账户/交易/合同/信用卡/产品/机构)
                                                    ├── Link (客户-账户/客户-贷款/交易关联)
                                                    └── Satellite (客户属性/余额快照/交易事实)
                                                 → DWS (客户日汇总)
                                                 → ADS (管理驾驶舱 KPI)
```

### ADS KPI 验证结果

| 指标 | 值 |
|------|-----|
| 总客户数 | 1,000 |
| 存款余额 | ¥39.2 亿 |
| 账户总数 | 2,000 |
| 当日交易笔数 | 49,859 |
| 当日交易金额 | ¥124.7 亿 |
| 贷款合同数 | 300 |
| 信用卡发卡量 | 500 |

---

## 四、项目结构

```
bank-data-warehouse/
├── pom.xml                              # Maven + Spring Boot 2.7.18
├── BUILD_LOG.md                         # 本文件
├── .gitignore
│
├── src/main/java/com/bank/dw/
│   ├── BankDWApplication.java           # @SpringBootApplication 入口
│   ├── DWConfig.java                    # 全局配置
│   ├── api/
│   │   ├── HiveService.java             # 数据服务（读 JSON 文件）
│   │   └── DashboardController.java     # 10 个 REST API 端点
│   └── scheduler/
│       ├── ETLScheduler.java            # ETL 调度器
│       ├── ETLJob.java                  # 作业定义
│       └── ETLJobType.java              # 作业类型枚举
│
├── src/main/resources/
│   ├── application.properties           # server.port=8899
│   ├── config.properties                # 数仓配置
│   ├── static/
│   │   └── index.html                   # macOS 毛玻璃驾驶舱
│   ├── sql/ddl/   etl/   dq/            # SQL 脚本
│   ├── spark/                           # PySpark 脚本
│   └── shell/                           # 运维脚本
│
├── scripts/                             # 可独立运行的脚本
│   ├── start_all.sh                     # 一键启动所有服务
│   ├── start_kafka.sh                   # Kafka 启动
│   ├── generate_mock_data.py            # 模拟数据生成
│   ├── load_ods_spark.py                # ODS 加载
│   ├── etl_dwd_spark.py                 # DWD ETL
│   ├── etl_dws_ads_spark.py             # DWS+ADS ETL
│   ├── export_dashboard_data.py         # 导出驾驶舱 JSON
│   ├── generate_dashboard.py            # 早期静态 HTML 生成器
│   └── run_etl_all.sh                   # 一键 ETL
│
├── data/                                # JSON 快照（Spring Boot 读取）
│   ├── kpi.json
│   ├── aum_distribution.json
│   ├── hourly_trend.json
│   ├── channel_distribution.json
│   ├── account_types.json
│   ├── top_customers.json
│   ├── transaction_summary.json
│   ├── transactions.json
│   ├── risk_overview.json
│   └── risk_alerts.json
│
├── 01_需求概述/ ... 09_实施路线图/      # 59 个需求文档
└── docs/superpowers/specs/              # 设计文档
```

---

## 五、驾驶舱 UI

### 技术栈

- **后端**：Spring Boot 2.7.18 (port 8899)
- **前端**：纯 HTML + ECharts 5.5 CDN + CSS Variables
- **设计**：macOS 原生风格 + 毛玻璃卡片 + SF Pro 字体

### 标签页

| 标签 | 内容 |
|------|------|
| 驾驶舱总览 | 7 KPI 卡片 + 交易趋势 + AUM 饼图 + 渠道分布 + 账户类型 + Top15 客户 |
| 交易监控 | 交易汇总卡片 + 最新 20 条流水表格 |
| 风险管理 | 风控指标 + 大额/夜间告警列表 |
| 客户分析 | 高净值客户横向柱状图 |
| 报表导出 | 监管报表导出入口 |

### API 端点

| 端点 | 数据文件 |
|------|---------|
| `GET /api/kpi` | kpi.json |
| `GET /api/aum-distribution` | aum_distribution.json |
| `GET /api/hourly-trend` | hourly_trend.json |
| `GET /api/channel-distribution` | channel_distribution.json |
| `GET /api/account-types` | account_types.json |
| `GET /api/top-customers` | top_customers.json |
| `GET /api/transaction-summary` | transaction_summary.json |
| `GET /api/transactions` | transactions.json |
| `GET /api/risk/overview` | risk_overview.json |
| `GET /api/risk/alerts` | risk_alerts.json |

---

## 六、运行方式

### 1. 启动全部服务（WSL 终端）
```bash
bash /mnt/d/bigdata-lab/start_bigdata_lab.sh
bash /mnt/d/bigdata-lab/bank-data-warehouse/scripts/start_kafka.sh
```

### 2. 更新数据（WSL 终端）
```bash
source /mnt/d/bigdata-lab/env_vars.sh
cd /mnt/d/bigdata-lab/bank-data-warehouse/scripts
spark-submit --master "local[*]" generate_mock_data.py    # 生成新模拟数据
spark-submit --master "local[*]" load_ods_spark.py         # 加载 ODS
spark-submit --master "local[*]" etl_dwd_spark.py          # DWD ETL
spark-submit --master "local[*]" etl_dws_ads_spark.py      # DWS + ADS
spark-submit --master "local[*]" export_dashboard_data.py  # 导出仪表盘 JSON
```

### 3. 启动驾驶舱（IDEA）
```
IDEA → 右键 BankDWApplication.java → Run
浏览器 → http://localhost:8899
```

---

## 七、关键决策记录

| 决策 | 选择 | 原因 |
|------|------|------|
| 数仓建模 | Lambda + Data Vault 融合 | 国内银行标准 + 外企实践 |
| 工程语言 | Java (Spring Boot) | 银行主流、与 Maven 统一 |
| ETL 执行 | PySpark + Hive SQL | 70% SQL + 30% Python |
| 前端方案 | 静态 HTML + ECharts CDN | 零依赖、IDEA 直接跑 |
| 数据桥接 | PySpark → JSON → Spring Boot | 规避 Windows→WSL JDBC 跨网络问题 |
| Docker 连接 | `tcp://localhost:2375` | daemon.json + systemd override |

---

## 八、待完成

- [ ] Phase 6: Flink 实时风控 CEP 验证
- [ ] Canal 实时采集部署
- [ ] DolphinScheduler 调度部署
- [ ] Hive JDBC 直连（替换 JSON 文件模式）
- [ ] 数据质量 DQ 脚本执行
- [ ] 监管报送 ADS 表填充

# 银行数据仓库项目 — 环境与进度总览

> 更新日期：2026-06-15  
> 环境：Windows 11 + WSL2 (Ubuntu 22.04)

---

## 一、WSL 已安装软件清单

### 大数据组件

| 软件 | 版本 | 安装路径 | 安装方式 |
|------|------|---------|---------|
| JDK 8 | 1.8.0_492 | `/usr/lib/jvm/java-8-openjdk-amd64` | apt |
| JDK 11 | 11.0.31 | `/usr/lib/jvm/java-11-openjdk-amd64` | apt（备用） |
| Hadoop | 3.3.6 | `/mnt/d/Study/Claude_temp_project_data_/hadoop-3.3.6` | 原生 |
| Hive | 3.1.3 | `/mnt/d/Study/Claude_temp_project_data_/hive-3.1.3` | 原生 |
| Spark | 3.5.8 | `/mnt/d/bigdata-lab/software/spark-3.5.8-bin-hadoop3` | 原生 |
| Flink | 1.18.1 | `/mnt/d/bigdata-lab/software/flink-1.18.1` | 原生 |
| DataX | — | `/mnt/d/bigdata-lab/software/datax` | 原生 |
| Kafka | 3.9.0 | `/mnt/d/bigdata-lab/software/kafka_2.13-3.9.0` | 原生 |
| socat | — | apt 安装 | apt（用于 TCP 代理） |

### 容器服务

| 软件 | 版本 | 运行方式 | 端口 |
|------|------|---------|------|
| Docker Engine | 29.5.3 | systemd 服务 | — |
| MySQL | 8.0.46 | Docker 容器 `mysql` | 3306 |
| Redis | 7-alpine | Docker 容器 `redis` | 6379 |

### Python 环境

| 环境 | 版本 | 路径 |
|------|------|------|
| WSL Python | 3.10.12 | `/mnt/d/bigdata-lab/software/python-venv` |
| Windows Python | 3.12 | `D:\Study\Claude_temp_project_data_\python312-full` |
| 关键库 | PySpark 3.5.8, PyHive, pandas | pip 安装 |

### 工具

| 工具 | 平台 | 用途 |
|------|------|------|
| IDEA (Community) | Windows | Java Maven 工程开发 |
| PyCharm (Community) | Windows | PySpark 脚本开发 |
| DataGrip | Windows | Hive / MySQL 数据查询 |

---

## 二、当前运行的服务

| 服务 | 端口 | 进程 | 启动命令 |
|------|------|------|---------|
| MySQL | 3306 | Docker | `docker compose up -d` |
| Redis | 6379 | Docker | `docker run -d redis:7-alpine` |
| HDFS NameNode | 9000/9870 | NameNode | `start-dfs.sh` |
| HDFS DataNode | 9866 | DataNode | `start-dfs.sh` |
| Hive Metastore | 9083 | RunJar | `hive --service metastore` |
| HiveServer2 | 10000 | RunJar | `hive --service hiveserver2` |
| Zookeeper | 2181 | QuorumPeerMain | Kafka 内嵌 |
| Kafka Broker | 9092 | Kafka | `kafka-server-start.sh` |
| Docker TCP | 2375 | dockerd | daemon.json 配置 |
| socat 代理 | 10099 | socat | TCP → HiveServer2:10000 |

### 一键重启命令

```bash
# WSL 终端
bash /mnt/d/bigdata-lab/start_bigdata_lab.sh      # MySQL + HDFS + Hive
bash /mnt/d/bigdata-lab/bank-data-warehouse/scripts/start_kafka.sh  # Kafka
```

---

## 三、Windows ↔ WSL 连接配置

### DataGrip → Hive

通过 Windows `netsh` 端口转发 + WSL socat 代理实现：

```
DataGrip → localhost:10099 → netsh转发 → 172.21.23.26:10000 → HiveServer2
```

| 配置项 | 值 |
|--------|-----|
| URL | `jdbc:hive2://localhost:10099/default` |
| User | `root` |

### IDEA → Docker

| 配置项 | 值 |
|--------|-----|
| Engine URL | `tcp://localhost:2375` |

Docker TCP 通过 `/etc/docker/daemon.json` + systemd override 开启。

### PyCharm 本地 PySpark

- Python: `D:\Study\Claude_temp_project_data_\python312-full\python.exe`
- JAVA_HOME: `D:\Study\Claude_temp_project_data_\jdk8`
- SPARK_HOME: `D:\bigdata-lab\software\spark-3.5.8-bin-hadoop3`
- HADOOP_HOME: `D:\bigdata-lab\software\hadoop`
- `winutils.exe` 放置在 `D:\bigdata-lab\software\hadoop\bin\`

> PySpak 本地只能做 DataFrame 操作和单元测试，**不能直连 WSL Hive**（版本兼容问题）。查表用 DataGrip，跑 ETL 用 WSL spark-submit。

---

## 四、数仓项目进度

### 已完成

| 阶段 | 内容 | 状态 |
|------|------|------|
| **需求文档** | 59个 Markdown 需求文档（01~09 章节） | ✅ |
| **基础设施** | MySQL + HDFS + Hive + Redis + Kafka + Zookeeper 全部就绪 | ✅ |
| **数仓 DDL** | ODS 8 张 + DWD 11 张(Hub/Sat/Link) + DWS 1 张 + ADS 1 张 | ✅ |
| **模拟数据** | 105,800 行（1000客户/2000账户/5万交易/3万支付/300贷款/500信用卡） | ✅ |
| **ODS 加载** | PySpark CSV → Hive ORC 全量加载 | ✅ |
| **DWD ETL** | Hub 实体识别 + Satellite 属性拉链 + Link 关系提取 | ✅ |
| **DWS 汇总** | 客户日汇总表 | ✅ |
| **ADS 应用** | 管理驾驶舱 KPI（7项指标） | ✅ |
| **Spring Boot 工程** | Maven + REST API + 驾驶舱页面 | ✅ |
| **macOS 毛玻璃驾驶舱** | ECharts 5图表 + 5个标签页，运行在 `localhost:8899` | ✅ |
| **IDE 集成** | IDEA (Java) + PyCharm (PySpark) + DataGrip (Hive/MySQL) | ✅ |

### ADR KPI 验证结果（2026-06-14）

| 指标 | 数值 |
|------|------|
| 总客户数 | 1,000 |
| 存款余额 | ¥3,921,908,918（≈39.2亿） |
| 账户总数 | 2,000 |
| 当日交易笔数 | 49,859 |
| 当日交易金额 | ¥12,468,981,136（≈124.7亿） |
| 贷款合同数 | 300 |
| 信用卡发卡量 | 500 |

### 待完成

| 阶段 | 内容 | 优先级 |
|------|------|--------|
| Phase 6 | Flink CEP 实时风控验证 | 🔴 高 |
| — | Canal 实时采集部署 | 🟡 中 |
| — | DolphinScheduler 调度部署 | 🟡 中 |
| — | Hive JDBC 直连（替换 JSON 文件模式） | 🟡 中 |
| — | 数据质量 DQ 检查脚本 | 🟢 低 |

---

## 五、项目目录结构

```
D:\bigdata-lab\
├── start_bigdata_lab.sh              # 一键启动 MySQL + HDFS + Hive
├── stop_all.sh                        # 一键停止
├── env_vars.sh                        # 全局环境变量（已含 KAFKA_HOME）
├── docker-compose.yml                 # MySQL 容器定义
├── config/                            # 配置文件备份
├── data/                              # HDFS 持久化数据
├── software/                          # 大数据软件
│   ├── spark-3.5.8-bin-hadoop3/
│   ├── flink-1.18.1/
│   ├── kafka_2.13-3.9.0/
│   ├── datax/
│   ├── python-venv/
│   └── hadoop/bin/winutils.exe        # Windows PySpark 依赖
│
└── bank-data-warehouse/               # 🏦 本项目
    ├── pom.xml                         # Maven + Spring Boot 2.7.18
    ├── BUILD_LOG.md                    # 今日构建日志
    ├── PROJECT_STATUS.md               # 本文件
    ├── .gitignore
    │
    ├── src/main/java/com/bank/dw/      # Java 代码
    │   ├── BankDWApplication.java      # Spring Boot 入口 (port 8899)
    │   ├── DWConfig.java               # 全局配置
    │   ├── api/
    │   │   ├── HiveService.java        # 数据服务（读 JSON 文件）
    │   │   └── DashboardController.java # 10 个 REST API
    │   └── scheduler/                  # ETL 调度器
    │
    ├── src/main/resources/
    │   ├── application.properties      # server.port=8899
    │   ├── static/index.html           # macOS 毛玻璃驾驶舱
    │   ├── sql/                        # SQL 脚本 (DDL/ETL/DQ)
    │   ├── spark/                      # PySpark 脚本
    │   └── shell/                      # 运维脚本
    │
    ├── scripts/                        # 可独立运行脚本
    │   ├── start_all.sh / start_kafka.sh
    │   ├── generate_mock_data.py
    │   ├── load_ods_spark.py
    │   ├── etl_dwd_spark.py
    │   ├── etl_dws_ads_spark.py
    │   ├── export_dashboard_data.py
    │   ├── test_local_pyspark.py
    │   └── run_etl_all.sh
    │
    ├── data/                           # 驾驶舱 JSON 快照
    ├── 01_需求概述/ ... 09_实施路线图/  # 59 个需求文档
    └── docs/superpowers/specs/         # 设计文档
```

---

## 六、日常使用速查

### 启动环境
```bash
# WSL 终端
bash /mnt/d/bigdata-lab/start_bigdata_lab.sh            # 基础服务
bash /mnt/d/bigdata-lab/bank-data-warehouse/scripts/start_kafka.sh  # Kafka
```

### 运行 ETL（更新数据）
```bash
cd /mnt/d/bigdata-lab/bank-data-warehouse/scripts
spark-submit --master "local[*]" generate_mock_data.py     # 生成模拟数据
spark-submit --master "local[*]" load_ods_spark.py          # 加载 ODS
spark-submit --master "local[*]" etl_dwd_spark.py           # DWD ETL
spark-submit --master "local[*]" etl_dws_ads_spark.py       # DWS + ADS
spark-submit --master "local[*]" export_dashboard_data.py   # 导出仪表盘 JSON
```

### 启动驾驶舱
```
IDEA → 右键 BankDWApplication.java → Run
浏览器 → http://localhost:8899
```

### 查询数据
```
WSL beeline:  beeline -u jdbc:hive2://localhost:10000
DataGrip:     jdbc:hive2://localhost:10099/default
```

### 查看 Kafka
```bash
$KAFKA_HOME/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
$KAFKA_HOME/bin/kafka-console-consumer.sh --topic bank.ods.core.t_transaction --bootstrap-server localhost:9092 --from-beginning --max-messages 3
```

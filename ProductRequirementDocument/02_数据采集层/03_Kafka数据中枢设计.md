# 03 Kafka 数据中枢设计

> 所属：02_数据采集层
> 上一文档：[02_采集策略与工具选型](./02_采集策略与工具选型.md)
> 下一文档：[01_数仓分层设计](../03_数据仓库模型/01_数仓分层设计.md)

---

## 1 Kafka 在整体架构中的定位

Kafka 是本项目的**数据中枢**，而非仅仅实时链路的管道：

```
                          ┌─────────────────────┐
                          │     Kafka Cluster    │
                          │                     │
  源系统采集 ─────────────→│  bank.ods.*         │──→ Flink 实时消费 (风控/对账/大屏)
  (DataX/Canal/Flume)     │  bank.dwd.*         │──→ Spark 离线回放 (T+1 批处理)
                          │  bank.dq.*          │──→ 监控告警消费
                          └─────────────────────┘
                                    │
                          ┌─────────┴──────────┐
                          ▼                    ▼
                    实时消费 (秒级)       离线回放 (T+1)
                    Flink Consumer      Spark Batch Consumer
```

**核心设计理念**：Kafka 是实时和离线数据的**唯一真实入口**。T+1 批处理从 Kafka 回放数据，而非重新连接源系统，保证实时和离线使用同一份原始数据。

---

## 2 Topic 命名规范

### 2.1 命名格式

```
格式: bank.{layer}.{source_system}.{table_name}

layer:          ods / dwd / dq (数据质量)
source_system:  core / pay / loan / cc / ebank / atmpos / wealth / aml / crm / gl / ecif
table_name:     源表名 (保持与源系统一致)

完整示例:
  bank.ods.core.t_transaction          # 核心银行-交易流水
  bank.ods.pay.t_payment_flow          # 支付网关-支付流水
  bank.ods.cc.t_cc_transaction         # 信用卡-消费交易
  bank.ods.ebank.t_login_event         # 网银-登录事件
  bank.ods.ebank.t_ebank_transaction   # 网银-电子渠道交易
  bank.ods.atmpos.atm_transaction      # ATM交易
  bank.ods.atmpos.pos_transaction      # POS交易
  bank.ods.wealth.t_wm_transaction     # 理财-交易记录
```

### 2.2 Topic 分类

| Topic 类别 | 命名模式 | 用途 | 分区数 | 保留时间 |
|-----------|---------|------|--------|---------|
| **实时消费 Topic** | `bank.ods.{实时系统}.*` | Canal → Flink 实时风控/对账 | 8-16 | 7天 |
| **回放 Topic** | `bank.ods.{离线系统}.*` | DataX → Spark 离线回放 | 4-8 | 30天 |
| **衍生数据 Topic** | `bank.dwd.*` | Flink 处理后的中间结果 | 8 | 3天 |
| **质量监控 Topic** | `bank.dq.*` | 质量检查结果事件 | 4 | 7天 |

---

## 3 Topic 分区策略

### 3.1 分区键设计

| Topic 类型 | 分区键 | 分区数 | 设计理由 |
|-----------|--------|--------|---------|
| 交易流水类 | `trans_id` / `pay_id` | 16 | 高吞吐，需均匀分布 |
| 账户维度类 | `account_no` | 8 | 按账户保证同类数据有序 |
| 客户维度类 | `customer_id` | 8 | 按客户保证同类数据有序 |
| 日志/事件类 | `event_time` (截断到分钟) | 16 | 时间分区方便按时间消费 |
| 主数据类 | `主键 hash` | 4 | 低吞吐，简单 hash 即可 |

### 3.2 分区数计算逻辑

```
分区数 = MAX(
    基准分区数: throughput / single_partition_capacity,
    并行度: consumer_parallelism,
    冗余: 预留 1.5x 空间
)

示例: 支付网关流水
  throughput = 50 MB/s
  single_partition_capacity = 10 MB/s
  基准分区数 = 50 / 10 = 5
  consumer_parallelism = 8 (Flink 并行度)
  最终分区数 = MAX(5, 8) * 1.5 ≈ 16
```

---

## 4 Kafka 配置参数

### 4.1 Broker 端配置

```properties
# server.properties (关键配置)
broker.id=0
listeners=PLAINTEXT://localhost:9092
log.dirs=/data/kafka/data
num.partitions=8

# 日志保留策略
log.retention.hours=720                  # 默认30天
log.retention.bytes=107374182400         # 单分区最大100GB
log.segment.bytes=1073741824             # 单Segment 1GB
log.cleanup.policy=delete

# 性能配置
num.network.threads=8
num.io.threads=16
socket.send.buffer.bytes=1048576
socket.receive.buffer.bytes=1048576
socket.request.max.bytes=104857600

# 副本配置 (单节点环境暂不启用)
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
```

### 4.2 Producer 端配置

```properties
# DataX HDFS Writer → Kafka Producer
bootstrap.servers=localhost:9092
key.serializer=org.apache.kafka.common.serialization.StringSerializer
value.serializer=org.apache.kafka.common.serialization.StringSerializer

# 可靠性配置
acks=1                           # 单节点环境
retries=3
max.in.flight.requests.per.connection=1

# 吞吐配置
batch.size=65536
linger.ms=10
compression.type=snappy
buffer.memory=33554432
```

### 4.3 Consumer 端配置

```properties
# Flink Consumer 配置
bootstrap.servers=localhost:9092
group.id=flink-bank-risk-engine
key.deserializer=org.apache.kafka.common.serialization.StringDeserializer
value.deserializer=org.apache.kafka.common.serialization.StringDeserializer

# 消费配置
enable.auto.commit=false
auto.offset.reset=latest          # 实时消费从最新开始
max.poll.records=500
fetch.min.bytes=1048576
fetch.max.wait.ms=500
```

---

## 5 数据格式规范

### 5.1 消息格式（Canal 输出）

```json
{
  "data": [
    {
      "column1": "value1",
      "column2": "value2"
    }
  ],
  "database": "source_db",
  "table": "source_table",
  "type": "INSERT",
  "ts": 1686808805000,
  "_meta": {
    "ingest_time": "2026-06-15T10:00:05.000Z",
    "ingest_tool": "canal",
    "schema_version": "1.0"
  }
}
```

### 5.2 消息格式（DataX 输出）

```json
{
  "data": {
    "columns": ["col1", "col2", "col3"],
    "rows": [
      ["val1", "val2", "val3"],
      ["val4", "val5", "val6"]
    ]
  },
  "source": "oracle",
  "database": "core_db",
  "table": "T_TRANSACTION",
  "mode": "incremental",
  "batch_id": "batch_20260615_001",
  "ts": 1686808805000,
  "_meta": {
    "ingest_time": "2026-06-15T00:15:30.000Z",
    "ingest_tool": "datax",
    "start_time": "2026-06-15T00:00:00",
    "end_time": "2026-06-15T00:15:00"
  }
}
```

---

## 6 生产与消费关系

### 6.1 数据流入（Producer → Topic 映射）

| Producer | 源系统 | Topic | 速率 |
|----------|--------|-------|------|
| Canal | 支付网关 | `bank.ods.pay.t_payment_flow` | ~1000 msg/s |
| Canal | 支付网关 | `bank.ods.pay.t_pay_channel_log` | ~2000 msg/s |
| Canal | 信用卡 | `bank.ods.cc.t_cc_transaction` | ~500 msg/s |
| Canal | 网银 | `bank.ods.ebank.t_login_event` | ~3000 msg/s |
| Canal | 网银 | `bank.ods.ebank.t_ebank_transaction` | ~500 msg/s |
| Canal | 理财 | `bank.ods.wealth.t_wm_transaction` | ~200 msg/s |
| Flume | ATM/POS | `bank.ods.atmpos.atm_transaction` | ~100 msg/s |
| Flume | ATM/POS | `bank.ods.atmpos.pos_transaction` | ~500 msg/s |
| DataX | 核心银行 | `bank.ods.core.t_transaction` | ~2000 msg/s (15min batch) |
| DataX | 其他离线 | `bank.ods.*` | 批量写入 |

### 6.2 数据流出（Topic → Consumer 映射）

| Consumer | Topic 范围 | 消费模式 | 处理 |
|----------|-----------|---------|------|
| Flink CEP 风控 | `bank.ods.pay.*`, `bank.ods.ebank.*`, `bank.ods.cc.*` | 实时 (latest) | 规则匹配 + 告警 |
| Flink SQL 对账 | `bank.ods.pay.t_payment_flow` | 实时 (latest) | 支付核心双流对账 |
| Flink SQL 大屏 | `bank.ods.pay.*`, `bank.ods.ebank.*` | 实时 (latest) | 5s滚动窗口聚合 |
| Spark 离线回放 | `bank.ods.core.*`, `bank.ods.loan.*`, ... | 批量 (earliest 昨日) | T+1 写入 Hive ODS |
| 监控告警 | `bank.ods.*`, `bank.dq.*` | 准实时 | 流量/延迟/异常监控 |

---

## 7 数据回放机制

### 7.1 离线 T+1 回放

```
每天 00:00，Spark 批处理作业启动：

1. 定位昨日 Kafka offset
   offsets_for_times(timestamp=yesterday_00:00:00)

2. 消费昨日全天数据
   assign(partition, startOffset, endOffset)

3. 批量写入 Hive ODS 表
   INSERT OVERWRITE TABLE ods_core.t_transaction
   PARTITION (dt='${yesterday}')

4. 定期提交 offset
   commitAsync() 每分钟

回放完成后，离线数据与实时数据来源一致
（都是同一个 Kafka Topic），避免数据不一致。
```

### 7.2 历史数据回溯

```
场景: 某 DWD 表需要重新计算过去 30 天数据

1. 判定 Kafka 中是否还保留数据
   if retention_ms > 30天: 从 Kafka 回放
   else: 从 Hive ODS 历史快照重新计算

2. 优先从 Kafka 回放（数据最原始）
   设置 auto.offset.reset = earliest

3. 触发全链路重跑
   指定日期范围 → 对应分区覆盖写入
```

---

## 8 监控与运维

### 8.1 Kafka 监控指标

| 指标 | 说明 | 采集方式 | 告警阈值 |
|------|------|---------|---------|
| `MessagesInPerSec` | 每秒消息入站速率 | JMX / Kafka Metrics | 突降 50% |
| `BytesInPerSec` | 每秒字节入站速率 | JMX | < 预期 50% |
| `UnderReplicatedPartitions` | 副本不足分区数 | JMX | > 0 |
| `ConsumerLag` | 消费者延迟 | Consumer Group Metrics | > 10000 条或 > 5min |
| `TopicSize` | Topic 磁盘占用 | `du -sh` | > 80% 磁盘 |

### 8.2 消费者延迟告警

```bash
# 检查 Consumer Group 延迟
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group flink-bank-risk-engine \
  --describe

# 输出示例:
# TOPIC              PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
# bank.ods.pay...    0          12345678        12345900        222   ← 正常
# bank.ods.pay...    1          23456789        23467890        11101 ← 异常，延迟 > 10000
```

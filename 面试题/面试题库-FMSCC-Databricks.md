# Databricks 大数据开发面试题库（FMSCC 项目）

> 岗位：ETL 数据开发工程师 | 技术栈：Databricks / Spark / Python / Delta Lake  
> 场景：DataWorks → Databricks 跨平台迁移 | 汽车行业数据中台

---

# 一、Databricks / Spark 核心技术

---

## Q1：PySpark DataFrame 和 Pandas DataFrame 有什么区别？什么场景该用哪个？

| 维度 | PySpark DataFrame | Pandas DataFrame |
|------|-------------------|------------------|
| 执行模式 | 惰性执行（Lazy），构建 DAG 后触发 Action 才计算 | 立即执行（Eager） |
| 数据规模 | 分布式，支持 TB/PB 级 | 单机内存，一般 < 几个 GB |
| 底层存储 | 跨多节点分区存储，Shuffle 涉及网络 IO | 单机内存连续存储 |
| API 风格 | SQL 风格 + 函数式（filter/select/groupBy/agg） | Numpy 风格，更灵活 |
| 使用场景 | ETL 批处理、大规模聚合、分布式计算 | 小数据集探索分析、单机特征工程 |

**场景选择**：
- PySpark DataFrame：ETL 管道（几百 GB 日志清洗）、Hive/Databricks 上的日常批处理
- Pandas DataFrame：Jupyter 里做数据探索（取几千行样本画图）、模型训练前的特征矩阵
- 桥接：`toPandas()` 转 Pandas（注意别 OOM），Pandas UDF 用 Arrow 加速分布式跑 Pandas 逻辑

---

## Q2：Spark 的窄依赖和宽依赖分别是什么？为什么宽依赖会触发 Shuffle？

**窄依赖（Narrow Dependency）**：
- 父 RDD 的每个分区最多被一个子 RDD 分区依赖
- 例如：`map`、`filter`、`flatMap`、`union`
- 不需要 Shuffle，数据在单节点内 Pipeline 执行

**宽依赖（Wide Dependency）**：
- 父 RDD 的一个分区可能被多个子 RDD 分区依赖
- 例如：`groupByKey`、`reduceByKey`、`join`（非 Broadcast）、`repartition`
- 需要 Shuffle：父分区数据按 Key 重新跨节点分配到子分区

**为什么触发 Shuffle**：
- 以 `groupByKey` 为例，相同 Key 的数据分散在不同节点
- 必须把所有相同 Key 的数据汇聚到同一个节点才能聚合
- 过程：Map 端写磁盘（Shuffle Write）→ 网络传输 → Reduce 端读磁盘（Shuffle Read）

**加分点**：`reduceByKey` 比 `groupByKey` 高效，因为 Map 端先做了预聚合（Map-side Combine），大幅减少 Shuffle 数据量。

---

## Q3：Pandas UDF（Vectorized UDF）和普通 UDF 性能差异有多大？为什么？

**普通 UDF（Row-at-a-Time）**：
```python
@udf("string")
def my_func(x):
    return x.upper() if x else None
```
- 每次处理一行，Python 进程频繁调用，`pickle` 序列化开销大
- 比内置 Spark SQL 函数慢 10-100 倍

**Pandas UDF（Vectorized UDF / Apache Arrow）**：
```python
@pandas_udf("string")
def my_func_arrow(s: pd.Series) -> pd.Series:
    return s.str.upper()
```
- Apache Arrow 列式内存传输，一次传一批数据（向量化）
- 在 Python 进程内用 Pandas/Numpy 操作整个 batch
- 比普通 UDF 快 5-100 倍，接近 JVM 内置函数

**性能差异原因**：
1. Arrow 是列式零拷贝，pickle 是行式逐行序列化
2. 普通 UDF 100万行 = 100万次 Python 调用；Pandas UDF 每批调一次（几百次）
3. Pandas 底层用 Numpy/C 向量化运算

**最佳实践**：优先 Spark SQL 内置函数 → 不够用 Pandas UDF → 避免普通 UDF

---

## Q4：Spark 作业 OOM 了你怎么排查？

### 排查步骤
1. **Spark UI → Executors 页**：哪个 Executor OOM？Driver 还是 Executor？
2. **GC 时间占比 > 10%** → 内存不够
3. **Stages → 单个 Task 的 Shuffle Read Size**：差异巨大 → 数据倾斜
4. **日志**：`OutOfMemoryError: Java heap space` vs `GC overhead limit exceeded`

### 解决方案

| 问题 | 解决 |
|------|------|
| Executor Memory 不足 | 增加 `spark.executor.memory`（不要超过节点 75%）+ 增加 `spark.executor.memoryOverhead` |
| Shuffle Partitions 不合理 | 默认 200，大数据量调大：`shuffle partitions = 总数据量(MB) / 128MB` |
| 数据倾斜 | ① 加盐（Salting）：在倾斜 Key 上加随机后缀打散 ② Broadcast Join 小表 ③ 开启 AQE：`spark.sql.adaptive.skewJoin.enabled = true` |

```python
# AQE 自动处理数据倾斜
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

---

## Q5：500GB 大表 JOIN 50MB 小表，Spark 里有哪些优化策略？Broadcast Hint 原理是什么？

### 推荐方案：Broadcast Hash Join（无 Shuffle）
```python
from pyspark.sql.functions import broadcast
result = big_df.join(broadcast(small_df), "key", "left")
```
- 小表全量广播到每个 Executor 内存，大表无需 Shuffle
- 50MB 需手动 Hint（默认阈值 `spark.sql.autoBroadcastJoinThreshold` 仅 10MB）

**Broadcast Hint 原理**：
1. Driver 收集小表数据到 `BroadcastVariable`
2. 通过 TorrentBroadcast（BitTorrent 协议）P2P 分发给所有 Executor
3. 每个 Executor 将小表加载到内存
4. 大表在**本地**与内存中小表 Hash Join，无 Shuffle
5. 条件：小表必须能装进每个 Executor 内存

**备选方案**：调大阈值 `spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "100m")`

---

## Q6：Databricks 的 Delta Lake 相比传统 Hive/Parquet 有什么优势？

| 能力 | Hive + Parquet | Delta Lake |
|------|---------------|------------|
| ACID 事务 | ❌ 并发写可能损坏数据 | ✅ Serializable 隔离级别 |
| Upsert/Merge | ❌ 只能全量覆盖 | ✅ `MERGE INTO` |
| Time Travel | ❌ | ✅ 按版本号/时间戳回滚 |
| Schema Evolution | ❌ 手动处理 | ✅ 自动合并兼容变更 |
| 数据一致性 | ❌ 可能读到不完整数据 | ✅ 写即一致 |
| 小文件合并 | ❌ 手动 | ✅ `OPTIMIZE` 命令 |
| 流批一体 | 分离 | ✅ 同一张表支持批和流 |

### 核心操作示例

```python
# MERGE INTO
delta_table.alias("target").merge(
    source_df.alias("source"), "target.order_id = source.order_id"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# Time Travel
df = spark.read.option("versionAsOf", 15).table("sales_bronze")
df = spark.read.option("timestampAsOf", "2026-06-14").table("sales_bronze")

# Schema Evolution
df.write.option("mergeSchema", "true").mode("append").save("/delta/sales")
```

---

## Q7：Databricks Auto Loader 的原理是什么？做增量接入注意什么？

**原理**：
1. **文件发现**：云存储通知服务（SQS/Event Grid）或目录列表发现新文件
2. **增量追踪**：RocksDB 记录已处理文件状态，保证 Exactly-Once
3. **Schema 推断**：自动推断 JSON/CSV/Parquet Schema，支持演化
4. **容错**：Checkpoint 机制保证故障恢复后不丢不重

```python
(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/checkpoint/schema")
    .load("/mnt/landing/sales/")
    .writeStream
    .option("checkpointLocation", "/checkpoint/sales")
    .trigger(availableNow=True)
    .table("fmscc_bronze.sales_raw")
)
```

**注意事项**：
1. Schema Evolution 模式配置：`addNewColumns` / `rescue` / `failOnNewColumns`
2. 小文件问题：Source 目录大量小文件影响发现性能
3. 通知模式延迟低（< 1min），目录列表模式免费但有延迟
4. Rescue Column：解析失败的记录进 `_rescued_data` 列，不丢数据

---

## Q8：Databricks Workflows 和 Airflow 的区别？你怎么设计多任务 DAG 依赖？

| 维度 | Databricks Workflows | Airflow |
|------|---------------------|---------|
| 部署 | 内置，无需维护 | 需自行部署 |
| 任务类型 | Notebook/JAR/Python/SQL/dbt | 任意（Operator 生态丰富） |
| 跨系统编排 | 弱，主要管 Databricks 内部 | 强，可编排外部系统 |
| 依赖管理 | DAG 可视化编辑器 + 条件分支 | Python DSL 定义完整 DAG |
| 参数传递 | Task Values API | XCom |

### FMSCC 典型调度 DAG
```
Sales_Bronze ──┬── Sales_Silver ── Sales_Gold ── CDP_Refresh
               │
Leads_Bronze ──┤
               │
DMS_Bronze ────┴── DMS_Silver ────────────────── DIP_Refresh
```

**设计原则**：
- Bronze 并行摄入（无依赖），Silver 依赖对应 Bronze，Gold 依赖所有 Silver
- 每个 Task 设置重试 3 次、超时 2 小时、失败 Email 告警

---

## Q9：Unity Catalog 是什么？和 Hive Metastore 比有什么改进？

| 能力 | Hive Metastore | Unity Catalog |
|------|---------------|---------------|
| 权限模型 | 表级 RBAC（需外部 Ranger） | 原生 表/行/列级 RBAC + ABAC |
| 账号体系 | 需外部 Kerberos/LDAP | 天然集成 Databricks 用户/组 |
| 跨工作区 | 每 Workspace 独立 | 跨 Workspace/Region 统一视图 |
| 血缘追踪 | 需额外工具（Atlas） | 内置自动血缘（表→列级） |
| 数据分类 | 无 | 内置 PII 标记 |
| 联邦查询 | 不支持 | Lakehouse Federation 查外部系统 |

**三级命名空间**：`catalog.schema.table`

```sql
GRANT SELECT ON SCHEMA sales_catalog.sales_schema TO `user@company.com`;
```

---

## Q10：SQL Warehouse 和 All-Purpose Cluster 什么时候用哪个？

| 维度 | SQL Warehouse | All-Purpose Cluster |
|------|--------------|---------------------|
| 用途 | BI 查询、仪表盘、SQL 分析 | 数据工程、ETL、ML 训练 |
| 启动时间 | 秒级（热池） | 分钟级 |
| 多用户共享 | ✅ | ❌ 独占或需配置 |
| 语言支持 | 仅 SQL | SQL/Python/Scala/R |
| 计费 | 按使用时长 | 按集群运行时长 |

**选择策略**：
- SQL Warehouse：CDP BI 报表查询、数据分析师 Ad-hoc 分析
- All-Purpose Cluster：ETL Notebook（Python 脚本）、数据迁移
- **成本优化**：ETL 用 Jobs Compute（比 All-Purpose 便宜 50%+），SQL Warehouse 用 Serverless

---

## Q11：OPTIMIZE 和 VACUUM 命令分别做什么？

**OPTIMIZE**：合并小文件 + Z-Order 优化查询
```sql
OPTIMIZE sales_silver;
OPTIMIZE sales_silver ZORDER BY (order_date);  -- 加速按日期过滤
```
- 写入频繁的表：每天或每周执行一次
- 小文件 > 1000 时执行

**VACUUM**：清理过期的旧版本文件，释放存储
```sql
VACUUM sales_silver;                           -- 默认保留 7 天
VACUUM sales_silver RETAIN 168 HOURS;
```
- ⚠️ VACUUM 后无法 Time Travel 回被删除版本
- 不要在流式查询运行时执行

---

## Q12：Photon Engine 是什么？什么查询受益最大？

Photon 是 Databricks 用 **C++ 重写的向量化查询引擎**，替代部分 Spark SQL 的 Java 执行路径：
- C++ 原生向量化执行（SIMD 指令加速）
- 显式内存管理（无 JVM GC 停顿）
- 不支持的算子自动 Fallback 到 Spark

**受益最大**：聚合查询（2-5x）、大表 JOIN、Parquet 扫描、窗口函数

**不需要 Photon**：小数据探索（秒级完成，Photon 启动有开销）、大量 Python UDF（Photon 无法加速）

---

## Q13：Spark UI 里你会看哪些指标排查性能问题？

**排查流程**：
```
作业慢了 → Stages 页找最慢 Stage
    ├── Shuffle 慢？→ SQL 页确认 Join 策略 → 调 Broadcast / 优化分区
    ├── 计算慢？→ Executor GC 时间 > 10% → 加大内存
    ├── 个别 Task 慢？→ 数据倾斜 → Salting / AQE
    └── Scan 慢？→ 小文件多 → OPTIMIZE
```

**关键指标**：
- Stages → Shuffle Read/Write Size（是否合理）、Task Duration 分布（Max vs Median 差异大 = 倾斜）
- SQL → 物理计划（确认 Broadcast Join 是否生效）
- Executors → GC Time、Failed Tasks

---
# 二、跨平台数据迁移（DataWorks → Databricks）

---

## Q14：从 DataWorks 迁移到 Databricks，按什么步骤来做？迁移前梳理哪些内容？

### 五阶段迁移

```
Phase 1: 资产盘点（2-3周）
├── 梳理所有数据表（数量、大小、分区策略）
├── 梳理所有调度任务（DAG 依赖、定时配置）
├── 梳理所有 MaxCompute SQL 脚本
├── 识别历史数据 vs 增量任务
└── 输出资产清单 + 依赖关系图

Phase 2: SQL 转换（2-4周）← 最耗时
├── MaxCompute SQL → Spark SQL 语法转换
├── UDF 重写（MaxCompute UDF → Spark Pandas UDF）
└── 自动化转换脚本 + 人工复核

Phase 3: 基础设施搭建（1周）
├── Databricks Workspace + Unity Catalog 初始化
├── 网络打通（阿里云 → AWS/Azure）
└── 迁移通道搭建

Phase 4: 数据迁移执行
├── 历史全量迁移（一次性）
├── 增量同步管道上线
├── 三重数据一致性校验
└── 灰度切换

Phase 5: 调度与验证（2周）
├── Databricks Workflows 重编排调度
├── 新老系统并行运行对比
├── 业务 UAT
└── 正式切流
```

### 迁移前梳理清单

| 类别 | 梳理内容 | 输出物 |
|------|----------|--------|
| 数据表 | 表名、行数、存储大小、分区、格式 | 数据资产清单 |
| SQL 脚本 | 所有 ETL SQL / 存储过程 / UDF | 脚本清单 + 复杂度评估 |
| 调度 | 任务依赖 DAG、定时配置、SLA | 调度拓扑图 |
| 下游消费 | 报表、接口、数据消费者 | 下游影响分析 |
| 数据质量 | 关键表校验规则、数据字典 | 校验规则文档 |

---

## Q15：MaxCompute SQL 迁移到 Spark SQL，语法上主要有哪些差异？怎么批量转换？

### 主要语法差异

| 场景 | MaxCompute SQL | Spark SQL |
|------|---------------|-----------|
| 空值处理 | `NVL(col, default)` | `COALESCE(col, default)` |
| 字符串聚合 | `WM_CONCAT(col, ',')` | `CONCAT_WS(',', COLLECT_LIST(col))` |
| 日期函数 | `TO_DATE('20260101', 'yyyymmdd')` | `TO_DATE('2026-01-01')` |
| DDL | `CREATE TABLE ... PARTITIONED BY` | `CREATE TABLE ... USING DELTA` |
| UDF | MaxCompute UDF（Java） | Spark Pandas UDF（Python） |
| 数据类型 | `DATETIME` | `TIMESTAMP` |
| 动态分区 | 需显式指定分区 | Delta 自动处理 |

### 批量转换方案

```python
# 正则替换规则
rules = [
    (r'\bNVL\(', 'COALESCE('),
    (r'\bDATETIME\b', 'TIMESTAMP'),
]

def convert_maxcompute_to_spark(sql):
    for pattern, replacement in rules:
        sql = re.sub(pattern, replacement, sql)
    return sql

# 批量处理：遍历 SQL 文件 → 转换 → 输出
# 标记无法自动转换的部分（WM_CONCAT, TRANS_ARRAY 等）→ -- TODO: 需人工确认
```

---

## Q16：历史数据（2023-2025）一次性迁移，怎么设计方案？如何校验一致性？

### 分批策略

| 方案 | 适用场景 | 优缺点 |
|------|----------|--------|
| 按年份/月份分批 | 数据量 > 1TB | 灵活、可中断恢复、需断点续传 |
| 按业务域分批 | 多业务线 | 灰度切换，单域验证通过再迁移下一个 |
| 全量快照 | 数据量 < 1TB | 简单但风险集中 |

**推荐**：高优先级小表先跑通 → 核心交易表（2025→2024→2023，从近到远）→ 日志表异步迁移

### 三重校验

```python
# 第1重：行数校验
assert spark.sql("SELECT COUNT(*) FROM source").collect()[0][0] == \
       spark.sql("SELECT COUNT(*) FROM target").collect()[0][0]

# 第2重：MD5 Checksum（所有列 concat 后 MD5 去重计数）
from pyspark.sql.functions import md5, concat_ws
source_md5 = spark.table("source").select(md5(concat_ws("|", "*"))).distinct().count()

# 第3重：业务规则（金额汇总、COUNT DISTINCT 关键维度）
assert abs(spark.sql("SELECT SUM(amount) FROM source").collect()[0][0] - 
           spark.sql("SELECT SUM(amount) FROM target").collect()[0][0]) < 0.01
```

---

## Q17：跨云数据迁移（阿里云 → AWS/Azure），传输方式怎么选？带宽受限怎么办？

### 传输方式

| 方式 | 适用场景 | 速度 | 成本 |
|------|----------|------|------|
| 中转存储（OSS → S3） | 数据量 > 100GB | 快（云间带宽高） | OSS+S3 存储费 |
| 公网传输 | < 100GB，临时 | 慢 | 出口流量费 |
| 专线/VPN | > 1TB，长期同步 | 稳定但贵 | 专线月租 |
| Spark 直连 | 表级同步 | 中等 | 执行资源费 |

**推荐**：MaxCompute → OSS（导出为 Parquet）→ S3/ADLS → Databricks Auto Loader → Delta

### 带宽受限应对
1. Parquet + Snappy 压缩（比 CSV 小 5-10x）
2. 断点续传（大文件分片）
3. 错峰传输（凌晨低峰期）
4. 增量优先（先同步增量，历史异步慢慢迁移）

---

## Q18：迁移后如何验证数据逻辑一致性？

### 分层校验

```
Level 1: 技术校验（自动化，每批迁移完执行）
├── 行数对比、列数+列名+类型对比
├── NULL 值占比对比
└── 数值列 SUM/MIN/MAX/AVG 对比

Level 2: 逻辑校验（核心表抽样）
├── 关键维度 COUNT DISTINCT 对比
├── JOIN 后结果集对比
└── 时间窗口数据对比（最近 7 天）

Level 3: 业务校验（业务方参与）
├── 核心报表指标对比
├── 驾驶舱大屏数字对比
└── 10 个最关键 KPI 偏差 < 0.1%
```

### 差异排查
```python
# FULL OUTER JOIN 找出差异行
diff = spark.sql("""
    SELECT s.order_id, s.amount as src_amount, t.amount as tgt_amount
    FROM source_table s
    FULL OUTER JOIN target_table t ON s.order_id = t.order_id
    WHERE s.amount <> t.amount OR s.order_id IS NULL OR t.order_id IS NULL
    LIMIT 100
""")
```

---
# 三、数据架构与 ETL 设计

---

## Q19：Medallion 架构（Bronze → Silver → Gold）怎么理解？每层做什么？

```
┌──────────────────────────────────────┐
│            Gold 层                     │
│  业务聚合表 / BI 数据集 / 特征宽表       │
│  → 面向业务消费，去范式化               │
│  → 保留：3 年（按业务需求）             │
├──────────────────────────────────────┤
│           Silver 层                    │
│  清洗后 / 标准化 / 关联后的中间层        │
│  → 去重、标准化、关联整合               │
│  → 保留：1-2 年                       │
├──────────────────────────────────────┤
│           Bronze 层                    │
│  原始数据快照（Source of Truth）        │
│  → 保留原始格式，只追加不修改           │
│  → 保留：5-7 年（金融合规）            │
└──────────────────────────────────────┘
```

| Medallion | 传统数仓 | FMSCC 示例 |
|-----------|----------|-----------|
| Bronze | ODS（贴源层） | 原始 DMS CSV、线索中心 JSON |
| Silver | DWD + DWS | 清洗后销售明细、去重线索 |
| Gold | ADS + DIM | CDP 看板数据集、DIP 汇总 |

---

## Q20：数仓分层（ODS/DWD/DWS/ADS/DIM）和 Medallion 架构怎么映射？

```
传统分层          Medallion       FMSCC 实际映射
ODS（贴源层）  →  Bronze    →  原始 DMS 全量数据，不做清洗
DWD（明细层）  →  Silver    →  去重后销售明细、线索清洗表
DWS（汇总层）  →  Silver    →  按天/月/经销商汇总的销售表
ADS（应用层）  →  Gold      →  CDP BI 看板数据集、DIP 驾驶舱汇总
DIM（维度层）  →  Gold      →  经销商维度、车型维度、日期维度
```

核心理念一致（原始 → 清洗 → 汇总 → 应用），区别在于 Delta Lake 用同一存储实现全部层次。

---

## Q21：Bronze 层用什么存储格式？为什么 Delta 优于原始 JSON/CSV/Parquet？

**统一使用 Delta Lake**：
```python
(spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")   # 源是 JSON
    .load("/mnt/landing/")
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file", input_file_name())
    .writeStream.format("delta")           # 存为 Delta
    .table("fmscc_bronze.dms_raw")
)
```

**为什么用 Delta 而非原始格式**：

| 需求 | 原始格式 | Delta |
|------|---------|-------|
| ACID 事务 | ❌ 写入中断产生脏数据 | ✅ |
| Time Travel | ❌ | ✅ 回滚到任意版本 |
| Schema 约束 | ❌ 字段漂移 | ✅ 防脏数据 |
| 增量处理 | ❌ 手动跟踪文件 | ✅ Auto Loader + Checkpoint |
| Schema Evolution | ❌ 手动处理 | ✅ 自动合并 |

---

## Q22：全量 + 增量双通道怎么设计？增量数据怎么识别？

### 双通道设计

```
全量通道（一次性 / 周期补偿）
├── 初次上线：全量拉取
├── 每周补偿：修正增量延迟问题
└── 故障恢复：增量链路断太久时使用

增量通道（持续运行）
├── 准实时捕获变更（15min / 1h）
└── MERGE INTO Delta 表
```

### 增量识别方式

| 方式 | 原理 | 优缺点 | FMSCC 适用场景 |
|------|------|--------|---------------|
| 时间戳字段 | `update_time > last_max_time` | 简单，无法捕获物理删除 | DMS 销售数据 |
| 自增 ID/版本号 | `version_id > last_version` | 精确，无法捕获删除 | 线索中心 |
| CDC（binlog） | 数据库日志 | 捕获所有 I/U/D，需源库权限 | 高实时性场景 |
| 文件时间戳 | 文件落地时间/MD5 | 零侵入 | DMS 导出的 CSV |

```python
# 增量 MERGE 示例
incremental_df = spark.read.jdbc(url, 
    f"(SELECT * FROM dms_sales WHERE update_time > '{last_max_time}') AS tmp")
delta_table.alias("target").merge(
    incremental_df.alias("source"),
    "target.order_id = source.order_id"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
```

---

## Q23：DMS 每天推送全量文件，怎么高效做增量合并？

```python
# 方案：Hash 指纹对比 + MERGE INTO

# Step 1: 计算 MD5 行指纹
df_new = spark.table("fmscc_bronze.dms_full") \
    .filter("ingestion_date = current_date()") \
    .withColumn("row_hash", md5(concat_ws("|", *all_columns)))

# Step 2: 找出新增 + 变更
new_records = df_new.join(df_existing.select("order_id"), "order_id", "left_anti")
changed_records = df_new.alias("n").join(df_existing.alias("e"), "order_id") \
    .filter(col("n.row_hash") != col("e.row_hash")).select("n.*")

# Step 3: MERGE
delta_silver.alias("target").merge(
    new_records.union(changed_records).alias("source"),
    "target.order_id = source.order_id"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
```

**优化技巧**：Hash 对比替代逐字段对比、分区裁剪（只 JOIN 近 3 天）、Z-Order 加速 JOIN

---

## Q24：Late-arriving data（迟到数据）你怎么处理？

### 策略

| 场景 | 处理方式 |
|------|----------|
| 销售 6/1 完成，6/4 才推送 | 按业务日期（order_date）覆盖对应分区 |
| update_time 是 6/1，到达是 6/4 | 按 update_time 分区，迟到覆盖旧分区 |
| 同一条记录先到旧版本后到新版本 | 按 version_id 去重，保留最新 |

### 实现

```python
# 方案 1：分区覆盖（迟到数据覆盖对应日期分区）
df.write.mode("overwrite") \
    .option("replaceWhere", f"order_date = '{late_date}'") \
    .saveAsTable("fmscc_silver.dms_sales")

# 方案 2：去重 Window + MERGE
late_df.withColumn("rn", row_number().over(
    Window.partitionBy("order_id").orderBy(col("update_time").desc())
)).filter("rn = 1")  # 保留最新版本

# 方案 3：标记迟到标签
df.withColumn("late_flag", 
    when(datediff(current_date(), col("order_date")) > 2, True).otherwise(False))
```

**Gold 层处理**：迟到数据 MERGE 到 Silver 后，重算受影响日期的 Gold 分区（`replaceWhere`）。

---
# 四、数据质量与运维

---

## Q25：数据质量的六个维度怎么做自动化监控？Databricks 上怎么实现？

| 维度 | 含义 | 监控规则示例 |
|------|------|-------------|
| 完整性 | 数据不缺失 | 关键字段 NULL 率 < 1% |
| 唯一性 | 无重复 | `COUNT(*) = COUNT(DISTINCT pk)` |
| 一致性 | 跨表一致 | CDP 销售额 = DIP 销售额（偏差 < 0.1%） |
| 准确性 | 值合理 | 金额 > 0，日期在合理范围 |
| 及时性 | 按时到达 | 每日 8:00 前 Bronze 有前一天数据 |
| 有效性 | 符合业务规则 | 订单状态在枚举值内、车型在维度表中 |

### Databricks 实现

```python
def data_quality_check(spark, table_name, rules):
    """执行数据质量规则，质量不过则 Fail Task"""
    for rule in rules:
        result = spark.sql(rule["sql"]).collect()[0][0]
        passed = result >= rule.get("threshold", 0.95)
        if not passed:
            dbutils.notebook.exit(f"质量失败: {rule['name']} = {result}")
    return spark.createDataFrame([{"table": table_name, "status": "PASSED"}])

# 规则配置化
quality_rules = {
    "fmscc_silver.dms_sales": [
        {"name": "完整性-关键字段NULL", "sql": "SELECT 1.0 - SUM(CASE WHEN order_id IS NULL OR amount IS NULL THEN 1 END)/COUNT(*) FROM fmscc_silver.dms_sales WHERE order_date = current_date()"},
        {"name": "唯一性-订单号", "sql": "SELECT CASE WHEN COUNT(*) = COUNT(DISTINCT order_id) THEN 1.0 ELSE 0.0 END FROM fmscc_silver.dms_sales WHERE order_date = current_date()"},
        {"name": "准确性-金额>0", "sql": "SELECT 1.0 - SUM(CASE WHEN amount <= 0 THEN 1 END)/COUNT(*) FROM fmscc_silver.dms_sales WHERE order_date = current_date()"},
    ]
}
# 结果写入监控表，用于仪表盘展示趋势
```

---

## Q26：ETL 作业凌晨 3 点失败，怎么设计告警和重试机制？怎么保证不丢不重？

### 告警流程

```
ETL 失败 → Databricks Workflows 捕获
    ├── 自动重试（最多 3 次，间隔 5min/15min/30min）
    ├── 3 次均失败 → 触发告警
    │   ├── Email：oncall 邮箱（含 Task 名、日志链接）
    │   ├── Webhook：企微/钉钉群通知
    │   └── 严重：PagerDuty 电话告警
    └── 告警内容：Task + 失败时间 + 错误 + 影响表 + Spark UI 链接
```

### 不丢不重保障

```python
# 保障 1：分区覆盖（幂等写入）
df.write.mode("overwrite") \
    .option("replaceWhere", f"report_date = '{target_date}'") \
    .saveAsTable("fmscc_gold.daily_sales")
# 同一天重跑 N 次，覆盖同一分区，不重复

# 保障 2：MERGE INTO（增量幂等）
delta_table.alias("target").merge(source_df.alias("source"), 
    "t.order_id = s.order_id AND t.order_date = s.order_date"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# 保障 3：Bronze 层兜底
# 即使 Silver/Gold 损坏，从 Bronze 重算即可恢复

# 保障 4：Checkpoint
# Auto Loader / Structured Streaming 的 Exactly-Once 语义
```

---

## Q27：上游 DMS 表结构变更（加字段/改类型），ETL 管道怎么感知和处理？

### 三层防护

```
Layer 1: 预感知
├── 与上游约定变更通知（提前 3 天）
└── Unity Catalog 注册正式 Schema

Layer 2: 自动适应
├── Bronze: Auto Loader schemaEvolutionMode = "addNewColumns"
├── Silver: spark.databricks.delta.schema.autoMerge.enabled = true
└── Rescue Column: 无法解析的数据进 _rescued_data

Layer 3: 告警阻断
├── 字段数变化超阈值 → 人工介入
└── 关键字段类型变更 → 阻断并告警
```

```python
# Bronze 层配置
.option("cloudFiles.schemaEvolutionMode", "addNewColumns")  # 允许加列
.option("cloudFiles.rescuedDataColumn", "_rescued_data")     # 兜底

# Silver 层配置
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
```

---

## Q28：如何做到 ETL 作业幂等？同一天重跑 3 次不重复。

| 操作 | 是否幂等 | 说明 |
|------|----------|------|
| `INSERT INTO` | ❌ | 用 `MERGE INTO` 替代 |
| `MERGE INTO` | ✅ | 相同主键更新而非新增 |
| `INSERT OVERWRITE` + `replaceWhere` | ✅ | 覆盖指定分区，推荐方案 |
| `DELETE + INSERT` | ⚠️ | 非原子，需用 Delta 事务包裹 |

```python
# 最佳实践：分区覆盖
df.write.mode("overwrite") \
    .option("replaceWhere", f"report_date = '{target_date}'") \
    .partitionBy("report_date") \
    .format("delta") \
    .saveAsTable("fmscc_gold.daily_sales")
```

---

## Q29：数据血缘怎么追踪？CDP 报表某个指标出错，怎么快速定位到上游？

### 血缘方案

1. **Unity Catalog 自动血缘**：Databricks 内置表级/列级血缘，Catalog Explorer 中可视化
2. **自定义血缘日志**：每个 ETL 步骤记录 `{source_tables → target_table → transform_notebook → batch_id}` 到血缘表

### Top-Down 故障排查

```
CDP "本月销量" 偏低 → 
Step 1: 查 Gold 层 — SUM(amount) 确认问题在数据层
Step 2: 查 Silver 层 — 发现某个日期数据量异常
Step 3: 查 Bronze 层 — 发现 DMS 推送少了文件
Step 4: 查 Auto Loader — 某个文件格式异常进了 _rescued_data
Step 5: 修复 — Schema Evolution 加列 + 从 Bronze 重算
```

---
# 五、文档规范与软技能

---

## Q30：你会怎么交付一份规范的数据流图？包含哪些要素？

**必需要素**：
- 数据源系统（方框 `[DMS]`）
- 存储/表（圆角框 `(Delta Table)`）
- 数据流向箭头，标注传输方式 + 频率
- 处理节点（ETL Notebook / Spark Job）
- 目标消费系统
- 各层 SLA（延迟 < 5 分钟）
- 依赖关系文字说明
- 符号图例、版本号、日期

**配套文档**：数据字典（字段名/类型/业务含义/来源）、ETL 代码规范、调度配置

---

## Q31：业务方频繁变更需求（口径要改），你怎么管理变更？

```
需求评估（1 天内）→ 变更方案 → 排期沟通 → 开发验证 → 上线通知

关键实践：
1. 配置化而非硬编码：阈值放配置表，改值不用改代码
2. 口径版本化：cdp_kpi_v1 / cdp_kpi_v2 并存，业务验证后再切
3. 影响分析：涉及哪些表、哪些下游系统、是否需要回填历史
4. 沟通：通知 CDP/DIP 团队，更新数据字典文档
```

---

# 六、场景设计题（高区分度）⭐

---

## Q32（设计题）：DMS 每天 100 万条销售 + 线索 50 万条，需每 15 分钟刷新 CDP 报表。设计完整数据架构。

### 架构

```
数据源层: [DMS CSV] [线索 API] [EMS JSON]
    ↓ Auto Loader
Bronze 层: dms_sales_raw | leads_raw | ems_raw (Append-only, Delta Lake)
    ↓ 每小时 Spark ETL (清洗/去重)
Silver 层: dms_sales_clean | leads_clean | dim_dealer/vehicle/customer
    ↓ 每 15 分钟 Spark SQL 聚合
Gold 层: cdp_kpi_daily | dip_summary | dealer_ranking (replaceWhere 分区覆盖)
    ↓
消费层: [SQL Warehouse → CDP BI 报表] [API → DIP 企微驾驶舱]
```

### 核心技术决策

| 决策点 | 选择 | 理由 |
|--------|------|------|
| 存储格式 | Delta Lake（全部三层） | ACID + Time Travel + Schema Evolution |
| 摄入 | Auto Loader + cloudFiles | Exactly-Once |
| 清洗 | MERGE INTO | 增量 Upsert，幂等 |
| 聚合 | partitionOverwrite | 只覆盖当天分区，瞬时完成 |
| 15 分钟刷新 | Gold 层 replaceWhere | 只重算当天，非全表 |
| BI 查询 | SQL Warehouse | 低延迟、高并发 |
| 治理 | Unity Catalog | 血缘 + 权限 |

### 性能指标
- Bronze 摄入延迟：< 5 分钟
- Silver 清洗延迟：< 30 分钟
- Gold 聚合：< 15 分钟（仅当日分区）
- DMS 100 万条增量处理：每次约 5 万条，< 2 分钟

---

## Q33（排查题）：DIP 驾驶舱"本月销量"和 CDP 报表差了 5%，怎么排查？

### Top-Down 排查链

```
Step 1: 确认问题范围
├── 哪个指标？什么时间范围？差多少？
├── CDP 和 DIP 筛选条件一致吗？
└── 两边刷新时间一致吗？

Step 2: 数据链路回溯
├── CDP → fmscc_gold.cdp_daily_sales
├── DIP → fmscc_gold.dip_summary
└── 两边是否来自同一 Silver 表？

Step 3: 逐日对比找到差异日期
├── 6月12日、6月15日 CDP 和 DIP 不一致
└── 不是持续性问题，是某几天

Step 4: 深入差异
├── 6/12：CDP 50条 vs DIP 45条
├── 少的 5 条全部来自经销商 A，新能源车
└── 线索：DIP 汇总时可能遗漏了某个维度

Step 5: 定位根因
├── DIP Gold SQL 中 JOIN dim_dealer 用了 INNER JOIN
├── 经销商 A 在 dim_dealer 维度表缺少记录
├── INNER JOIN 导致 5 条销售记录被丢弃
└── 根因：维度表未及时更新 + INNER JOIN

Step 6: 修复
├── 短期：补全 dim_dealer 缺失记录 + 重跑当日 Gold
├── 长期：LEFT JOIN + COALESCE('未知经销商') 兜底
├── 监控：每日检查 JOIN 后行数是否减少
└── 复盘报告给业务方
```

### 排查 SQL
```sql
-- 快速对比
SELECT 'CDP' as src, SUM(amount), COUNT(DISTINCT order_id) 
FROM fmscc_gold.cdp_daily_sales 
WHERE report_date BETWEEN '2026-06-01' AND '2026-06-17'
UNION ALL
SELECT 'DIP', SUM(amount), COUNT(DISTINCT order_id) 
FROM fmscc_gold.dip_summary 
WHERE report_date BETWEEN '2026-06-01' AND '2026-06-17';

-- 逐日对比找差异
SELECT a.report_date, a.total_sales as cdp, b.total_sales as dip, 
       a.total_sales - b.total_sales as diff
FROM (SELECT report_date, SUM(amount) as total_sales 
      FROM fmscc_gold.cdp_daily_sales GROUP BY report_date) a
FULL OUTER JOIN 
     (SELECT report_date, SUM(amount) as total_sales 
      FROM fmscc_gold.dip_summary GROUP BY report_date) b 
ON a.report_date = b.report_date
WHERE ABS(a.total_sales - b.total_sales) > 0.01
ORDER BY ABS(a.total_sales - b.total_sales) DESC;
```

---

## Q34（迁移题）：JLR 过去 3 年 3000 张 MaxCompute 表迁移到 Databricks，怎么拆解任务？自动化程度能多高？

### 任务拆解（5 阶段，8-10 周）

```
Week 1-2: 资产盘点
├── 导出 3000 张表清单（表名、行数、大小、分区、最后访问时间）
├── 分类：P0 核心业务 ~200 张 / P1 中间表 ~800 张 
│         P2 日志 ~1000 张 / P3 待定 ~1000 张
└── 梳理表依赖关系 + 拓扑图

Week 2-4: SQL 转换
├── P0：200 张手动逐张核对（核心逻辑不能出错）
├── P1：800 张自动化脚本 + 抽样人工复核
├── P2：1000 张全自动 + 执行验证
└── P3：暂缓，评估后决定

Week 3-4: 基础设施（并行）
├── Databricks Workspace + Unity Catalog
├── 网络通道 + 存储配置
└── 调度模板 + 质检框架

Week 5-7: 分批迁移
├── Batch 1（W5）：P0 200 张 + 并行验证
├── Batch 2（W6）：P1 800 张 + 并行验证
├── Batch 3（W7）：P2 1000 张
└── 每批三重校验 + 灰度切换

Week 8-10: 切换收尾
├── 下游系统逐步指向新平台
├── 旧平台保留 1 个月后下线
└── 文档交付 + 培训
```

### 自动化程度

| 环节 | 自动化 | 工具 |
|------|--------|------|
| 表清单导出 | 100% | `information_schema.tables` |
| SQL 语法转换 | 70% | 正则替换脚本，复杂逻辑人工 |
| DDL 生成 | 95% | Python 读 MaxCompute Schema → 生成 Delta DDL |
| 数据搬运 | 90% | DataX → OSS → S3 → Auto Loader |
| 一致性校验 | 85% | Python 校验脚本（Row Count + Checksum） |
| 调度 DAG | 60% | 解析旧 DAG → Workflows JSON，需人工调整 |
| 质量规则 | 50% | 基础规则自动，业务规则人工 |

```python
# DDL 自动生成示例
def generate_delta_ddl(meta):
    type_map = {'BIGINT':'BIGINT','STRING':'STRING',
                'DATETIME':'TIMESTAMP','DOUBLE':'DOUBLE'}
    cols = [f"  {c['name']} {type_map.get(c['type'],'STRING')}" 
            for c in meta['columns']]
    return f"CREATE TABLE IF NOT EXISTS {meta['table_name']} \
({','.join(cols)}) USING DELTA;"
```

---

## 附录：面试快速复习卡片

| 类别 | 关键概念 | 容易追问的点 |
|------|----------|-------------|
| Spark | Lazy Eval, DAG, Shuffle, Partition | AQE 三优化：动态合并分区、动态切换 Join、动态处理倾斜 |
| Delta Lake | ACID, Time Travel, MERGE, Z-Order | Delta Log 底层（JSON 事务日志） |
| Databricks | Medallion, Auto Loader, Unity Catalog, Workflows | Photon 引擎适用场景 |
| 迁移 | 一致性校验、SQL 转换、灰度切换 | 回滚方案：旧系统保留多久？怎么切回来？ |
| 数据质量 | 六维度、幂等、Schema Evolution | 质量不过阻断还是告警？什么级别阻断？ |
| 场景题 | 架构设计、故障排查、项目拆解 | 你的设计有没有单点故障？怎么容错？ |

---

> 📅 生成日期：2026-06-17  
> 📍 目标岗位：ETL 数据开发工程师 | FMSCC | Databricks / Spark / Delta Lake  
> ⚠️ 本面试题为技术准备参考，面试需结合个人项目经验灵活回答

# PySpark 数据处理进阶面试题库

> 岗位：ETL 数据开发工程师 | 场景：大规模数据清洗、聚合、窗口计算、UDF  
> 配套：[Python数据处理-面试题.md](Python数据处理-面试题.md) | [DataWorks-vs-Databricks-代码对比.md](DataWorks-vs-Databricks-代码对比.md)

---

# 一、窗口函数（Window Functions）⭐ 高频考点

---

## Q1：`row_number()` / `rank()` / `dense_rank()` 的区别？手写 SQL + PySpark 两种实现。

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank, col

df = spark.createDataFrame([
    ("A", 100), ("B", 90), ("C", 90), ("D", 80), ("E", 80), ("F", 70)
], ["dealer", "score"])

window = Window.orderBy(col("score").desc())

df.withColumn("row_number", row_number().over(window)) \
  .withColumn("rank", rank().over(window)) \
  .withColumn("dense_rank", dense_rank().over(window)) \
  .show()
```

| dealer | score | row_number | rank | dense_rank |
|--------|-------|------------|------|------------|
| A      | 100   | 1          | 1    | 1          |
| B      | 90    | 2          | 2    | 2          |
| C      | 90    | 3          | 2    | 2          |
| D      | 80    | 4          | 4    | 3          |
| E      | 80    | 5          | 4    | 3          |
| F      | 70    | 6          | 6    | 4          |

**区别**：同分时，`row_number` 唯一递增、`rank` 跳过、`dense_rank` 不跳过。

**Spark SQL 写法**：
```sql
SELECT dealer, score,
    ROW_NUMBER() OVER (ORDER BY score DESC) as row_number,
    RANK()       OVER (ORDER BY score DESC) as rank,
    DENSE_RANK() OVER (ORDER BY score DESC) as dense_rank
FROM dealer_scores;
```

---

## Q2：`LAG` / `LEAD` 实战：计算每条销售记录与上一条的天数差？

```python
from pyspark.sql.functions import lag, lead, datediff

# 场景：计算同一客户相邻两笔订单的时间间隔
window = Window.partitionBy("customer_id").orderBy("order_date")

df.withColumn("prev_order_date", lag("order_date", 1).over(window)) \
  .withColumn("next_order_date", lead("order_date", 1).over(window)) \
  .withColumn("days_since_last", datediff("order_date", "prev_order_date")) \
  .withColumn("days_to_next", datediff("next_order_date", "order_date"))

# 典型应用：识别「连续3天有交易」的经销商
df.withColumn("prev1", lag("order_date", 1).over(window)) \
  .withColumn("prev2", lag("order_date", 2).over(window)) \
  .withColumn("consecutive_3days",
      (datediff("order_date", "prev1") == 1) &
      (datediff("prev1", "prev2") == 1))
```

**Spark SQL 写法**：
```sql
SELECT customer_id, order_date, amount,
    LAG(order_date, 1)  OVER (PARTITION BY customer_id ORDER BY order_date) AS prev_date,
    LEAD(order_date, 1) OVER (PARTITION BY customer_id ORDER BY order_date) AS next_date,
    DATEDIFF(order_date, LAG(order_date, 1) OVER (PARTITION BY customer_id ORDER BY order_date)) AS days_since_last
FROM orders;
```

---

## Q3：累加求和 + 移动平均 —— `rowsBetween` / `rangeBetween` 怎么用？

```python
from pyspark.sql.functions import sum as spark_sum, avg, col

# 场景：每月销售 + 累计到当前月的总额 + 近3个月移动平均
monthly_sales = spark.table("fmscc_gold.monthly_sales")

window_cum = Window.partitionBy("dealer_id").orderBy("year_month") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    # 含义：从分区第一行到当前行

window_ma3 = Window.partitionBy("dealer_id").orderBy("year_month") \
    .rowsBetween(-2, Window.currentRow)
    # 含义：往前2行到当前行（含当前行共3行）

result = monthly_sales.withColumn("cumulative_sales", spark_sum("amount").over(window_cum)) \
                      .withColumn("moving_avg_3m", avg("amount").over(window_ma3))

# rowsBetween vs rangeBetween：
# - rowsBetween: 按行号计数（不管值差多大，固定向前N行）
# - rangeBetween: 按值范围计数（如 order_date 往前30天，不管中间多少行）
window_range = Window.partitionBy("dealer_id").orderBy("order_date") \
    .rangeBetween(-30 * 86400, Window.currentRow)  # 前30天（需要时间戳类型）
```

**面试追问**：`rangeBetween` 要求 orderBy 列是数字类型，日期要转 timestamp。

---

## Q4：分组取 Top N（每组销售额 Top 5 的经销商）有哪几种写法？

```python
# 方法 1：row_number + filter（最通用）
window = Window.partitionBy("region").orderBy(col("amount").desc())
result = df.withColumn("rn", row_number().over(window)).filter("rn <= 5")

# 方法 2：rank（允许并列）
result = df.withColumn("rk", rank().over(window)).filter("rk <= 5")

# 方法 3：Spark SQL
spark.sql("""
    SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) as rn
        FROM dealer_sales
    ) WHERE rn <= 5
""")

# ⚠️ 常见坑：如果 Top 5 分区间差异巨大（一个区 100 万条一个区 100 条），
# 单分区跑会很慢 → 检查分区数据分布
```

---

## Q5：复杂场景：同一客户单日多次交易只保留最新一条？

```python
# 场景：去重 — 同 customer_id + 同一天，保留 update_time 最新的记录
window = Window.partitionBy("customer_id", "order_date") \
               .orderBy(col("update_time").desc())

deduped = df.withColumn("rn", row_number().over(window)) \
            .filter("rn = 1") \
            .drop("rn")

# 如果有 version_id 而非 update_time：
window = Window.partitionBy("customer_id", "order_date") \
               .orderBy(col("version_id").desc())
```

**面试追问**：为什么不直接 `dropDuplicates(["customer_id", "order_date"])`？
→ `dropDuplicates` 保留的是不确定的哪一条，不能控制保留逻辑。

---

# 二、UDF 与 Pandas UDF ⭐

---

## Q6：普通 UDF vs Pandas UDF，什么场景必须用 Pandas UDF？

```python
from pyspark.sql.functions import udf, pandas_udf
from pyspark.sql.types import StringType, DoubleType, StructType, StructField
import pandas as pd

# === 普通 UDF：逐行处理 ===
# 适用：简单逻辑、非性能关键
@udf(StringType())
def mask_phone(phone):
    if phone and len(phone) >= 7:
        return phone[:3] + "****" + phone[-4:]
    return phone

# === Pandas UDF (Series to Series)：向量化 ===
# 适用：复杂计算、大数据量、需 Pandas/Numpy 操作
@pandas_udf(DoubleType())
def calculate_tax(amount: pd.Series) -> pd.Series:
    """阶梯税率计算"""
    tax = pd.Series(0.0, index=amount.index)
    tax = tax.where(amount <= 5000, amount * 0.03 - 0)
    tax = tax.where(amount <= 50000, amount * 0.10 - 210)
    tax = tax.where(amount <= 100000, amount * 0.20 - 1410)
    return tax

# === Pandas UDF (Grouped Map)：分组处理 ===
# 适用：每组需要独立复杂逻辑（如每组训练一个小模型）
schema = df.select("dealer_id", "amount").schema

@pandas_udf(schema, functionType="grouped_map")
def remove_outliers(pdf: pd.DataFrame) -> pd.DataFrame:
    """每个经销商组内去除金额异常值"""
    q1, q3 = pdf["amount"].quantile([0.25, 0.75])
    iqr = q3 - q1
    return pdf[(pdf["amount"] >= q1 - 1.5 * iqr) & (pdf["amount"] <= q3 + 1.5 * iqr)]

result = df.groupBy("dealer_id").apply(remove_outliers)
```

| 类型 | 输入 → 输出 | 性能 | 典型场景 |
|------|------------|------|---------|
| 普通 UDF | Row → Row | O(slow) | 简单映射 |
| Pandas UDF (Series) | Series → Series | O(fast) | 批量计算、Numpy |
| Pandas UDF (Grouped Map) | DataFrame → DataFrame | 中等 | 组内复杂逻辑 |

**面试追问**：Pandas UDF 为什么快？→ Apache Arrow 列式零拷贝 + 批量调用而非逐行调用。

---

## Q7：UDF 注册为 Spark SQL 函数怎么用？

```python
# 注册 UDF 到 Spark SQL 上下文
spark.udf.register("mask_phone_sql", mask_phone, StringType())
spark.udf.register("calc_tax_sql", calculate_tax, DoubleType())

# 现在可以在所有 SQL 中使用
spark.sql("""
    SELECT order_id, amount,
           MASK_PHONE_SQL(customer_phone) AS masked_phone,
           CALC_TAX_SQL(amount) AS tax
    FROM fmscc_bronze.dms_sales
""")

# Databricks 上：写在 Notebook 前面的 Cmd 里，
# 后续 %%sql cell 都能直接调用
```

---

# 三、Join 优化实战

---

## Q8：500GB 大表 JOIN 500GB 大表（两个都大），怎么优化？

```python
# 场景：两表都几百 GB，都不能 Broadcast

# 策略 1：检查是否能提前过滤减少数据量
df1_filtered = df1.filter(col("order_date") >= "2026-01-01")
df2_filtered = df2.filter(col("order_date") >= "2026-01-01")
result = df1_filtered.join(df2_filtered, "key", "inner")

# 策略 2：Sort Merge Join（默认，数据已在 join key 上有序时最优）
# Spark 自动选择，但可以优化 shuffle 分区数
spark.conf.set("spark.sql.shuffle.partitions", 2000)  # 大数据量调大
spark.conf.set("spark.sql.adaptive.enabled", "true")   # AQE 动态调整

# 策略 3：Bucket Join（数据提前按 Key 分桶，避免 Shuffle）
# 建表时就分好桶
df1.write.bucketBy(200, "order_id").sortBy("order_id").saveAsTable("orders_bucketed")
df2.write.bucketBy(200, "order_id").sortBy("order_id").saveAsTable("payments_bucketed")
# Join 时两个表已按相同 key 分桶，无需 Shuffle
spark.table("orders_bucketed").join(spark.table("payments_bucketed"), "order_id")

# 策略 4：Key 加盐解倾斜 + 两阶段 Join
import pyspark.sql.functions as F
salt_num = 100
# 第一阶段：大表打散小表复制
df1_salted = df1.withColumn("salt", (F.hash("key") % salt_num).cast("int"))
df2_salted = df2.withColumn("salt", F.explode(F.array([F.lit(i) for i in range(salt_num)])))
# 第二阶段：按 salt 局部 Join
result = df1_salted.join(df2_salted, ["key", "salt"])
```

---

## Q9：数据倾斜怎么发现和处理？

```python
# 发现倾斜
# 方法 1：Spark UI → Stages → 某个 Task 耗时远超其他（Max >> Median）
# 方法 2：直接查数据分布
spark.sql("""
    SELECT key, COUNT(*) as cnt
    FROM big_table
    GROUP BY key
    ORDER BY cnt DESC
    LIMIT 20
""").show()

# 处理方案
# 方案 1：AQE 自动处理（开箱即用）
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", 5)
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")

# 方案 2：加盐打散（AQE 之前的经典手写方案）
import random
def salt_key(key, salt_range=100):
    return f"{key}_{random.randint(0, salt_range - 1)}"

salt_udf = udf(salt_key, StringType())

# 大表加盐
big_salted = big_df.withColumn("salted_key", salt_udf("join_key"))
# 小表膨胀（每个 key 复制 100 份）
small_exploded = small_df.crossJoin(
    spark.range(0, 100).withColumnRenamed("id", "salt")
).withColumn("salted_key", F.concat("join_key", F.lit("_"), "salt"))

result = big_salted.join(small_exploded, "salted_key").drop("salted_key", "salt")

# 方案 3：拆分倾斜 key 单独处理
skewed_keys = ["DEALER_A", "DEALER_B"]  # 已知的倾斜 key
skewed_data = big_df.filter(F.col("dealer_id").isin(skewed_keys))
normal_data = big_df.filter(~F.col("dealer_id").isin(skewed_keys))

# 倾斜部分用 Broadcast Join（拆出来之后数据量就小了）
skewed_result = skewed_data.join(F.broadcast(small_df), "dealer_id")
normal_result = normal_data.join(small_df, "dealer_id")
result = skewed_result.union(normal_result)
```

---

# 四、复杂数据类型处理

---

## Q10：Struct / Array / Map 类型怎么操作？

```python
from pyspark.sql.functions import struct, array, map_from_arrays, explode, col

# === Struct: 从多列构造、提取子字段 ===
df.withColumn("address", struct("province", "city", "district")) \
  .withColumn("city_from_struct", col("address.city"))

# === Array: 构造、展开、按索引取 ===
df.withColumn("scores", array("score_m1", "score_m2", "score_m3")) \
  .withColumn("first_score", col("scores")[0])                 # 按索引取
  .withColumn("exploded", explode("scores"))                    # 一行变多行

# 处理 JSON 数组
# {"orders": [{"id":1, "amt":100}, {"id":2, "amt":200}]}
df.withColumn("order", explode(col("orders"))) \
  .withColumn("order_id", col("order.id")) \
  .withColumn("amount", col("order.amt"))

# === Map: 构造、取 key ===
df.withColumn("props", map_from_arrays(
    array(F.lit("color"), F.lit("size")),
    array(col("color_val"), col("size_val"))
)).withColumn("color", col("props")["color"])

# 复杂 JSON schema 提取
df.withColumn("parsed", F.from_json("json_str", schema)) \
  .select("parsed.*")  # 展开所有字段

# 嵌套数据打平（flatten）
df.select(
    "order_id",
    F.explode("items").alias("item")
).select(
    "order_id",
    "item.sku",
    "item.quantity",
    "item.price"
)
```

---

## Q11：如何用 Spark 高效读取和解析嵌套 JSON？

```python
# 场景：API 日志，每行是一个深层嵌套 JSON

# 方法 1：自动推断 Schema（开发阶段）
df = spark.read.option("inferSchema", "true").json("/path/to/logs")
df.printSchema()  # 看结构

# 方法 2：手动指定 Schema（生产环境，避免推断开销）
from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType

schema = StructType([
    StructField("trace_id", StringType(), True),
    StructField("timestamp", LongType(), True),
    StructField("request", StructType([
        StructField("method", StringType(), True),
        StructField("path", StringType(), True),
        StructField("headers", MapType(StringType(), StringType()), True),
    ]), True),
    StructField("response", StructType([
        StructField("status", LongType(), True),
        StructField("body", StructType([
            StructField("code", StringType(), True),
            StructField("data", ArrayType(StructType([
                StructField("dealer_id", StringType()),
                StructField("sales", LongType()),
            ]))),
        ]), True),
    ]), True),
])

df = spark.read.schema(schema).json("/path/to/logs")

# 打平 3 层嵌套
flat = df.select(
    "trace_id",
    col("request.method"),
    col("response.status"),
    col("response.body.code"),
    F.explode("response.body.data").alias("item")
).select(
    "trace_id", "method", "status", "code",
    col("item.dealer_id"),
    col("item.sales")
)

# Databricks 优势：Auto Loader 自动推断和演化 Schema
(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/checkpoint/schema")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load("/mnt/raw/logs/")
    .writeStream
    .option("checkpointLocation", "/checkpoint/logs")
    .trigger(availableNow=True)
    .table("fmscc_bronze.api_logs"))
```

---

# 五、缓存与持久化

---

## Q12：`cache()` / `persist()` / `checkpoint()` 的区别和使用场景？

```python
# cache() = persist(MEMORY_AND_DISK)
# 场景：一个 DataFrame 被多次使用
df = spark.table("fmscc_bronze.dms_sales").filter("order_date >= '2026-01-01'")
df.cache()

# 第一次 Action 时计算并缓存
df.count()  # 触发计算，结果进缓存

# 后续直接从内存读，跳过重算
result1 = df.groupBy("dealer_id").agg(F.sum("amount"))
result2 = df.groupBy("order_date").agg(F.count("*"))
# result1 和 result2 都受益于缓存

# === 不同存储级别 ===
from pyspark.storagelevel import StorageLevel

# MEMORY_ONLY: 只内存，不够就重算（适合小数据）
df.persist(StorageLevel.MEMORY_ONLY)

# MEMORY_AND_DISK: 内存 + 磁盘溢出（最常用，不丢数据）
df.persist(StorageLevel.MEMORY_AND_DISK)

# MEMORY_AND_DISK_SER: 序列化压缩，省内存但费 CPU（数据量大时）
df.persist(StorageLevel.MEMORY_AND_DISK_SER)

# DISK_ONLY: 内存紧张时（不常用）
df.persist(StorageLevel.DISK_ONLY)

# === checkpoint: 截断血缘，防止 DAG 太长 ===
# 场景：迭代算法 / 非常长的 ETL 链路
spark.sparkContext.setCheckpointDir("/tmp/checkpoint")
df.checkpoint()  # 物化到磁盘，截断 RDD Lineage

# cache vs persist vs checkpoint：
# - cache: persist 的简化版，MEMORY_AND_DISK
# - persist: 可指定存储级别
# - checkpoint: 截断血缘（血统太长失败恢复慢），数据写可靠存储

# ⚠️ 常见坑：
# 1. 用完记得 unpersist()，否则占用 Executor 内存
df.unpersist()
# 2. 不要对每次只遍历一次的数据 cache（白浪费内存）
# 3. 不要对大 DataFrame 用 MEMORY_ONLY（容易 OOM）
```

---

## Q13：什么情况下该 cache，什么情况下不该 cache？

```python
# ✅ 该 cache：
# 1. DataFrame 被多次使用（多次 groupBy / join）
base = spark.table("big_table").cache()
df1 = base.groupBy("col1").count()  # ← 第二次使用
df2 = base.groupBy("col2").sum()    # ← 第三次使用

# 2. 迭代算法中重复使用的数据
for i in range(10):
    model = train_step(cached_training_data, model)  # 每轮迭代都用

# 3. 探索性分析时反复查询的中间结果

# ❌ 不该 cache：
# 1. 只用一次的数据
df = spark.table("big_table").cache()  # 多余
df.count()

# 2. 数据太大，缓存会 OOM
# 先看数据量：df.count() 后用 print(df.storageLevel) 看是否已缓存

# 3. 流式查询中不断增长的数据（无限数据流）

# 调试技巧：在 Spark UI Storage 页查看缓存命中率
```

---

# 六、文件读写与分区

---

## Q14：Spark 读文件时分区数怎么确定？小文件问题怎么办？

```python
# === 分区数确定 ===
# 读取时 Spark 根据文件大小和分片大小决定分区数
# 默认：spark.sql.files.maxPartitionBytes = 128MB

# 场景 1：一个 1GB 的文件 → 约 8 个分区 (1024 / 128)
df = spark.read.parquet("/data/1gb_file.parquet")
print(df.rdd.getNumPartitions())  # 8

# 场景 2：1000 个 1MB 的小文件 → 1000 个分区（太多了！）
# 问题：每个 Task 处理 1MB 太轻量，调度开销 >> 计算开销

# 解决小文件问题
# 方法 1：合并小文件（读后 repartition）
df = spark.read.parquet("/data/many_small_files/")
df = df.repartition(10)  # 合并到 10 个分区

# 方法 2：coalesce（减少分区，不 Shuffle，更快但不能增加）
df = df.coalesce(10)  # 窄依赖合并，无 Shuffle

# 方法 3：调整参数读时就合并
spark.conf.set("spark.sql.files.openCostInBytes", 4194304)   # 4MB：小于此的文件合并读
spark.conf.set("spark.sql.files.maxPartitionBytes", 134217728)  # 128MB

# 方法 4：Delta Lake OPTIMIZE（推荐生产方案）
spark.sql("OPTIMIZE fmscc_bronze.dms_sales")
spark.sql("OPTIMIZE fmscc_bronze.dms_sales ZORDER BY (order_date)")
```

---

## Q15：`repartition` vs `coalesce` 的区别？

| 维度 | `repartition(n)` | `coalesce(n)` |
|------|------------------|---------------|
| 能否增加分区 | ✅ | ❌（只能减少） |
| 是否 Shuffle | ✅ 全量 Shuffle | ❌ 窄依赖（部分移动） |
| 数据均衡 | ✅ Hash 重分布，平衡 | ⚠️ 可能不均衡 |
| 速度 | 慢（网络+磁盘 IO） | 快（本地合并） |
| 使用场景 | 增加并行度 | 减少分区数（如写入前） |

```python
# coalesce 典型场景：写入前合并分区，避免小文件
df = spark.table("big_table").filter("date = '2026-06-17'")
# 过滤后只剩 5 万行，但有 200 个分区
df.coalesce(1).write.csv("/output/today.csv")  # 合并为 1 个文件（小数据时）

# 更好的做法：预估输出大小
row_count = df.count()
target_partitions = max(1, row_count * estimated_row_bytes // (128 * 1024 * 1024))
df.coalesce(target_partitions).write.parquet("/output/")

# ⚠️ coalesce(1) 风险：所有数据汇聚到一个 Executor，OOM 风险
# 生产上建议 ≥ 4 个分区
```

---

## Q16：分区表怎么设计分区策略才高效？

```python
# 分区键选择原则
# ✅ 好分区键：
# - 过滤条件最常用的列（WHERE order_date = '2026-06-17'）
# - 基数适中（几百到几万，不要太多也不要太少）
# - 分区大小 100MB-1GB 之间

# ❌ 坏分区键：
# - 基数太高：user_id（产生几十万个分区，元数据爆炸）
# - 基数太低：gender（只有 M/F 两个分区，每个几百 GB）
# - 高并发写入同一分区（产生锁竞争）

# 建分区表
spark.sql("""
    CREATE TABLE fmscc_silver.dms_sales (
        order_id STRING,
        dealer_id STRING,
        amount DOUBLE,
        order_date DATE
    ) USING DELTA
    PARTITIONED BY (order_date)
""")

# 写入时自动分区
df.write.mode("append").partitionBy("order_date").saveAsTable("fmscc_silver.dms_sales")

# 分区裁剪：只读需要的分区
# ✅ Spark 自动裁剪分区，性能提升巨大
spark.table("fmscc_silver.dms_sales").filter("order_date = '2026-06-17'")
# → 只读 order_date=2026-06-17 这一个分区

# ⚠️ 避免全表扫描写法
# ❌ spark.table("...").filter("YEAR(order_date) = 2026")  -- 函数包裹，无法分区裁剪
# ✅ spark.table("...").filter("order_date BETWEEN '2026-01-01' AND '2026-12-31'")
```

---

# 七、流处理基础

---

## Q17：Structured Streaming 中 `append` / `update` / `complete` 输出模式有什么区别？

```python
# 场景：实时销售数据流
streaming_df = (spark.readStream
    .format("delta")
    .table("fmscc_bronze.dms_sales"))

# append 模式：只输出新增行（默认）
# 适用：无聚合的流，如过滤 → 写出
streaming_df.filter("amount > 0") \
    .writeStream.outputMode("append") \
    .trigger(processingTime="1 minute") \
    .format("delta").table("fmscc_silver.sales_positive")

# update 模式：只输出变更的行
# 适用：有聚合，关心最新结果
streaming_df.groupBy("dealer_id").agg(F.sum("amount").alias("total")) \
    .writeStream.outputMode("update") \
    .trigger(processingTime="1 minute") \
    .format("delta").table("fmscc_gold.dealer_realtime_sales")

# complete 模式：每次输出完整结果集（全量覆盖）
# 适用：结果集本身很小的聚合
# ⚠️ 结果集不能太大，否则每次写全量太慢

# outputMode 对比
# append:  「新来的行新增到结果表」 — 无聚合的流
# update:  「变了哪行更新哪行」 — 有聚合，需看到变化
# complete: 「每次全量覆盖整个结果表」 — 小结果集
```

---

## Q18：流处理中的 Watermark 和状态管理？

```python
# Watermark：处理迟到数据的阈值
# "等多久就不再等迟到的数据了"

from pyspark.sql.functions import window, current_timestamp

events = spark.readStream.format("delta").table("fmscc_bronze.events")

# 10 分钟窗口 + 5 分钟 Watermark（迟到超过 5 分钟就丢掉）
windowed = events \
    .withWatermark("event_time", "5 minutes") \
    .groupBy(
        window("event_time", "10 minutes", "5 minutes"),  # 窗口 10 分钟，滑动步长 5 分钟
        "event_type"
    ).count()

windowed.writeStream \
    .outputMode("update") \
    .option("checkpointLocation", "/checkpoint/events/") \
    .trigger(processingTime="1 minute") \
    .format("delta").table("fmscc_gold.event_counts")

# ⚠️ Watermark 必须有：没有 Watermark 的状态无限增长，最终 OOM
# ⚠️ Watermark 只能用于 append/update 模式，不能用于 complete
```

---

# 八、错误处理与监控

---

## Q19：PySpark 作业中怎么捕获异常并优雅退出？

```python
import sys
import traceback

def run_etl(spark, target_date):
    """ETL 主流程，异常时写日志并抛出明确错误"""
    try:
        # Step 1: Extract
        logger.info(f"开始抽取 Bronze 数据, date={target_date}")
        bronze_df = spark.table("fmscc_bronze.dms_sales") \
            .filter(f"order_date = '{target_date}'")
        row_count = bronze_df.count()
        logger.info(f"抽取完成: {row_count} 行")

        if row_count == 0:
            raise ValueError(f"日期 {target_date} 无数据，请检查上游推送")

        # Step 2: Transform
        logger.info("开始清洗转换")
        silver_df = clean_and_transform(bronze_df, target_date)
        logger.info(f"清洗完成: {silver_df.count()} 行")

        # Step 3: Quality Check
        logger.info("开始质量校验")
        quality_results = validator.validate(silver_df)
        failed = [r for r in quality_results if not r.passed and r.severity == "ERROR"]
        if failed:
            raise ValidationError(f"质量校验失败: {[f.name for f in failed]}")

        # Step 4: Load
        logger.info("开始写入 Silver 层")
        (silver_df.write.mode("overwrite")
             .option("replaceWhere", f"order_date = '{target_date}'")
             .saveAsTable("fmscc_silver.dms_sales_clean"))
        logger.info("ETL 完成")

    except ValueError as e:
        logger.error(f"数据异常: {e}")
        # 数据异常不重试，直接告警
        raise
    except ValidationError as e:
        logger.error(f"质量异常: {e}")
        raise
    except Exception as e:
        logger.exception(f"ETL 未知异常: {e}")
        raise
```

---

## Q20：Spark 作业的 Metrics 怎么自定义上报？

```python
# 方式 1：简单计数器
from pyspark import Accumulator

invalid_rows = spark.sparkContext.accumulator(0)
null_amount_rows = spark.sparkContext.accumulator(0)

def validate_and_count(row):
    global invalid_rows, null_amount_rows
    if row.amount is None:
        null_amount_rows.add(1)
    if row.amount is not None and row.amount < 0:
        invalid_rows.add(1)
    return row

df.foreach(validate_and_count)
print(f"空金额: {null_amount_rows.value}, 负金额: {invalid_rows.value}")

# 方式 2：SparkListener（高级，自定义 Metrics）
class ETLMetricsListener(SparkListener):
    def __init__(self):
        self.stage_metrics = {}

    def onStageCompleted(self, stageCompleted):
        stage_id = stageCompleted.stageInfo.stageId
        metrics = stageCompleted.stageInfo.taskMetrics
        self.stage_metrics[stage_id] = {
            "executor_run_time": metrics.executorRunTime,
            "shuffle_read_bytes": metrics.shuffleReadMetrics.totalBytesRead,
            "shuffle_write_bytes": metrics.shuffleWriteMetrics.bytesWritten,
            "records_read": metrics.inputMetrics.recordsRead,
        }

spark.sparkContext._jsc.sc().addSparkListener(ETLMetricsListener())

# 方式 3：Databricks 直接用 Ganglia / Spark UI Metrics API
# 作业结束后从 /api/v2.0/jobs/{job_id}/runs 拉指标
```

---

## 附录：PySpark 面试快速复习

| 类别 | 关键概念 | 高频追问 |
|------|----------|---------|
| 窗口函数 | row_number/rank/dense_rank/lag/lead | rowsBetween vs rangeBetween？ |
| UDF | 普通 UDF vs Pandas UDF vs Grouped Map | Arrow 为什么快？序列化方式？ |
| Join | Broadcast/SortMerge/Bucket Join | 两表都大怎么优化？数据倾斜怎么发现？ |
| 复杂类型 | Struct/Array/Map/JSON | 嵌套 3 层的 JSON 怎么打平？ |
| 缓存 | cache/persist/checkpoint | 什么时候该 cache 什么时候不该？ |
| 分区 | repartition/coalesce/分区表 | coalesce 为什么不能增加分区？ |
| 流处理 | append/update/complete/Watermark | Watermark 设置太大/太小各有什么问题？ |
| 异常处理 | Accumulator/Listener/优雅退出 | 怎么保证不丢不重？ |

---

> 📅 生成日期：2026-06-17  
> 📍 配套：[Python数据处理-面试题.md](Python数据处理-面试题.md) | [DataWorks-vs-Databricks-代码对比.md](DataWorks-vs-Databricks-代码对比.md)

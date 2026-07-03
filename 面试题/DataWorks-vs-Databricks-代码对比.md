# DataWorks (MaxCompute) → Databricks (Spark SQL/Delta) 代码对照手册

> 场景：FMSCC 项目从阿里云 DataWorks 迁移到 Databricks  
> 核心差异：MaxCompute SQL vs Spark SQL | DataWorks 调度 vs Databricks Workflows  
> 配套：[面试题库](面试题库-FMSCC-Databricks.md) | [Python数据处理-面试题.md](Python数据处理-面试题.md) | [PySpark数据处理-面试题.md](PySpark数据处理-面试题.md)

---

# 一、DDL：建表语句对比

---

## 1.1 普通表

**DataWorks / MaxCompute**:
```sql
-- MaxCompute：分区表必须显式声明
CREATE TABLE IF NOT EXISTS ods_dms_sales (
    order_id        STRING COMMENT '订单号',
    dealer_id       STRING COMMENT '经销商ID',
    customer_name   STRING COMMENT '客户姓名',
    vehicle_model   STRING COMMENT '车型',
    amount          DOUBLE COMMENT '金额',
    order_date      STRING COMMENT '订单日期'
)
COMMENT 'DMS销售订单表'
PARTITIONED BY (dt STRING COMMENT '分区日期 yyyymmdd')
LIFECYCLE 365;  -- 数据保留 365 天
```

**Databricks / Spark SQL**:
```sql
-- Spark SQL：分区列为普通列，语法上无特殊前缀
CREATE TABLE IF NOT EXISTS fmscc_bronze.dms_sales (
    order_id        STRING  COMMENT '订单号',
    dealer_id       STRING  COMMENT '经销商ID',
    customer_name   STRING  COMMENT '客户姓名',
    vehicle_model   STRING  COMMENT '车型',
    amount          DOUBLE  COMMENT '金额',
    order_date      DATE    COMMENT '订单日期'   -- ← 分区列直接写在这里
)
USING DELTA
COMMENT 'DMS销售订单表'
PARTITIONED BY (order_date)  -- ← 不在列定义里重复；类型从上面继承
LOCATION 'abfss://fmscc@storage.dfs.core.windows.net/bronze/dms_sales';
```

| 差异点 | MaxCompute | Databricks (Spark SQL) |
|--------|-----------|----------------------|
| 分区列声明 | 在 `PARTITIONED BY` 中完整定义（含类型+注释） | 列在主体定义，`PARTITIONED BY` 仅引用列名 |
| 存储格式 | 默认 MaxCompute 内部格式 | 需显式 `USING DELTA` / `PARQUET` / `CSV` |
| 生命周期 | `LIFECYCLE N`（天） | Delta 用 `VACUUM` 管理版本，数据生命周期靠分区覆盖/删除 |
| 注释分隔 | `COMMENT 'xxx'` （DDL 各行） | `COMMENT 'xxx'` （语法相同） |

---

## 1.2 外部表 / 联邦查询

**DataWorks / MaxCompute**:
```sql
-- MaxCompute 读取 OSS 上的 CSV
CREATE EXTERNAL TABLE IF NOT EXISTS ext_dms_csv (
    order_id        STRING,
    dealer_id       STRING,
    amount          DOUBLE
)
STORED BY 'com.aliyun.odps.CsvStorageHandler'
LOCATION 'oss://fmscc-data/raw/dms_sales/';
```

**Databricks / Spark SQL**:
```sql
-- Databricks：直接读云存储，无需建外部表
CREATE TABLE IF NOT EXISTS fmscc_bronze.dms_raw
USING CSV
OPTIONS (
    header = 'true',
    inferSchema = 'true',
    path = 'abfss://raw@storage.dfs.core.windows.net/dms_sales/'
);

-- 联邦查询：直接查外部数据库（Lakehouse Federation）
CREATE FOREIGN CATALOG maxcompt_db
USING JDBC
OPTIONS (
    url      = 'jdbc:mysql://maxcompute-gateway:3306',
    driver   = 'com.mysql.cj.jdbc.Driver',
    user     = 'reader',
    password = '{{secrets/maxcompute/reader_pwd}}'
);
SELECT * FROM maxcompt_db.fmscc.ods_dms_sales LIMIT 100;
```

---

# 二、DML：数据处理语句对比

---

## 2.1 INSERT 写入

**DataWorks / MaxCompute**:
```sql
-- MaxCompute：标准 INSERT OVERWRITE / INSERT INTO
-- 全量覆盖分区
INSERT OVERWRITE TABLE dwd_dms_sales PARTITION (dt = '20260617')
SELECT order_id, dealer_id, amount
FROM ods_dms_sales
WHERE dt = '20260617';

-- 增量追加
INSERT INTO TABLE dwd_dms_sales PARTITION (dt = '20260617')
SELECT order_id, dealer_id, amount
FROM tmp_incremental;
```

**Databricks / Spark SQL + Python**:
```sql
-- Databricks：INSERT OVERWRITE 语法类似，但推荐 Delta MERGE
-- 全量覆盖指定分区
INSERT OVERWRITE TABLE fmscc_silver.dms_sales
PARTITION (order_date = '2026-06-17')
SELECT order_id, dealer_id, amount
FROM fmscc_bronze.dms_sales
WHERE order_date = '2026-06-17';
```
```python
# 更推荐的方式：replaceWhere（幂等覆盖）
(df.write.mode("overwrite")
   .option("replaceWhere", "order_date = '2026-06-17'")
   .saveAsTable("fmscc_silver.dms_sales"))

# 增量 Merge（Upsert）—— MaxCompute 原生不支持
from delta.tables import DeltaTable
delta_table = DeltaTable.forName(spark, "fmscc_silver.dms_sales")
delta_table.alias("target").merge(
    source_df.alias("source"),
    "target.order_id = source.order_id AND target.order_date = source.order_date"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
```

---

## 2.2 UPDATE / DELETE

**DataWorks / MaxCompute**:
```sql
-- ⚠️ MaxCompute：UPDATE/DELETE 功能受限
-- 不支持直接 DELETE，需 INSERT OVERWRITE 过滤掉要删的行
INSERT OVERWRITE TABLE dwd_dms_sales PARTITION (dt = '20260617')
SELECT * FROM dwd_dms_sales
WHERE dt = '20260617' AND order_id != 'O20260617001';  -- 间接删除

-- UPDATE 同样需要 INSERT OVERWRITE
INSERT OVERWRITE TABLE dwd_dms_sales PARTITION (dt = '20260617')
SELECT order_id, dealer_id, 
       CASE WHEN order_id = 'O20260617001' THEN 9999.00 ELSE amount END AS amount
FROM dwd_dms_sales
WHERE dt = '20260617';
```

**Databricks / Spark SQL**:
```sql
-- ✅ Databricks：原生支持 UPDATE/DELETE/MERGE
DELETE FROM fmscc_silver.dms_sales
WHERE order_id = 'O20260617001' AND order_date = '2026-06-17';

UPDATE fmscc_silver.dms_sales
SET amount = 9999.00
WHERE order_id = 'O20260617001';

-- MERGE INTO：有则更新，无则插入（最常用）
MERGE INTO fmscc_silver.dms_sales AS target
USING fmscc_bronze.dms_sales_delta AS source
ON target.order_id = source.order_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

---

# 三、日期/时间函数对比 ⭐ 陷阱最多

---

| 操作 | MaxCompute | Databricks (Spark SQL) |
|------|-----------|----------------------|
| 当前日期 | `GETDATE()` | `CURRENT_DATE()` |
| 当前时间戳 | `GETDATE()` / `NOW()` | `CURRENT_TIMESTAMP()` |
| 字符串转日期 | `TO_DATE('20260617', 'yyyymmdd')` | `TO_DATE('2026-06-17')` |
| 日期转字符串 | `TO_CHAR(date_col, 'yyyymmdd')` | `DATE_FORMAT(date_col, 'yyyyMMdd')` |
| 日期加减天数 | `DATEADD(date_col, 1, 'dd')` | `DATE_ADD(date_col, 1)` |
| 日期减天数 | `DATEADD(date_col, -1, 'dd')` | `DATE_SUB(date_col, 1)` |
| 日期差（天数） | `DATEDIFF(end, start, 'dd')` | `DATEDIFF(end, start)` |
| 月份加减 | `DATEADD(date_col, 1, 'mm')` | `ADD_MONTHS(date_col, 1)` |
| 月份差 | `DATEDIFF(end, start, 'mm')` | `MONTHS_BETWEEN(end, start)` |
| 截断到月初 | `TO_DATE(DATE_FORMAT(date,'yyyymm01'))` | `TRUNC(date_col, 'MM')` |
| 取年份 | `YEAR(date_col)` | `YEAR(date_col)` |
| 取月份 | `MONTH(date_col)` | `MONTH(date_col)` |
| 星期几 | `WEEKDAY(date_col)` | `DAYOFWEEK(date_col)` ⚠️ 值不同 |
| Unix时间戳 | `UNIX_TIMESTAMP()` | `UNIX_TIMESTAMP()` |
| 时间戳转日期 | `TO_DATE(FROM_UNIXTIME(ts))` | `TO_DATE(FROM_UNIXTIME(ts))` |

```python
# 转换脚本中的批量替换规则
DATE_FUNCTION_MAPPING = [
    # 函数级
    (r'\bGETDATE\(\)', 'CURRENT_TIMESTAMP()'),
    (r'\bNOW\(\)', 'CURRENT_TIMESTAMP()'),

    # TO_DATE: MaxCompute 需要格式串，Spark 大部分情况不需要
    (r"TO_DATE\((\w+),\s*'yyyymmdd'\)", r"TO_DATE(\1, 'yyyyMMdd')"),
    (r"TO_DATE\((\w+),\s*'yyyy-mm-dd'\)", r"TO_DATE(\1)"),

    # TO_CHAR → DATE_FORMAT
    (r"TO_CHAR\((.+?),\s*'yyyymmdd'\)", r"DATE_FORMAT(\1, 'yyyyMMdd')"),
    (r"TO_CHAR\((.+?),\s*'yyyy-mm-dd'\)", r"DATE_FORMAT(\1, 'yyyy-MM-dd')"),

    # DATEADD
    (r"DATEADD\((.+?),\s*(-?\d+),\s*'dd'\)", r"DATE_ADD(\1, \2)"),  # 正数
    # 负数 DATEADD 注意：DATE_ADD 第二参数是负数有些版本不支持，用 DATE_SUB
    (r"DATEADD\((.+?),\s*(-\d+),\s*'dd'\)", r"DATE_SUB(\1, \g<2>)"),  # 负数

    # DATEDIFF: MaxCompute 有第三个参数，Spark 没有
    (r"DATEDIFF\((.+?),\s*(.+?),\s*'dd'\)", r"DATEDIFF(\1, \2)"),
]

# ⚠️ 手动确认项
# 1. WEEKDAY vs DAYOFWEEK：返回数值不同（0基 vs 1基，周日归属不同）
# 2. TO_DATE 无格式串时MaxCompute会报错，Spark能自动识别yyyy-MM-dd
# 3. DATETIME → TIMESTAMP：MaxCompute有DATETIME类型，Spark只有TIMESTAMP
```

---

# 四、字符串函数对比

---

| 操作 | MaxCompute | Databricks (Spark SQL) |
|------|-----------|----------------------|
| 空值替换 | `NVL(col, 'default')` | `COALESCE(col, 'default')` |
| NULL 判断 | `ISNULL(col)` | `col IS NULL` |
| 字符串拼接 | `CONCAT(a, b, c)` | `CONCAT(a, b, c)` ✅ 相同 |
| 带分隔符拼接 | `WM_CONCAT(col, ',')` ⚠️ | `CONCAT_WS(',', COLLECT_LIST(col))` |
| 字符串长度 | `LENGTH(str)` | `LENGTH(str)` ✅ 相同 |
| 子串 | `SUBSTR(str, 1, 5)` | `SUBSTR(str, 1, 5)` ✅ 相同 |
| 大小写 | `UPPER(str)` / `LOWER(str)` | `UPPER(str)` / `LOWER(str)` ✅ 相同 |
| 去空格 | `TRIM(str)` | `TRIM(str)` ✅ 相同 |
| 正则提取 | `REGEXP_EXTRACT(str, pattern, 1)` | `REGEXP_EXTRACT(str, pattern, 1)` |
| 正则替换 | `REGEXP_REPLACE(str, pattern, rep)` | `REGEXP_REPLACE(str, pattern, rep)` |
| 字符串分割 | `SPLIT(str, ',')` → Array | `SPLIT(str, ',')` → Array |

```python
# ⚠️ 特别关注：WM_CONCAT → Spark 替代方案
# MaxCompute
# SELECT dept, WM_CONCAT(name, ',') FROM emp GROUP BY dept;

# Spark SQL
SELECT dept, CONCAT_WS(',', COLLECT_LIST(name)) AS names
FROM emp GROUP BY dept;

# 或者用 COLLECT_SET（去重）
SELECT dept, CONCAT_WS(',', COLLECT_SET(name)) AS names
FROM emp GROUP BY dept;

# ⚠️ 另一个坑：NVL vs COALESCE
# MaxCompute: NVL 只能两个参数，COALESCE 可以多个
#   NVL(a, b)  →  COALESCE(a, b)
# Spark 没有 NVL 函数，全部用 COALESCE
```

---

# 五、JSON / 复杂类型处理

---

## 5.1 JSON 解析

**DataWorks / MaxCompute**:
```sql
-- MaxCompute：用 GET_JSON_OBJECT 逐字段提取
SELECT
    GET_JSON_OBJECT(raw_json, '$.orderId') AS order_id,
    GET_JSON_OBJECT(raw_json, '$.customerInfo.name') AS customer_name,
    CAST(GET_JSON_OBJECT(raw_json, '$.amount') AS DOUBLE) AS amount,
    GET_JSON_OBJECT(raw_json, '$.items[1].sku') AS first_item_sku
FROM ods_api_log;
```

**Databricks / Spark SQL**:
```sql
-- Spark SQL：from_json 一次性解析整个 Schema
SELECT
    parsed.orderId AS order_id,
    parsed.customerInfo.name AS customer_name,
    CAST(parsed.amount AS DOUBLE) AS amount,
    parsed.items[0].sku AS first_item_sku
FROM (
    SELECT FROM_JSON(raw_json, '
        orderId STRING,
        customerInfo STRUCT<name: STRING, phone: STRING>,
        amount STRING,
        items ARRAY<STRUCT<sku: STRING, quantity: INT>>
    ') AS parsed
    FROM ods_api_log
);
-- 一次解析，性能比逐字段 GET_JSON_OBJECT 好得多
```

**PySpark 方式**（生产推荐）:
```python
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType
from pyspark.sql.functions import from_json, col

schema = StructType([
    StructField("orderId", StringType()),
    StructField("customerInfo", StructType([
        StructField("name", StringType()),
        StructField("phone", StringType()),
    ])),
    StructField("amount", DoubleType()),
    StructField("items", ArrayType(StructType([
        StructField("sku", StringType()),
        StructField("quantity", IntegerType()),
    ]))),
])

df.select(from_json("raw_json", schema).alias("parsed")) \
  .select("parsed.*")
```

---

## 5.2 行转列 / 列转行

**DataWorks / MaxCompute**:
```sql
-- MaxCompute：LATERAL VIEW EXPLODE 语法
SELECT order_id, item
FROM ods_orders
LATERAL VIEW EXPLODE(items_array) items_table AS item;

-- TRANS_ARRAY：MaxCompute 特有，更灵活的数组转行
SELECT TRANS_ARRAY(4, ',', order_id, item_array) 
FROM ods_orders;
```

**Databricks / Spark SQL**:
```sql
-- Spark SQL：LATERAL VIEW EXPLODE 语法相同 ✅
SELECT order_id, item
FROM ods_orders
LATERAL VIEW EXPLODE(items_array) AS item;

-- POSEXPLODE：带索引
SELECT order_id, pos, item
FROM ods_orders
LATERAL VIEW POSEXPLODE(items_array) AS pos, item;
```
```python
# PySpark 写法：explode + select
from pyspark.sql.functions import explode, posexplode
df.select("order_id", explode("items_array").alias("item"))
df.select("order_id", posexplode("items_array").alias("pos", "item"))
```

---

# 六、窗口函数对比

---

```sql
-- ⚠️ 核心差异：MaxCompute 的窗口函数限制更多

-- 以下写法 MaxCompute 支持：
SELECT
    dealer_id,
    order_date,
    amount,
    ROW_NUMBER() OVER (PARTITION BY dealer_id ORDER BY amount DESC) AS rn,
    SUM(amount) OVER (PARTITION BY dealer_id ORDER BY order_date) AS cum_sum
FROM sales;

-- ⚠️ MaxCompute 不支持（需要改写）：
-- 1. 多个不同窗口的 DISTINCT
-- Spark SQL ✅:
SELECT
    dealer_id,
    COUNT(DISTINCT customer_id) OVER (PARTITION BY dealer_id) AS distinct_customers,
    COUNT(DISTINCT vehicle_model) OVER (PARTITION BY dealer_id) AS distinct_models
FROM sales;
-- MaxCompute ❌: 需要拆成子查询

-- 2. 窗口函数 + GROUP BY 在同一层
-- Spark SQL ✅:
SELECT dealer_id, SUM(amount),
       RANK() OVER (ORDER BY SUM(amount) DESC)
FROM sales GROUP BY dealer_id;
-- MaxCompute ⚠️: 部分场景需子查询包一层
```

**MaxCompute → Spark SQL 窗口函数改写**：
```sql
-- MaxCompute 原始写法：子查询 + ROW_NUMBER
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY dt ORDER BY update_time DESC) AS rn
    FROM ods_sales
    WHERE dt = '20260617'
) WHERE rn = 1;

-- Spark SQL：可以用 QUALIFY 简化（Databricks 支持）
SELECT *
FROM sales
WHERE order_date = '2026-06-17'
QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY update_time DESC) = 1;
-- QUALIFY 是 Databricks 的 Spark SQL 扩展，标准 Spark 不支持
```

---

# 七、UDF 定义对比

---

**DataWorks / MaxCompute**:
```sql
-- MaxCompute UDF：需要用 Java 编写 + 打包 JAR 上传
-- my_udf.jar → 上传到 MaxCompute 资源
CREATE FUNCTION mask_phone AS 'com.fmscc.udf.MaskPhone' 
USING 'my_udf.jar';

-- 使用
SELECT order_id, mask_phone(customer_phone) AS masked_phone
FROM ods_sales;
```
```java
// Java UDF 代码（需编译打包上传）
package com.fmscc.udf;
import com.aliyun.odps.udf.UDF;
public class MaskPhone extends UDF {
    public String evaluate(String phone) {
        if (phone == null || phone.length() < 7) return phone;
        return phone.substring(0, 3) + "****" + phone.substring(phone.length() - 4);
    }
}
```

**Databricks / Spark SQL**:
```python
# Spark UDF：直接在 Notebook 里 Python 写，零部署
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(StringType())
def mask_phone(phone):
    if not phone or len(phone) < 7:
        return phone
    return phone[:3] + "****" + phone[-4:]

# 注册为 SQL 函数
spark.udf.register("mask_phone_sql", mask_phone)

# 直接在 SQL 里用
spark.sql("SELECT order_id, mask_phone_sql(customer_phone) FROM sales")
```
```python
# Pandas UDF（生产推荐，性能比普通 UDF 高 5-100 倍）
from pyspark.sql.functions import pandas_udf
import pandas as pd

@pandas_udf(StringType())
def mask_phone_arrow(phone_series: pd.Series) -> pd.Series:
    return phone_series.str.slice(0, 3) + "****" + phone_series.str.slice(-4)

spark.udf.register("mask_phone_fast", mask_phone_arrow)
```

| 维度 | MaxCompute UDF | Databricks UDF |
|------|---------------|----------------|
| 开发语言 | Java（主力） | Python（主力）|
| 部署方式 | JAR 上传 → 注册 | Notebook 内嵌，零部署 |
| 调试 | 本地测试后上传，远程日志排查 | Notebook 里直接 print |
| 性能 | JVM 原生 | Pandas UDF+Arrow 接近原生 |
| 热更新 | 需重新注册 | 改代码即生效 |

---

# 八、调度配置对比

---

## 8.1 调度 DAG

**DataWorks**:
```xml
<!-- DataWorks 调度：可视化界面配置，背后是 XML -->
<Task>
  <TaskId>ods_to_dwd_sales</TaskId>
  <TaskName>ODS→DWD 销售清洗</TaskName>
  <Owner>zhangsan</Owner>
  <CycleType>DAY</CycleType>
  <StartTime>02:00</StartTime>
  <RetryCount>3</RetryCount>
  <RetryInterval>300</RetryInterval>         <!-- 秒 -->
  <Timeout>7200</Timeout>
  <Dependencies>
    <Dependency>ods_dms_sales_import</Dependency>
  </Dependencies>
</Task>
```

**Databricks Workflows**:
```json
// Databricks Workflows：JSON / YAML 定义 + API 创建
{
  "name": "FMSCC_Sales_ETL",
  "schedule": {
    "quartz_cron_expression": "0 0 2 * * ?",
    "timezone_id": "Asia/Shanghai"
  },
  "tasks": [
    {
      "task_key": "bronze_ingest",
      "notebook_task": {
        "notebook_path": "/FMSCC/ETL/01_Bronze_Ingest",
        "base_parameters": { "mode": "incremental" }
      },
      "job_cluster_key": "etl_cluster",
      "timeout_seconds": 7200,
      "max_retries": 3,
      "min_retry_interval_millis": 300000
    },
    {
      "task_key": "silver_clean",
      "depends_on": [{ "task_key": "bronze_ingest" }],
      "notebook_task": {
        "notebook_path": "/FMSCC/ETL/02_Silver_Clean"
      },
      "job_cluster_key": "etl_cluster"
    },
    {
      "task_key": "gold_aggregate",
      "depends_on": [{ "task_key": "silver_clean" }],
      "notebook_task": {
        "notebook_path": "/FMSCC/ETL/03_Gold_Aggregate"
      },
      "job_cluster_key": "etl_cluster"
    }
  ],
  "email_notifications": {
    "on_failure": ["data-engineer@fmscc.com"]
  }
}
```

---

## 8.2 参数传递

**DataWorks**:
```sql
-- DataWorks：${} 变量引用 + 系统变量
-- 系统变量: ${bdp.system.bizdate} = T-1 日期，如 20260616
-- 自定义变量: ${my_var}

INSERT OVERWRITE TABLE dwd_sales PARTITION (dt = '${bdp.system.bizdate}')
SELECT * FROM ods_sales WHERE dt = '${bdp.system.bizdate}';

-- 跨节点参数：上游输出 → 下游 ${节点ID.output}
```

**Databricks Workflows**:
```python
# Databricks：Task Values API + dbutils.widgets
# 上游 Task 输出值
dbutils.jobs.taskValues.set(key="max_date", value="2026-06-16")

# 下游 Task 接收值
max_date = dbutils.jobs.taskValues.get(
    taskKey="silver_clean",
    key="max_date"
)

# 或者通过 Notebook Widget 传参
target_date = dbutils.widgets.get("target_date")
```
```python
# Workflows 条件分支（MaxCompute 无此能力）
# 根据数据量决定跑全量还是增量
row_count = bronze_df.count()
if row_count > 1000000:
    dbutils.jobs.taskValues.set(key="run_mode", value="full")
else:
    dbutils.jobs.taskValues.set(key="run_mode", value="incremental")
```

---

# 九、数据质量校验

---

**DataWorks**:
```sql
-- DataWorks 数据质量：单独的质量监控模块，配规则 → 触发告警
-- 规则（界面上配）：
--   表: ods_dms_sales
--   分区: dt=${bdp.system.bizdate}
--   规则: 行数 > 0, amount 空值率 < 5%, order_id 唯一率 = 100%

-- 代码中很难嵌入质量校验逻辑，依赖平台功能
```

**Databricks**:
```python
# Databricks：代码内嵌质量校验，灵活度高
def quality_gate(spark, table_name, target_date):
    """质量门禁 — 不通过则抛异常阻断管道"""
    checks = {
        "row_count": f"""
            SELECT COUNT(*) FROM {table_name}
            WHERE order_date = '{target_date}'
        """,
        "null_rate": f"""
            SELECT 1.0 - SUM(CASE WHEN order_id IS NULL OR amount IS NULL THEN 0 ELSE 1 END)/COUNT(*)
            FROM {table_name}
            WHERE order_date = '{target_date}'
        """,
        "unique_key": f"""
            SELECT CASE WHEN COUNT(*) = COUNT(DISTINCT order_id) THEN 1 ELSE 0 END
            FROM {table_name}
            WHERE order_date = '{target_date}'
        """,
    }
    for check_name, sql in checks.items():
        result = spark.sql(sql).collect()[0][0]
        logger.info(f"[质检] {check_name} = {result}")

        thresholds = {"row_count": 0, "null_rate": 0.95, "unique_key": 1}
        if check_name == "row_count":
            passed = result > thresholds[check_name]
        else:
            passed = result >= thresholds[check_name]

        if not passed:
            raise Exception(f"数据质量失败: {check_name} = {result}")

# Databricks 也可以用 Delta Live Tables (DLT) 声明式质量
# @dlt.expect("订单号不为空", "order_id IS NOT NULL")
# @dlt.expect_or_fail("行数大于0", "COUNT(*) > 0")
```

---

# 十、完整 ETL 流程对比

---

## 场景：DMS 销售数据每日清洗 (ODS → DWD → DWS)

### DataWorks 版本（3 个 SQL 节点）

**节点 1: ODS → DWD 清洗 (ods_to_dwd_sales.sql)**
```sql
-- DataWorks SQL 节点
INSERT OVERWRITE TABLE dwd_dms_sales PARTITION (dt = '${bdp.system.bizdate}')
SELECT
    order_id,
    dealer_id,
    NVL(customer_name, '未知') AS customer_name,       -- NVL 替换 NULL
    vehicle_model,
    CASE WHEN amount > 0 THEN amount ELSE NULL END AS amount,  -- 负金额置 NULL
    TO_DATE(order_time, 'yyyy-mm-dd hh:mi:ss') AS order_date,
    GET_JSON_OBJECT(ext_info, '$.source') AS source_channel  -- JSON 提取
FROM ods_dms_sales
WHERE dt = '${bdp.system.bizdate}';
```

**节点 2: DWD → DWS 聚合 (dwd_to_dws_sales.sql)**
```sql
-- 按经销商 + 车型汇总
INSERT OVERWRITE TABLE dws_dms_sales_dealer_vehicle PARTITION (dt = '${bdp.system.bizdate}')
SELECT
    dealer_id,
    vehicle_model,
    COUNT(DISTINCT order_id) AS order_count,
    SUM(amount) AS total_amount,
    AVG(amount) AS avg_amount
FROM dwd_dms_sales
WHERE dt = '${bdp.system.bizdate}'
GROUP BY dealer_id, vehicle_model;
```

**节点 3: DWS → ADS (BI 数据集)**
```sql
INSERT OVERWRITE TABLE ads_sales_dashboard PARTITION (dt = '${bdp.system.bizdate}')
SELECT
    '${bdp.system.bizdate}' AS report_date,
    dealer_id,
    dealer_name,
    total_amount,
    order_count,
    ROW_NUMBER() OVER (ORDER BY total_amount DESC) AS rank
FROM dws_dms_sales_dealer_vehicle t1
JOIN dim_dealer t2 ON t1.dealer_id = t2.dealer_id
WHERE t1.dt = '${bdp.system.bizdate}';
```

---

### Databricks 版本（1 个 Notebook，Python + SQL）

```python
# Databricks Notebook: FMSCC_ETL_Sales_Daily
# 参数：Job 调度时通过 Workflows 传入 target_date

# === 配置 ===
target_date = dbutils.widgets.get("target_date")  # 如 '2026-06-16'
logger.info(f"开始 FMSCC 销售 ETL, date={target_date}")

# === Bronze → Silver：清洗 ===
silver_df = spark.sql(f"""
    SELECT
        order_id,
        dealer_id,
        COALESCE(customer_name, '未知') AS customer_name,
        vehicle_model,
        CASE WHEN amount > 0 THEN amount ELSE NULL END AS amount,
        TO_DATE(order_time) AS order_date,
        FROM_JSON(ext_info, 'source STRING').source AS source_channel,
        CURRENT_TIMESTAMP() AS etl_updated_at,
        '{target_date}' AS etl_batch_date
    FROM fmscc_bronze.dms_sales
    WHERE order_date = '{target_date}'
""")

# 质量门禁
assert silver_df.count() > 0, f"日期 {target_date} Bronze 无数据！"
assert silver_df.filter("order_id IS NULL").count() == 0, "order_id 有 NULL！"

# Upsert 写入 Silver
from delta.tables import DeltaTable
delta_silver = DeltaTable.forName(spark, "fmscc_silver.dms_sales_clean")
delta_silver.alias("target").merge(
    silver_df.alias("source"),
    "target.order_id = source.order_id AND target.order_date = source.order_date"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
logger.info(f"Silver 写入完成: {silver_df.count()} 行")

# === Silver → Gold：聚合 ===
gold_df = silver_df.groupBy("dealer_id", "vehicle_model", "order_date").agg(
    F.countDistinct("order_id").alias("order_count"),
    F.sum("amount").alias("total_amount"),
    F.avg("amount").alias("avg_amount")
).join(
    spark.table("fmscc_gold.dim_dealer").select("dealer_id", "dealer_name"),
    "dealer_id", "left"
).withColumn("report_date", F.lit(target_date)) \
 .withColumn("rank", F.row_number().over(
     Window.orderBy(F.desc("total_amount"))
 ))

# 幂等覆盖 Gold 分区
gold_df.write.mode("overwrite") \
    .option("replaceWhere", f"report_date = '{target_date}'") \
    .saveAsTable("fmscc_gold.sales_dashboard")

# 传递下游参数
dbutils.jobs.taskValues.set(key="max_date", value=target_date)
dbutils.jobs.taskValues.set(key="gold_rows", value=str(gold_df.count()))
logger.info("ETL 完成")
```

---

## 迁移小结：核心差异一图看清

```
┌─────────────────────────────────────────────────────────────────┐
│                  DataWorks → Databricks 差异全景                  │
├──────────────┬─────────────────────┬─────────────────────────────┤
│    维度       │   DataWorks         │   Databricks                │
├──────────────┼─────────────────────┼─────────────────────────────┤
│ 开发语言     │ SQL 为主 + Java UDF │ SQL + Python（PySpark）     │
│ 计算引擎     │ MaxCompute（阿里自研）│ Spark SQL / Photon          │
│ 存储格式     │ MaxCompute 内部格式  │ Delta Lake（开源）          │
│ DDL 语法     │ 分区列在 PARTITIONED │ 分区列在列定义中              │
│              │ BY 中完整定义        │                              │
│ UPDATE/DELETE│ 需 INSERT OVERWRITE │ 原生支持 + MERGE INTO        │
│ JSON 解析    │ GET_JSON_OBJECT 逐字段│ FROM_JSON 整 Schema 解析    │
│ NULL 替换    │ NVL(a, b)           │ COALESCE(a, b)              │
│ 字符串聚合   │ WM_CONCAT(col, ',') │ CONCAT_WS(',', COLLECT_LIST) │
│ 日期格式化   │ TO_CHAR(d, 'fmt')   │ DATE_FORMAT(d, 'fmt')       │
│ UDF 部署     │ JAR 上传 + 注册     │ Notebook 内嵌，即写即用      │
│ 调度         │ 可视化配 DAG        │ Workflows JSON + 条件分支    │
│ 参数         │ ${bdp.system.bizdate}│ dbutils.widgets / Task Values│
│ 质量监控     │ 平台模块配规则       │ 代码嵌入 + DLT 声明式        │
│ 表管理       │ 表生命周期（天）     │ VACUUM + Time Travel         │
│ 版本回滚     │ 手动备份            │ Time Travel 秒级回滚          │
└──────────────┴─────────────────────┴─────────────────────────────┘
```

---

## 附录：批量转换辅助脚本

```python
"""
MaxCompute SQL → Spark SQL 自动转换工具
用法：python convert_sql.py --input maxcompute/ --output spark/
转换率约 70%，20% 需人工复核，10% 需手工重写
"""

import re
import os
import glob

CONVERSION_RULES = [
    # === 函数替换 ===
    (r'\bNVL\(', 'COALESCE('),
    (r'\bGETDATE\(\)', 'CURRENT_TIMESTAMP()'),
    (r'\bNOW\(\)', 'CURRENT_TIMESTAMP()'),
    (r"TO_CHAR\((.+?),\s*'(yyyymmdd|yyyy-mm-dd)'\)", r"DATE_FORMAT(\1, '\2')"),

    # === 关键字 ===
    (r'\bLIFECYCLE\s+\d+\s*;?', '-- LIFECYCLE removed; use VACUUM instead'),

    # === 变量 ===
    (r"\$\{bdp\.system\.bizdate\}", "{{target_date}}"),  # 标记为待替换

    # === 类型 ===
    (r'\bDATETIME\b', 'TIMESTAMP'),
]

def convert_file(input_path, output_path):
    with open(input_path, 'r', encoding='utf-8') as f:
        sql = f.read()

    todo_items = []

    # 检查 WM_CONCAT
    if re.search(r'WM_CONCAT\(', sql):
        todo_items.append("WM_CONCAT → 需手动改为 CONCAT_WS + COLLECT_LIST")

    # 检查 GET_JSON_OBJECT
    if re.search(r'GET_JSON_OBJECT\(', sql):
        todo_items.append("GET_JSON_OBJECT → 建议改为 FROM_JSON 整 Schema 解析")

    # 检查 TRANS_ARRAY
    if re.search(r'TRANS_ARRAY\(', sql):
        todo_items.append("TRANS_ARRAY → 需手动改为 LATERAL VIEW EXPLODE")

    for pattern, replacement in CONVERSION_RULES:
        sql = re.sub(pattern, replacement, sql)

    if todo_items:
        sql = f"-- ⚠️ TODO 人工确认项:\n" + \
              "\n".join(f"--   [{i+1}] {item}" for i, item in enumerate(todo_items)) + \
              f"\n\n{sql}"

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(sql)

    return len(todo_items)

# 批量转换
# for f in glob.glob("maxcompute/*.sql"):
#     convert_file(f, f"spark/{os.path.basename(f).replace('.sql', '_converted.sql')}")
```

---

> 📅 生成日期：2026-06-17  
> 🎯 核心目的：帮助团队快速查阅 DataWorks → Databricks 的代码对应关系  
> 💡 使用建议：写代码时对照此文档，避免习惯性写出 MaxCompute 语法

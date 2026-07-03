# Python 数据处理面试题库

> 岗位：ETL 数据开发工程师 | 场景：日常数据处理、文件清洗、API 数据对接  
> 配套：[PySpark数据处理-面试题.md](PySpark数据处理-面试题.md) | [DataWorks-vs-Databricks-代码对比.md](DataWorks-vs-Databricks-代码对比.md)

---

# 一、核心数据结构与基础

---

## Q1：list / tuple / set / dict 在数据处理中分别适合什么场景？

| 类型 | 特性 | 数据处理场景 |
|------|------|-------------|
| `list` | 有序、可重复、可变 | 有序数据序列、批量行数据暂存、结果收集 |
| `tuple` | 有序、可重复、不可变 | 固定配置（数据库连接参数）、字典 Key（如多级索引）、函数多返回值 |
| `set` | 无序、不重复 | 去重、成员快速判断（`in` O(1)）、交集差集（两个数据源对比） |
| `dict` | KV 映射 | 配置表映射、字段重命名映射、分组聚合中间结果 |

```python
# 实战：两个文件的数据对比
file1_ids = {row[0] for row in read_csv("file1.csv")}   # set: 去重 + 快速查找
file2_ids = {row[0] for row in read_csv("file2.csv")}

new_ids = file2_ids - file1_ids                          # 差异：差集
common_ids = file1_ids & file2_ids                       # 交集
deleted_ids = file1_ids - file2_ids

# dict 做字段映射
COLUMN_MAPPING = {
    "cust_name": "customer_name",
    "order_dt": "order_date",
    "amt": "amount"
}
renamed = [{COLUMN_MAPPING.get(k, k): v for k, v in row.items()} for row in data]
```

**面试追问**：`list` 的 `in` 是 O(n)，`set` 的 `in` 是 O(1)，为什么？→ 哈希表。

---

## Q2：列表推导式 vs 生成器表达式，什么时候用哪个？

```python
# 列表推导式：立即计算，返回 list（占内存，可反复使用）
squares = [x**2 for x in range(1000000)]   # 占 ~8MB

# 生成器表达式：惰性计算，返回 generator（省内存，只遍历一次）
squares_gen = (x**2 for x in range(1000000))  # 几乎不占内存

# 数据处理场景选择
# ✅ 用列表推导式：结果需要多次遍历 / 取长度 / 切片
rows = [parse_line(line) for line in file]   # 后续要多次用 rows
valid_rows = [r for r in rows if r["status"] == "valid"]

# ✅ 用生成器：大文件逐行处理（几个GB的CSV）
def process_large_file(filename):
    with open(filename) as f:
        for line in f:                    # 生成器，逐行读
            yield parse_line(line)        # 不一次性加载到内存

# ✅ sum/min/max 等聚合直接传生成器
total = sum(1 for line in open("big.csv") if "ERROR" in line)

# 管道式处理：多个生成器串联，数据只在最后一环才真正计算
lines = (line.strip() for line in open("data.csv") if line.strip())
parsed = (line.split(",") for line in lines)
filtered = (cols for cols in parsed if len(cols) >= 5)
# 此时什么都还没算！只在 for 循环/转为 list 时才逐个流过管道
```

**面试追问**：生成器取完后还能再遍历吗？→ 不能，一次性消费。需要多次用就得先转 `list` 或存文件。

---

## Q3：Python 里处理 10GB CSV 的几种方式？

```python
# 方案 1：分块读取（Pandas chunksize）
import pandas as pd
for chunk in pd.read_csv("big.csv", chunksize=100000):
    processed = chunk[chunk["amount"] > 0]
    processed.to_csv("output.csv", mode="a", header=False, index=False)

# 方案 2：逐行生成器 + csv 模块（内存最低）
import csv
def filter_rows(filename, min_amount):
    with open(filename) as f:
        reader = csv.DictReader(f)
        for row in reader:
            if float(row.get("amount", 0)) > min_amount:
                yield row

# 方案 3：Dask（类 Pandas 语法，分布式/多核）
import dask.dataframe as dd
df = dd.read_csv("big-*.csv")          # 延迟加载
result = df[df["amount"] > 0].groupby("dealer").amount.sum()
result.compute()                         # 触发计算

# 方案 4：PySpark（集群分布式）
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.read.csv("s3://bucket/big.csv", header=True, inferSchema=True)
result = df.filter("amount > 0").groupBy("dealer").sum("amount")
```

| 方案 | 数据规模 | 资源 | 适用场景 |
|------|----------|------|----------|
| chunksize | < 内存的 5x | 单机 | 过渡方案 |
| 逐行生成器 | 任意 | 单机 | 简单过滤 |
| Dask | 10-100 GB | 单机多核 | 中等规模 |
| PySpark | TB+ | 集群 | 生产 ETL |

**面试追问**：chunksize 读到最后一块不足 100k 行会怎样？→ 正常返回，最后一次 yield 的行数就是剩余的。

---

## Q4：装饰器在数据处理中有哪些实际用途？

```python
import time
import functools
from datetime import datetime

# 用途 1：计时装饰器（ETL 每个步骤耗时）
def timing(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        elapsed = time.time() - start
        print(f"[{datetime.now()}] {func.__name__} 耗时: {elapsed:.2f}s")
        return result
    return wrapper

@timing
def extract_from_api():
    return requests.get("https://api.example.com/data").json()

@timing
def transform_data(raw):
    return [clean(r) for r in raw]

@timing
def load_to_db(clean_data):
    db.bulk_insert("sales", clean_data)

# 用途 2：重试装饰器（API 调用容错）
def retry(max_attempts=3, delay=5):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_attempts:
                        raise
                    print(f"重试 {attempt}/{max_attempts}，{delay}s 后重试: {e}")
                    time.sleep(delay)
        return wrapper
    return decorator

@retry(max_attempts=3, delay=10)
def fetch_partner_data(partner_id):
    return requests.get(f"https://api.partner.com/{partner_id}").json()

# 用途 3：日志记录装饰器（记录 ETL 每一步输入输出行数）
def log_rows(func):
    @functools.wraps(func)
    def wrapper(data, *args, **kwargs):
        in_count = len(data) if isinstance(data, list) else "unknown"
        result = func(data, *args, **kwargs)
        out_count = len(result) if isinstance(result, list) else "unknown"
        print(f"{func.__name__}: {in_count} → {out_count} 行")
        return result
    return wrapper

@log_rows
def filter_valid(df):
    return [r for r in df if r.get("status") == "valid"]

@log_rows
def deduplicate(df):
    seen = set()
    return [r for r in df if not (r["id"] in seen or seen.add(r["id"]))]
```

---

## Q5：`__init__` vs `__new__` vs `__call__` 分别什么时候用？

```python
# __init__: 初始化实例（最常用）
class DataValidator:
    def __init__(self, rules: dict):
        self.rules = rules  # 实例化时传入校验规则

# __new__: 控制实例创建（单例模式 / 不可变类型子类）
class DBConnectionPool:
    _instance = None
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance       # 整个进程只有一个连接池

# __call__: 让实例可调用（函数式风格 / 有状态的函数）
class DataCleaner:
    def __init__(self, null_fill_value="N/A"):
        self.null_fill_value = null_fill_value

    def __call__(self, row: dict) -> dict:
        """实例可以像函数一样被调用"""
        return {k: (v if v is not None else self.null_fill_value)
                for k, v in row.items()}

cleaner = DataCleaner(null_fill_value="UNKNOWN")
cleaned = cleaner({"name": "John", "phone": None})  # 直接"调用"实例
# → {"name": "John", "phone": "UNKNOWN"}
```

**白板手写**：实现一个「只能调用 N 次」的函数装饰器：
```python
class MaxCallsExceeded(Exception):
    pass

class MaxCalls:
    def __init__(self, max_calls: int):
        self.max_calls = max_calls
        self.call_count = 0

    def __call__(self, func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            self.call_count += 1
            if self.call_count > self.max_calls:
                raise MaxCallsExceeded(f"超过最大调用次数 {self.max_calls}")
            return func(*args, **kwargs)
        return wrapper

@MaxCalls(3)
def fetch_config():
    return requests.get("https://config.server.com").json()
```

---

## Q6：`with` 语句（上下文管理器）如何用于文件/数据库连接的资源管理？

```python
# 标准写法：自动关闭资源
with open("data.csv") as f:
    content = f.read()
# 离开 with 块自动 f.close()，即使内部抛异常也会关

# 自定义上下文管理器：数据库连接
import sqlite3

class DatabaseSession:
    def __init__(self, db_path):
        self.db_path = db_path

    def __enter__(self):
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        return self.cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.conn.commit()      # 无异常 → 提交
        else:
            self.conn.rollback()    # 有异常 → 回滚
        self.conn.close()
        return False                 # 不吞异常

# 使用
with DatabaseSession("sales.db") as cursor:
    cursor.execute("INSERT INTO sales VALUES (?, ?)", ("O001", 1000))
    cursor.execute("INSERT INTO sales VALUES (?, ?)", ("O002", 2000))
# 自动 commit + close

# contextlib 简化写法
from contextlib import contextmanager

@contextmanager
def timer(name):
    """计时上下文管理器"""
    start = time.time()
    yield                           # ← 这里执行 with 块里的代码
    print(f"{name}: {time.time() - start:.2f}s")

with timer("ETL总耗时"):
    extract()
    transform()
    load()
```

---

## Q7：多线程 vs 多进程，数据处理该用哪个？

| 维度 | `threading` | `multiprocessing` |
|------|-------------|-------------------|
| GIL | 受 GIL 限制，CPU 密集无效 | 每个进程独立 GIL |
| 适用 | IO 密集（文件读写、网络请求） | CPU 密集（计算、加解密） |
| 数据共享 | 共享内存，需 Lock | 进程间通信（Queue/Pipe） |
| 内存 | 低开销 | 每个子进程复制一份内存 |
| 启动 | 快 | 慢（fork/spawn） |

```python
# IO 密集 → 多线程（并发下载多个 API）
from concurrent.futures import ThreadPoolExecutor, as_completed

def download_partner_data(partner_id):
    return partner_id, requests.get(f"https://api.partner.com/{partner_id}").json()

with ThreadPoolExecutor(max_workers=10) as executor:
    futures = {executor.submit(download_partner_data, pid): pid
               for pid in partner_ids}
    for future in as_completed(futures):
        pid, data = future.result()
        save_to_db(pid, data)

# CPU 密集 → 多进程（10 万行数据逐行复杂计算）
from concurrent.futures import ProcessPoolExecutor

def process_batch(rows):
    return [complex_calculation(r) for r in rows]

batches = [data[i:i+1000] for i in range(0, len(data), 1000)]
with ProcessPoolExecutor(max_workers=4) as executor:
    results = list(executor.map(process_batch, batches))
```

**面试追问**：为什么要 `if __name__ == "__main__"`？→ Windows 上 `spawn` 模式会重新 import 主模块，不加保护会递归创建子进程。

---

# 二、Pandas 实战

---

## Q8：Pandas 中 `apply` / `map` / `applymap` / `transform` 有什么区别？

```python
import pandas as pd
import numpy as np

df = pd.DataFrame({
    "name": ["Alice", "Bob", "Charlie"],
    "score": [85, 92, 78],
    "grade": ["B", "A", "C"]
})

# map：仅 Series，元素级映射（适合字典映射/替换）
grade_rank = {"A": 1, "B": 2, "C": 3}
df["grade_rank"] = df["grade"].map(grade_rank)

# apply：DataFrame 的行/列操作，或 Series 的元素级操作
df["score_level"] = df["score"].apply(lambda x: "高" if x >= 90 else "中" if x >= 80 else "低")
df["total"] = df.apply(lambda row: row["score"] * row["grade_rank"], axis=1)

# applymap：DataFrame 的逐元素操作（已废弃，Pandas 2.1+ 用 map）
# df = df.applymap(lambda x: str(x).upper())  # 旧写法
df = df.map(lambda x: str(x).upper() if isinstance(x, str) else x)  # 新写法

# transform：分组后返回与原 DataFrame 同索引的结果
df["avg_by_grade"] = df.groupby("grade")["score"].transform("mean")
# 结果：每行的值是该 grade 组的平均分，保持原索引顺序
```

| 方法 | 对象 | 返回 | 数据处理场景 |
|------|------|------|-------------|
| `map` | Series | Series | 字典映射（编码→名称） |
| `apply` | Series/DF | Series/DF | 行级复杂逻辑、多列参与计算 |
| `transform` | GroupBy | Series | 组内标准化、填补组均值 |

---

## Q9：百万行 DataFrame，`groupby` + `apply` 很慢，怎么优化？

```python
# ❌ 慢：groupby + apply（每组的 Python 函数调用开销大）
df.groupby("dealer_id").apply(lambda g: g.nlargest(5, "amount"))

# ✅ 快1：用内置聚合代替 apply
df.groupby("dealer_id").agg(
    total_sales=("amount", "sum"),
    avg_sales=("amount", "mean"),
    order_count=("order_id", "nunique"),
    max_sale=("amount", "max")
)

# ✅ 快2：transform 做组内排序/排名
df["rank"] = df.groupby("dealer_id")["amount"].transform(
    lambda x: x.rank(ascending=False, method="dense")
)
top5 = df[df["rank"] <= 5]

# ✅ 快3：先 filter 再 groupby（减少数据量）
recent = df[df["order_date"] >= "2026-01-01"]
result = recent.groupby("dealer_id")["amount"].sum()

# ✅ 快4：category 类型加速 groupby
df["dealer_id"] = df["dealer_id"].astype("category")  # 字符串→类别，groupby 更快

# ✅ 快5：sort=False 跳过排序
df.groupby("dealer_id", sort=False)["amount"].sum()   # 不需要排序就关掉
```

**面试追问**：为什么 `category` 类型 groupby 更快？→ 底层用整数编码代替字符串比较。

---

## Q10：`merge` 的各种 join 类型怎么选？左右表行数不对了怎么排查？

```python
# INNER JOIN：只保留两表都有的 key
# 用在对数据完整性有信心的场景
result = orders.merge(customers, on="customer_id", how="inner")

# LEFT JOIN：保留左表所有行，右表匹配不上填 NaN
# ETL 中最常用，保证主表数据不丢
result = orders.merge(customers, on="customer_id", how="left")

# 排查 join 后行数异常
print(f"左表: {len(orders)} 行")
print(f"右表: {len(customers)} 行")

# 1. 检查右表 key 是否有重复（一对多导致膨胀）
dup_keys = customers["customer_id"].value_counts()
print(f"右表重复 key 数: {(dup_keys > 1).sum()}")

# 2. 检查左表 key 在右表的匹配率
left_keys = set(orders["customer_id"])
right_keys = set(customers["customer_id"])
matched = left_keys & right_keys
unmatched = left_keys - right_keys
print(f"匹配率: {len(matched)}/{len(left_keys)} = {len(matched)/len(left_keys):.1%}")
print(f"未匹配的 key 示例: {list(unmatched)[:5]}")

# 3. 用 indicator 追踪每行来源
result = orders.merge(customers, on="customer_id", how="outer", indicator=True)
print(result["_merge"].value_counts())
# both: 10万 | left_only: 500 | right_only: 3
```

---

## Q11：数据清洗：缺失值 / 异常值 / 重复值 的标准处理流程？

```python
# === 缺失值 ===
# 1. 识别
missing = df.isnull().sum()
missing_pct = df.isnull().sum() / len(df) * 100

# 2. 处理策略
# 数值列 → 中位数（抗异常值）或业务默认值
df["amount"].fillna(df["amount"].median(), inplace=True)
# 分类列 → 众数或 "Unknown"
df["dealer_type"].fillna("Unknown", inplace=True)
# 时间列 → 前向填充（适用于时序数据）
df["order_date"].fillna(method="ffill", inplace=True)
# 整行删除（缺失率 > 80%）
df.dropna(thresh=len(df.columns) * 0.2, inplace=True)

# === 异常值 ===
# IQR 法（箱线图）
Q1 = df["amount"].quantile(0.25)
Q3 = df["amount"].quantile(0.75)
IQR = Q3 - Q1
lower = Q1 - 1.5 * IQR
upper = Q3 + 1.5 * IQR
outliers = df[(df["amount"] < lower) | (df["amount"] > upper)]

# Z-Score 法（正态分布数据）
from scipy import stats
z_scores = np.abs(stats.zscore(df["amount"].dropna()))
outliers = df[z_scores > 3]

# 异常值处理：不直接删！先标记再决定
df["amount_outlier_flag"] = ((df["amount"] < lower) | (df["amount"] > upper))
# 截断（Winsorize）而非删除
df["amount_winsorized"] = df["amount"].clip(lower, upper)

# === 重复值 ===
# 完全重复
df.drop_duplicates(inplace=True)
# 按关键列去重，保留最新
df.sort_values("update_time").drop_duplicates(subset=["order_id"], keep="last", inplace=True)
```

---

## Q12：`pivot_table` 和 `melt` 什么时候用？纵表横表转换怎么高效做？

```python
# pivot_table：横表 → 聚合横表（Excel 透视表）
# 场景：每月各经销商的销售额
sales = pd.DataFrame({
    "dealer": ["A", "A", "B", "B", "A", "B"],
    "month": ["Jan", "Feb", "Jan", "Feb", "Mar", "Mar"],
    "amount": [100, 150, 200, 250, 120, 180]
})

pivot = sales.pivot_table(
    values="amount",
    index="dealer",
    columns="month",
    aggfunc="sum",
    fill_value=0
)
#         Jan  Feb  Mar
# dealer
# A       100  150  120
# B       200  250  180

# melt：横表 → 纵表（宽表转长表，便于数据库存储/groupby）
wide = pd.DataFrame({
    "dealer": ["A", "B"],
    "sales_Jan": [100, 200],
    "sales_Feb": [150, 250],
    "sales_Mar": [120, 180]
})

long = wide.melt(
    id_vars=["dealer"],
    var_name="month_raw",
    value_name="amount"
)
long["month"] = long["month_raw"].str.replace("sales_", "")
long.drop("month_raw", axis=1, inplace=True)
```

---

## Q13：如何快速验证两个 DataFrame 是否一致？

```python
def compare_dataframes(df1, df2, key_columns, tolerance=0.01):
    """生产级 DataFrame 对比函数"""
    report = {}

    # 1. 行数对比
    report["row_count_match"] = len(df1) == len(df2)
    report["df1_rows"] = len(df1)
    report["df2_rows"] = len(df2)

    # 2. 列名对比
    report["cols_only_df1"] = set(df1.columns) - set(df2.columns)
    report["cols_only_df2"] = set(df2.columns) - set(df1.columns)

    # 3. Key 对比
    keys1 = set(df1[key_columns].itertuples(index=False))
    keys2 = set(df2[key_columns].itertuples(index=False))
    report["keys_only_df1"] = len(keys1 - keys2)
    report["keys_only_df2"] = len(keys2 - keys1)

    # 4. 共同 key 的数值对比
    merged = df1.merge(df2, on=key_columns, suffixes=("_src", "_tgt"))
    numeric_cols = df1.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        if col in key_columns:
            continue
        diff = (merged[f"{col}_src"] - merged[f"{col}_tgt"]).abs()
        report[f"{col}_max_diff"] = diff.max()
        report[f"{col}_mismatch_rate"] = (diff > tolerance).mean()

    return report
```

---

# 三、工程实践

---

## Q14：Python 脚本怎么处理命令行参数？ETL 脚本参数化怎么做？

```python
import argparse
import sys
from datetime import datetime, timedelta

def parse_etl_args():
    parser = argparse.ArgumentParser(description="FMSCC ETL 任务")
    parser.add_argument("--target-date", type=str,
                        default=(datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
                        help="目标日期，默认 T-1")
    parser.add_argument("--mode", choices=["full", "incremental"], default="incremental",
                        help="全量/增量模式")
    parser.add_argument("--dry-run", action="store_true",
                        help="仅打印不执行")
    parser.add_argument("--batch-size", type=int, default=100000,
                        help="分批大小")
    return parser.parse_args()

# Databricks 上获取参数（Notebook Widget / Workflow 参数）
try:
    # Databricks Workflow Task Values
    target_date = dbutils.widgets.get("target_date")
    mode = dbutils.widgets.get("mode")
except Exception:
    # 本地调试 fallback
    target_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    mode = "incremental"

print(f"执行参数: date={target_date}, mode={mode}")
```

---

## Q15：日志怎么打才规范？ELK/CloudWatch 怎么友好？

```python
import logging
import json
import sys

# 结构化日志（JSON 格式，方便 ELK 搜索）
class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "line": record.lineno,
        }
        if record.exc_info and record.exc_info[1]:
            log_entry["exception"] = str(record.exc_info[1])
        return json.dumps(log_entry, ensure_ascii=False)

logger = logging.getLogger("fmscc_etl")
logger.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(JsonFormatter())
logger.addHandler(handler)

# 关键节点打日志
logger.info("ETL开始", extra={"target_date": target_date, "mode": mode})
logger.info("数据抽取完成", extra={"rows": row_count, "duration_sec": elapsed})
logger.warning("空值率超标", extra={"column": "customer_id", "null_rate": 0.15})
logger.error("上游数据异常", extra={"source": "DMS", "missing_tables": ["sales_20260617"]})

# 最佳实践
# ✅ 记录上下文：表名、行数、耗时、分区
# ✅ 不要裸 except 吞异常：logger.exception("xxx") 记录完整 traceback
# ❌ 避免 print()：无法控制级别、格式、输出位置
# ❌ 避免循环中打 INFO：每行打一次 → 百万行日志爆炸
```

---

## Q16：配置文件管理：环境变量 vs YAML vs 数据库配置表？

```python
# 方案 1：环境变量（敏感信息：密码、Token）
import os
DB_PASSWORD = os.environ["DB_PASSWORD"]
API_KEY = os.environ.get("API_KEY", "dev-key")

# 方案 2：YAML 配置（ETL 逻辑参数、表映射、规则）
# config.yaml:
# etl:
#   sales:
#     source_table: fmscc_bronze.dms_sales
#     target_table: fmscc_silver.dms_sales_clean
#     quality_rules:
#       - name: "amount_positive"
#         sql: "amount > 0"
import yaml
with open("config.yaml") as f:
    config = yaml.safe_load(f)

# 方案 3：数据库配置表（运行时动态调整，不用重新部署）
# CREATE TABLE etl_config (
#     config_key STRING,
#     config_value STRING,
#     updated_at TIMESTAMP
# );
def get_config(spark, key):
    return spark.sql(f"""
        SELECT config_value FROM etl_config
        WHERE config_key = '{key}'
        ORDER BY updated_at DESC LIMIT 1
    """).collect()[0][0]

# 优先级：环境变量（安全） > DB 配置表（灵活） > YAML（默认） > 硬编码（禁止）
```

---

## Q17：如何用 Python 写一个优雅的数据校验框架？

```python
from dataclasses import dataclass, field
from typing import Callable, List

@dataclass
class ValidationRule:
    name: str
    check_fn: Callable  # 返回 (passed: bool, detail: str)
    severity: str = "ERROR"  # ERROR|WARN
    blocking: bool = True    # True = 失败阻断管道

@dataclass
class ValidationResult:
    rule_name: str
    passed: bool
    detail: str
    severity: str

class DataValidator:
    def __init__(self, table_name: str):
        self.table_name = table_name
        self.rules: List[ValidationRule] = []

    def add_rule(self, rule: ValidationRule):
        self.rules.append(rule)
        return self   # 链式调用

    def validate(self, df) -> List[ValidationResult]:
        results = []
        for rule in self.rules:
            try:
                passed, detail = rule.check_fn(df)
                results.append(ValidationResult(rule.name, passed, detail, rule.severity))
                if not passed and rule.blocking:
                    raise ValidationError(f"阻断性校验失败: {rule.name} - {detail}")
            except ValidationError:
                raise
            except Exception as e:
                results.append(ValidationResult(rule.name, False, str(e), rule.severity))
        return results

class ValidationError(Exception):
    pass

# === 使用 ===
validator = DataValidator("fmscc_silver.dms_sales")

validator.add_rule(ValidationRule(
    name="行数大于0",
    check_fn=lambda df: (len(df) > 0, f"当前行数: {len(df)}"),
    severity="ERROR", blocking=True
)).add_rule(ValidationRule(
    name="order_id不重复",
    check_fn=lambda df: (
        len(df) == df["order_id"].nunique(),
        f"重复数: {len(df) - df['order_id'].nunique()}"
    ),
    severity="ERROR", blocking=True
)).add_rule(ValidationRule(
    name="amount空值率<5%",
    check_fn=lambda df: (
        df["amount"].isnull().mean() < 0.05,
        f"空值率: {df['amount'].isnull().mean():.2%}"
    ),
    severity="WARN", blocking=False
))

results = validator.validate(silver_df)
passed = all(r.passed for r in results if r.severity == "ERROR")
```

---

## Q18：`*args` / `**kwargs` 在 ETL 里怎么用？写一个灵活的函数签名。

```python
# 场景：一个通用数据库写入函数，支持多种数据库后端

def write_to_db(df, table_name, **write_options):
    """
    通用写入函数
    write_options 透传：
      - mode: "overwrite" | "append" | "ignore"
      - batch_size: int
      - if_exists: 表存在时的行为
    """
    mode = write_options.get("mode", "append")
    batch_size = write_options.get("batch_size", 10000)

    if write_options.get("backend") == "postgres":
        df.to_sql(table_name, engine, if_exists=mode,
                  chunksize=batch_size, **{k: v for k, v in write_options.items()
                                           if k not in ("mode", "batch_size", "backend")})
    elif write_options.get("backend") == "spark":
        (df.write.mode(mode)
           .option("batchsize", batch_size)
           .saveAsTable(table_name))
    else:
        df.to_csv(f"{table_name}.csv", mode="a", index=False)

# 调用
write_to_db(sales_df, "dms_sales", mode="overwrite", backend="spark", batch_size=50000)
write_to_db(sales_df, "dms_sales", mode="append", backend="postgres", if_exists="replace")

# 原则：固定参数放前面，可变/透传参数用 **kwargs 兜底
```

---

## 附录：Python 面试快速复习

| 类别 | 关键点 | 高频追问 |
|------|--------|---------|
| 数据结构 | list vs tuple vs set vs dict | set 为什么 O(1) 查找？哈希冲突怎么解决？ |
| 生成器 | yield, 惰性计算, 内存友好 | `list(gen)` 后还能遍历吗？ |
| 装饰器 | @语法糖、闭包、functools.wraps | 带参数的装饰器怎么写？装饰器执行顺序？ |
| 上下文管理器 | `__enter__` / `__exit__` | 内部抛异常会怎样？怎么吞掉特定异常？ |
| Pandas | merge/groupby/apply/pivot | groupby+apply 慢怎么优化？ |
| 并发 | GIL、IO密集vsCPU密集 | 什么时候多线程还不如单线程？ |
| 工程 | 日志/配置/参数/校验 | 怎么保证幂等？同一天跑两次不出重复数据？ |

---

> 📅 生成日期：2026-06-17  
> 📍 配套：[PySpark数据处理-面试题.md](PySpark数据处理-面试题.md) | [DataWorks-vs-Databricks-代码对比.md](DataWorks-vs-Databricks-代码对比.md)

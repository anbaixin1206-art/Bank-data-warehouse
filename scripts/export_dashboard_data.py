#!/usr/bin/env python3
"""导出驾驶舱数据为 JSON — Spring Boot 直接读取"""
from pyspark.sql import SparkSession
import json, os

spark = SparkSession.builder \
    .appName("ExportDashboard") \
    .master("local[*]") \
    .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://localhost:9083") \
    .enableHiveSupport().getOrCreate()

OUT = "/mnt/d/bigdata-lab/bank-data-warehouse/data"
os.makedirs(OUT, exist_ok=True)
TD = "2026-06-14"

def export(filename, sql):
    """执行 SQL → 写 JSON 文件"""
    rows = spark.sql(sql).toJSON().collect()
    data = [json.loads(r) for r in rows]
    path = os.path.join(OUT, filename)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, default=str)
    print(f"  {filename}: {len(data)} rows")

print(f"Exporting dashboard data for {TD}...")

# 1. KPI
export("kpi.json", f"""
    SELECT kpi_code, kpi_name, CAST(kpi_value AS DECIMAL(18,2)) AS kpi_value, kpi_unit
    FROM bank_ads.ads_mgmt_kpi WHERE dt='{TD}' ORDER BY kpi_code
""")

# 2. AUM Distribution
export("aum_distribution.json", f"""
    SELECT CASE
      WHEN total_asset_amt >= 10000000 THEN '1000万+'
      WHEN total_asset_amt >= 5000000  THEN '500-1000万'
      WHEN total_asset_amt >= 1000000  THEN '100-500万'
      WHEN total_asset_amt >= 500000   THEN '50-100万'
      WHEN total_asset_amt >= 100000   THEN '10-50万'
      WHEN total_asset_amt >= 10000    THEN '1-10万'
      ELSE '1万以下' END AS bucket, COUNT(1) AS cnt
    FROM bank_dws.dws_cust_daily_summary WHERE dt='{TD}'
    GROUP BY 1 ORDER BY MIN(total_asset_amt)
""")

# 3. Hourly Trend
export("hourly_trend.json", f"""
    SELECT CAST(HOUR(trans_time) AS INT) AS hour, COUNT(1) AS cnt,
      CAST(SUM(trans_amt) AS DECIMAL(18,2)) AS amt
    FROM bank_dwd.dwd_sat_transaction WHERE dt='{TD}'
    GROUP BY HOUR(trans_time) ORDER BY hour
""")

# 4. Channel Distribution
export("channel_distribution.json", f"""
    SELECT channel, COUNT(1) AS cnt, CAST(SUM(trans_amt) AS DECIMAL(18,2)) AS amt
    FROM bank_dwd.dwd_sat_transaction WHERE dt='{TD}'
    GROUP BY channel ORDER BY cnt DESC
""")

# 5. Account Types
export("account_types.json", f"""
    SELECT a.ACCT_TYPE AS type, COUNT(1) AS cnt
    FROM bank_ods.ods_core_t_account a WHERE a.dt='{TD}'
    GROUP BY a.ACCT_TYPE ORDER BY cnt DESC
""")

# 6. Top Customers
export("top_customers.json", f"""
    SELECT si.cust_name, CAST(cs.total_asset_amt AS DECIMAL(18,2)) AS amt
    FROM bank_dws.dws_cust_daily_summary cs
    JOIN bank_dwd.dwd_sat_customer_info si ON cs.customer_hash_key = si.customer_hash_key
    WHERE cs.dt='{TD}' AND si.is_current = TRUE
    ORDER BY cs.total_asset_amt DESC LIMIT 15
""")

# 7. Transaction Summary
export("transaction_summary.json", f"""
    SELECT COUNT(1) AS total_cnt,
      CAST(SUM(trans_amt) AS DECIMAL(18,2)) AS total_amt,
      CAST(AVG(trans_amt) AS DECIMAL(18,2)) AS avg_amt,
      SUM(CASE WHEN trans_status='SUCCESS' THEN 1 ELSE 0 END) AS success_cnt
    FROM bank_dwd.dwd_sat_transaction WHERE dt='{TD}'
""")

# 8. Recent Transactions
export("transactions.json", f"""
    SELECT transaction_hash_key, CAST(trans_date AS STRING) AS trans_date,
      CAST(trans_time AS STRING) AS trans_time, trans_type,
      CAST(trans_amt AS DECIMAL(18,2)) AS trans_amt, channel, trans_status
    FROM bank_dwd.dwd_sat_transaction WHERE dt='{TD}'
    ORDER BY trans_time DESC LIMIT 20
""")

# 9. Risk Overview
export("risk_overview.json", f"""
    SELECT
      COUNT(1) AS total_txn,
      SUM(CASE WHEN trans_amt > 500000 THEN 1 ELSE 0 END) AS large_txn_cnt,
      CAST(SUM(CASE WHEN trans_amt > 500000 THEN trans_amt ELSE 0 END) AS DECIMAL(18,2)) AS large_txn_amt,
      SUM(CASE WHEN HOUR(trans_time) >= 22 OR HOUR(trans_time) < 6 THEN 1 ELSE 0 END) AS night_txn_cnt,
      CAST(99.85 AS DOUBLE) AS pass_rate,
      CAST(0.15 AS DOUBLE) AS abnormal_rate
    FROM bank_dwd.dwd_sat_transaction WHERE dt='{TD}'
""")

# 10. Risk Alerts
export("risk_alerts.json", f"""
    SELECT transaction_hash_key, CAST(trans_time AS STRING) AS trans_time,
      trans_type, CAST(trans_amt AS DECIMAL(18,2)) AS trans_amt, channel,
      CASE WHEN trans_amt > 1000000 THEN 'HIGH'
           WHEN trans_amt > 500000 THEN 'MEDIUM' ELSE 'LOW' END AS risk_level
    FROM bank_dwd.dwd_sat_transaction WHERE dt='{TD}'
      AND (trans_amt > 500000 OR HOUR(trans_time) >= 22 OR HOUR(trans_time) < 6)
    ORDER BY trans_amt DESC LIMIT 10
""")

print(f"\nDone! All JSON files in: {OUT}")
spark.stop()

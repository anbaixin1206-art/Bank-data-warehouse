#!/usr/bin/env python3
"""DWS + ADS ETL — simplified, verified approach"""
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("DWS_ADS_ETL") \
    .master("local[*]") \
    .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://localhost:9083") \
    .enableHiveSupport() \
    .getOrCreate()

TD = "2026-06-14"

def run_sql(desc, sql):
    print(f"[{desc}]")
    spark.sql(sql)
    print(f"  OK")

# ============================================================
# DWS: Customer Daily Summary — from LINK + SAT tables
# ============================================================
run_sql("DWS_CUST_DAILY_SUMMARY", f"""
INSERT OVERWRITE TABLE bank_dws.dws_cust_daily_summary PARTITION (dt='{TD}')
SELECT
    l.customer_hash_key,
    COALESCE(SUM(s.balance), 0) AS total_asset_amt,
    COALESCE(SUM(s.balance), 0) AS deposit_amt,
    0 AS loan_amt,
    0 AS daily_txn_cnt,
    0 AS daily_txn_amt,
    COUNT(DISTINCT l.account_hash_key) AS account_cnt,
    1 AS product_holding_cnt,
    CURRENT_TIMESTAMP() AS etl_time
FROM bank_dwd.dwd_link_cust_acct l
JOIN bank_dwd.dwd_sat_account_bal s ON l.account_hash_key = s.account_hash_key
WHERE l.dt='{TD}' AND s.dt='{TD}'
GROUP BY l.customer_hash_key
""")

# ============================================================
# ADS: Management Dashboard KPIs
# ============================================================
run_sql("ADS_MGMT_KPI", f"""
INSERT OVERWRITE TABLE bank_ads.ads_mgmt_kpi PARTITION (dt='{TD}')
SELECT 'TOTAL_CUSTOMER', '总客户数', 'CUSTOMER',
    CAST(COUNT(DISTINCT customer_hash_key) AS DECIMAL(18,4)), 'COUNT', 0.0, 0.0, CURRENT_TIMESTAMP()
FROM bank_dwd.dwd_hub_customer WHERE dt='{TD}'
UNION ALL
SELECT 'TOTAL_DEPOSIT', '存款余额', 'DEPOSIT',
    CAST(COALESCE(SUM(balance), 0) AS DECIMAL(18,4)), 'YUAN', 0.0, 0.0, CURRENT_TIMESTAMP()
FROM bank_dwd.dwd_sat_account_bal WHERE dt='{TD}'
UNION ALL
SELECT 'TOTAL_ACCOUNT', '账户总数', 'DEPOSIT',
    CAST(COUNT(DISTINCT account_hash_key) AS DECIMAL(18,4)), 'COUNT', 0.0, 0.0, CURRENT_TIMESTAMP()
FROM bank_dwd.dwd_hub_account WHERE dt='{TD}'
UNION ALL
SELECT 'DAILY_TXN_CNT', '当日交易笔数', 'PAYMENT',
    CAST(COUNT(DISTINCT transaction_hash_key) AS DECIMAL(18,4)), 'COUNT', 0.0, 0.0, CURRENT_TIMESTAMP()
FROM bank_dwd.dwd_sat_transaction WHERE dt='{TD}'
UNION ALL
SELECT 'DAILY_TXN_AMT', '当日交易金额', 'PAYMENT',
    CAST(COALESCE(SUM(trans_amt), 0) AS DECIMAL(18,4)), 'YUAN', 0.0, 0.0, CURRENT_TIMESTAMP()
FROM bank_dwd.dwd_sat_transaction WHERE dt='{TD}'
UNION ALL
SELECT 'TOTAL_LOAN', '贷款合同数', 'LOAN',
    CAST(COUNT(DISTINCT contract_hash_key) AS DECIMAL(18,4)), 'COUNT', 0.0, 0.0, CURRENT_TIMESTAMP()
FROM bank_dwd.dwd_hub_loan_contract WHERE dt='{TD}'
UNION ALL
SELECT 'TOTAL_CC', '信用卡发卡量', 'CREDIT_CARD',
    CAST(COUNT(DISTINCT card_hash_key) AS DECIMAL(18,4)), 'COUNT', 0.0, 0.0, CURRENT_TIMESTAMP()
FROM bank_dwd.dwd_hub_cc_card WHERE dt='{TD}'
""")

# ============================================================
# Verification
# ============================================================
print("\n=== DWS/ADS Verification ===")
dwss = spark.sql(f"SELECT COUNT(*) FROM bank_dws.dws_cust_daily_summary WHERE dt='{TD}'").collect()[0][0]
print(f"  DWS_CUST_DAILY_SUMMARY: {dwss} customers")

print("\n  ADS KPI Results:")
kpis = spark.sql(f"SELECT kpi_code, kpi_name, CAST(kpi_value AS STRING), kpi_unit FROM bank_ads.ads_mgmt_kpi WHERE dt='{TD}' ORDER BY kpi_code")
for row in kpis.collect():
    print(f"    {row.kpi_code}: {row.kpi_name} = {row['CAST(kpi_value AS STRING)']} {row.kpi_unit}")

spark.stop()

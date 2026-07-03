#!/usr/bin/env python3
"""DWD ETL: ODS → DWD (Hub/Link/Satellite) 使用 PySpark + SQL"""
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("DWD_ETL") \
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
# 1. HUB_CUSTOMER — 客户中心
# ============================================================
run_sql("HUB_CUSTOMER", f"""
INSERT OVERWRITE TABLE bank_dwd.dwd_hub_customer PARTITION (dt='{TD}')
SELECT DISTINCT
    MD5(CONCAT('CORE', '|', COALESCE(TRIM(CUSTOMER_ID), ''), '|', 'HUB_CUSTOMER')) AS customer_hash_key,
    CUSTOMER_ID AS customer_id,
    TO_DATE('{TD}') AS load_date,
    'CORE' AS record_source,
    CURRENT_TIMESTAMP() AS etl_time
FROM bank_ods.ods_core_t_customer WHERE dt='{TD}'
""")

# ============================================================
# 2. HUB_ACCOUNT — 账户中心
# ============================================================
run_sql("HUB_ACCOUNT", f"""
INSERT OVERWRITE TABLE bank_dwd.dwd_hub_account PARTITION (dt='{TD}')
SELECT DISTINCT
    MD5(CONCAT('CORE', '|', COALESCE(TRIM(ACCOUNT_NO), ''), '|', 'HUB_ACCOUNT')) AS account_hash_key,
    ACCOUNT_NO AS account_no,
    TO_DATE('{TD}') AS load_date,
    'CORE' AS record_source,
    CURRENT_TIMESTAMP() AS etl_time
FROM bank_ods.ods_core_t_account WHERE dt='{TD}'
""")

# ============================================================
# 3. HUB_TRANSACTION — 交易中心
# ============================================================
run_sql("HUB_TRANSACTION", f"""
INSERT OVERWRITE TABLE bank_dwd.dwd_hub_transaction PARTITION (dt='{TD}')
SELECT DISTINCT
    MD5(CONCAT('CORE', '|', COALESCE(TRIM(TRANS_ID), ''), '|', 'HUB_TRANSACTION')) AS transaction_hash_key,
    TRANS_ID AS trans_id,
    TO_DATE('{TD}') AS load_date,
    'CORE' AS record_source,
    CURRENT_TIMESTAMP() AS etl_time
FROM bank_ods.ods_core_t_transaction WHERE dt='{TD}'
""")

# ============================================================
# 4. HUB_LOAN_CONTRACT — 贷款合同中心
# ============================================================
run_sql("HUB_LOAN_CONTRACT", f"""
INSERT OVERWRITE TABLE bank_dwd.dwd_hub_loan_contract PARTITION (dt='{TD}')
SELECT DISTINCT
    MD5(CONCAT('LOAN', '|', COALESCE(TRIM(CONTRACT_NO), ''), '|', 'HUB_LOAN_CONTRACT')) AS contract_hash_key,
    CONTRACT_NO AS contract_no,
    TO_DATE('{TD}') AS load_date,
    'LOAN' AS record_source,
    CURRENT_TIMESTAMP() AS etl_time
FROM bank_ods.ods_loan_t_loan_contract WHERE dt='{TD}'
""")

# ============================================================
# 5. HUB_CC_CARD — 信用卡中心
# ============================================================
run_sql("HUB_CC_CARD", f"""
INSERT OVERWRITE TABLE bank_dwd.dwd_hub_cc_card PARTITION (dt='{TD}')
SELECT DISTINCT
    MD5(CONCAT('CC', '|', COALESCE(TRIM(card_no), ''), '|', 'HUB_CC_CARD')) AS card_hash_key,
    card_no,
    TO_DATE('{TD}') AS load_date,
    'CC' AS record_source,
    CURRENT_TIMESTAMP() AS etl_time
FROM bank_ods.ods_cc_t_cc_card WHERE dt='{TD}'
""")

# ============================================================
# 6. HUB_PRODUCT — 产品中心
# ============================================================
run_sql("HUB_PRODUCT", f"""
INSERT OVERWRITE TABLE bank_dwd.dwd_hub_product PARTITION (dt='{TD}')
SELECT DISTINCT
    MD5(CONCAT('CORE', '|', COALESCE(TRIM(PRODUCT_ID), ''), '|', 'HUB_PRODUCT')) AS product_hash_key,
    PRODUCT_ID AS product_id,
    TO_DATE('{TD}') AS load_date,
    'CORE' AS record_source,
    CURRENT_TIMESTAMP() AS etl_time
FROM bank_ods.ods_core_t_account WHERE dt='{TD}' AND PRODUCT_ID IS NOT NULL
""")

# ============================================================
# 7. HUB_ORG — 机构中心
# ============================================================
run_sql("HUB_ORG", f"""
INSERT OVERWRITE TABLE bank_dwd.dwd_hub_org PARTITION (dt='{TD}')
SELECT DISTINCT
    MD5(CONCAT('CORE', '|', COALESCE(TRIM(BRANCH_ID), ''), '|', 'HUB_ORG')) AS org_hash_key,
    BRANCH_ID AS org_id,
    TO_DATE('{TD}') AS load_date,
    'CORE' AS record_source,
    CURRENT_TIMESTAMP() AS etl_time
FROM bank_ods.ods_core_t_account WHERE dt='{TD}' AND BRANCH_ID IS NOT NULL
""")

# ============================================================
# 8. SAT_CUSTOMER_INFO — 客户属性 (SCD Type 2)
# ============================================================
run_sql("SAT_CUSTOMER_INFO", f"""
INSERT OVERWRITE TABLE bank_dwd.dwd_sat_customer_info PARTITION (dt='{TD}')
SELECT
    MD5(CONCAT('CORE', '|', COALESCE(TRIM(o.CUSTOMER_ID), ''), '|', 'HUB_CUSTOMER')) AS customer_hash_key,
    TO_DATE('{TD}') AS load_date,
    TO_DATE('9999-12-31') AS load_end_date,
    TRUE AS is_current,
    o.CUST_NAME, o.ID_TYPE, o.ID_NO, o.MOBILE, o.PHONE, o.EMAIL, o.ADDRESS,
    o.CUST_TYPE, o.CUST_LEVEL, o.GENDER, o.BIRTH_DATE, o.NATIONALITY,
    o.OCCUPATION, o.ANNUAL_INCOME, o.EDUCATION, o.OPEN_DATE, o.STATUS,
    'CORE' AS record_source,
    MD5(CONCAT_WS('|', COALESCE(o.CUST_NAME,''), COALESCE(o.ID_TYPE,''),
        COALESCE(o.ID_NO,''), COALESCE(o.MOBILE,''), COALESCE(o.CUST_TYPE,''),
        COALESCE(o.CUST_LEVEL,''), COALESCE(o.GENDER,''), COALESCE(o.STATUS,''))) AS hash_diff,
    CURRENT_TIMESTAMP() AS etl_time
FROM bank_ods.ods_core_t_customer o WHERE o.dt='{TD}'
""")

# ============================================================
# 9. SAT_ACCOUNT_BAL — 账户余额快照
# ============================================================
run_sql("SAT_ACCOUNT_BAL", f"""
INSERT OVERWRITE TABLE bank_dwd.dwd_sat_account_bal PARTITION (dt='{TD}')
SELECT
    MD5(CONCAT('CORE', '|', COALESCE(TRIM(b.ACCOUNT_NO), ''), '|', 'HUB_ACCOUNT')) AS account_hash_key,
    TO_DATE('{TD}') AS load_date,
    b.BALANCE, b.AVAIL_BALANCE, b.FROZEN_AMT,
    a.CURRENCY, b.LAST_UPDATE AS last_txn_time,
    'CORE' AS record_source,
    CURRENT_TIMESTAMP() AS etl_time
FROM bank_ods.ods_core_t_account_balance b
JOIN bank_ods.ods_core_t_account a ON b.ACCOUNT_NO = a.ACCOUNT_NO AND a.dt='{TD}'
WHERE b.dt='{TD}'
""")

# ============================================================
# 10. SAT_TRANSACTION — 交易事实
# ============================================================
run_sql("SAT_TRANSACTION", f"""
INSERT OVERWRITE TABLE bank_dwd.dwd_sat_transaction PARTITION (dt='{TD}')
SELECT
    MD5(CONCAT('CORE', '|', COALESCE(TRIM(TRANS_ID), ''), '|', 'HUB_TRANSACTION')) AS transaction_hash_key,
    TO_DATE('{TD}') AS load_date,
    TO_DATE(TRANS_TIME) AS trans_date,
    TRANS_TIME,
    TRANS_TYPE,
    TRANS_AMT,
    'CNY' AS currency,
    DR_CR_FLAG,
    NULL AS from_account_no,
    NULL AS to_account_no,
    CHANNEL,
    TELLER_ID,
    OPP_ACCOUNT AS opp_account_no,
    MEMO,
    'SUCCESS' AS trans_status,
    CASE WHEN CHANNEL IN ('ALIPAY','WECHAT_PAY','UNIONPAY') THEN TRUE ELSE FALSE END AS is_cross_bank,
    'CORE' AS record_source,
    CURRENT_TIMESTAMP() AS etl_time
FROM bank_ods.ods_core_t_transaction WHERE dt='{TD}'
""")

# ============================================================
# 11. LINK_CUST_ACCT — 客户-账户关系
# ============================================================
run_sql("LINK_CUST_ACCT", f"""
INSERT OVERWRITE TABLE bank_dwd.dwd_link_cust_acct PARTITION (dt='{TD}')
SELECT
    MD5(CONCAT(
        MD5(CONCAT('CORE', '|', COALESCE(TRIM(a.CUSTOMER_ID), ''), '|', 'HUB_CUSTOMER')), '|',
        MD5(CONCAT('CORE', '|', COALESCE(TRIM(a.ACCOUNT_NO), ''), '|', 'HUB_ACCOUNT')), '|',
        'PRIMARY', '|', 'LINK_CUST_ACCT'
    )) AS cust_acct_hash_key,
    MD5(CONCAT('CORE', '|', COALESCE(TRIM(a.CUSTOMER_ID), ''), '|', 'HUB_CUSTOMER')) AS customer_hash_key,
    MD5(CONCAT('CORE', '|', COALESCE(TRIM(a.ACCOUNT_NO), ''), '|', 'HUB_ACCOUNT')) AS account_hash_key,
    'PRIMARY' AS rel_type,
    CAST(a.OPEN_DATE AS DATE) AS rel_start_date,
    CAST(NULL AS DATE) AS rel_end_date,
    TRUE AS is_active,
    TO_DATE('{TD}') AS load_date,
    'CORE' AS record_source,
    CURRENT_TIMESTAMP() AS etl_time
FROM bank_ods.ods_core_t_account a WHERE a.dt='{TD}'
""")

# ============================================================
# Verification
# ============================================================
print("\n=== DWD ETL Verification ===")
for tbl, name in [
    ("bank_dwd.dwd_hub_customer","HUB_CUSTOMER"),
    ("bank_dwd.dwd_hub_account","HUB_ACCOUNT"),
    ("bank_dwd.dwd_hub_transaction","HUB_TRANSACTION"),
    ("bank_dwd.dwd_hub_loan_contract","HUB_LOAN_CONTRACT"),
    ("bank_dwd.dwd_hub_cc_card","HUB_CC_CARD"),
    ("bank_dwd.dwd_hub_product","HUB_PRODUCT"),
    ("bank_dwd.dwd_hub_org","HUB_ORG"),
    ("bank_dwd.dwd_sat_customer_info","SAT_CUSTOMER_INFO"),
    ("bank_dwd.dwd_sat_account_bal","SAT_ACCOUNT_BAL"),
    ("bank_dwd.dwd_sat_transaction","SAT_TRANSACTION"),
    ("bank_dwd.dwd_link_cust_acct","LINK_CUST_ACCT"),
]:
    cnt = spark.sql(f"SELECT COUNT(*) FROM {tbl} WHERE dt='{TD}'").collect()[0][0]
    print(f"  {name}: {cnt} rows")

spark.stop()

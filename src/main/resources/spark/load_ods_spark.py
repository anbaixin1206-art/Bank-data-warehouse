#!/usr/bin/env python3
"""PySpark: Load CSV data into Hive ODS ORC tables (simplified)"""
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("LoadODS") \
    .master("local[*]") \
    .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://localhost:9083") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("USE bank_ods")
TD = "2026-06-14"

def insert_from_csv(csv_path, table_name, col_select):
    """Create temp view from CSV, then INSERT INTO ODS"""
    print(f"Loading {table_name}...")
    df = spark.read.option("header","true").option("delimiter","\t").csv(csv_path)
    df.createOrReplaceTempView("_tmp")
    sql = f"INSERT OVERWRITE TABLE bank_ods.{table_name} PARTITION (dt='{TD}') SELECT {col_select} FROM _tmp"
    spark.sql(sql)
    cnt = spark.sql(f"SELECT COUNT(*) FROM bank_ods.{table_name} WHERE dt='{TD}'").collect()[0][0]
    print(f"  {table_name}: {cnt} rows")

STG = "hdfs://localhost:9000/data/stg"

# 1. Core Customers
insert_from_csv(f"{STG}/core/t_customer/dt={TD}/ods_core_t_customer.csv", "ods_core_t_customer",
    "CUSTOMER_ID, CUST_NAME, ID_TYPE, ID_NO, MOBILE, PHONE, EMAIL, ADDRESS, "
    "CUST_TYPE, CUST_LEVEL, GENDER, CAST(BIRTH_DATE AS DATE), NATIONALITY, OCCUPATION, "
    "CAST(ANNUAL_INCOME AS DECIMAL(18,2)), EDUCATION, CAST(OPEN_DATE AS DATE), "
    "CAST(NULL AS DATE), STATUS, CAST(CREATE_DATE AS DATE), CAST(UPDATE_TIME AS TIMESTAMP), "
    "CURRENT_TIMESTAMP(), 'CORE', 'batch_20260614'")

# 2. Core Accounts
insert_from_csv(f"{STG}/core/t_account/dt={TD}/ods_core_t_account.csv", "ods_core_t_account",
    "ACCOUNT_NO, CUSTOMER_ID, ACCT_TYPE, CURRENCY, CAST(OPEN_DATE AS DATE), "
    "STATUS, BRANCH_ID, PRODUCT_ID, CURRENT_TIMESTAMP(), 'CORE', 'batch_20260614'")

# 3. Account Balances
insert_from_csv(f"{STG}/core/t_account_balance/dt={TD}/ods_core_t_account_balance.csv", "ods_core_t_account_balance",
    "ACCOUNT_NO, CAST(BALANCE AS DECIMAL(18,2)), CAST(AVAIL_BALANCE AS DECIMAL(18,2)), "
    "CAST(FROZEN_AMT AS DECIMAL(18,2)), CAST(LAST_UPDATE AS TIMESTAMP), "
    "CURRENT_TIMESTAMP(), 'CORE', 'batch_20260614'")

# 4. Transactions
insert_from_csv(f"{STG}/core/t_transaction/dt={TD}/ods_core_t_transaction.csv", "ods_core_t_transaction",
    "TRANS_ID, ACCOUNT_NO, TRANS_TYPE, CAST(TRANS_AMT AS DECIMAL(18,2)), DR_CR_FLAG, "
    "CAST(TRANS_TIME AS TIMESTAMP), CHANNEL, TELLER_ID, OPP_ACCOUNT, MEMO, "
    "CURRENT_TIMESTAMP(), 'CORE', 'batch_20260614'")

# 5. Payment flows
insert_from_csv(f"{STG}/pay/t_payment_flow/dt={TD}/ods_pay_t_payment_flow.csv", "ods_pay_t_payment_flow",
    "pay_id, order_no, payer_acct, payee_acct, CAST(amount AS DECIMAL(18,2)), pay_channel, "
    "pay_status, CAST(create_time AS TIMESTAMP), CAST(complete_time AS TIMESTAMP), "
    "CURRENT_TIMESTAMP(), 'PAY', 'batch_20260614'")

# 6. Loan contracts
insert_from_csv(f"{STG}/loan/t_loan_contract/dt={TD}/ods_loan_t_loan_contract.csv", "ods_loan_t_loan_contract",
    "CONTRACT_NO, CUSTOMER_ID, LOAN_TYPE, CAST(LOAN_AMT AS DECIMAL(18,2)), "
    "CAST(RATE AS DECIMAL(9,6)), RATE_TYPE, CAST(TERM AS INT), REPAY_METHOD, "
    "CAST(SIGN_DATE AS DATE), CAST(START_DATE AS DATE), CAST(END_DATE AS DATE), "
    "GUARANTEE_TYPE, LOAN_PURPOSE, STATUS, CURRENT_TIMESTAMP(), 'LOAN', 'batch_20260614'")

# 7. Credit cards
insert_from_csv(f"{STG}/cc/t_cc_card/dt={TD}/ods_cc_t_cc_card.csv", "ods_cc_t_cc_card",
    "card_no, customer_id, card_type, card_level, CAST(credit_limit AS DECIMAL(18,2)), "
    "CAST(open_date AS DATE), CAST(expire_date AS DATE), status, "
    "CURRENT_TIMESTAMP(), 'CC', 'batch_20260614'")

# 8. E-bank transactions
insert_from_csv(f"{STG}/ebank/t_ebank_transaction/dt={TD}/ods_ebank_t_ebank_transaction.csv", "ods_ebank_t_ebank_transaction",
    "trans_id, customer_id, trans_type, CAST(amount AS DECIMAL(18,2)), "
    "from_acct, to_acct, channel, CAST(trans_time AS TIMESTAMP), "
    "CURRENT_TIMESTAMP(), 'EBANK', 'batch_20260614'")

print("\n=== Verification ===")
for t in ["ods_core_t_customer","ods_core_t_account","ods_core_t_account_balance",
          "ods_core_t_transaction","ods_pay_t_payment_flow","ods_loan_t_loan_contract",
          "ods_cc_t_cc_card","ods_ebank_t_ebank_transaction"]:
    cnt = spark.sql(f"SELECT COUNT(*) FROM bank_ods.{t} WHERE dt='{TD}'").collect()[0][0]
    print(f"  {t}: {cnt} rows")

spark.stop()

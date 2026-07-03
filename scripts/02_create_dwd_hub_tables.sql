-- ============================================================
-- DWD 层 Hub 表 DDL — Data Vault 核心实体
-- ============================================================

USE bank_dwd;

-- Hub: 客户中心
CREATE TABLE IF NOT EXISTS dwd_hub_customer (
    customer_hash_key   STRING      COMMENT '客户哈希主键 MD5(source|id|HUB_CUSTOMER)',
    customer_id         STRING      COMMENT '客户业务编号(源系统原始ID)',
    load_date           DATE        COMMENT '首次加载日期',
    record_source       STRING      COMMENT '首次来源系统',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '客户中心表 (Hub)'
PARTITIONED BY (dt STRING COMMENT '数据日期')
CLUSTERED BY (customer_hash_key) INTO 32 BUCKETS
STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB');

-- Hub: 账户中心
CREATE TABLE IF NOT EXISTS dwd_hub_account (
    account_hash_key    STRING      COMMENT '账户哈希主键',
    account_no          STRING      COMMENT '账号',
    load_date           DATE        COMMENT '首次加载日期',
    record_source       STRING      COMMENT '首次来源系统',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '账户中心表 (Hub)'
PARTITIONED BY (dt STRING COMMENT '数据日期')
CLUSTERED BY (account_hash_key) INTO 32 BUCKETS
STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB');

-- Hub: 交易中心
CREATE TABLE IF NOT EXISTS dwd_hub_transaction (
    transaction_hash_key STRING     COMMENT '交易哈希主键',
    trans_id             STRING     COMMENT '交易流水号(源系统)',
    load_date            DATE       COMMENT '首次加载日期',
    record_source        STRING     COMMENT '首次来源系统',
    etl_time             TIMESTAMP  COMMENT 'ETL处理时间'
)
COMMENT '交易中心表 (Hub)'
PARTITIONED BY (dt STRING COMMENT '数据日期')
CLUSTERED BY (transaction_hash_key) INTO 32 BUCKETS
STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB');

-- Hub: 贷款合同中心
CREATE TABLE IF NOT EXISTS dwd_hub_loan_contract (
    contract_hash_key   STRING      COMMENT '贷款合同哈希主键',
    contract_no         STRING      COMMENT '贷款合同号',
    load_date           DATE        COMMENT '首次加载日期',
    record_source       STRING      COMMENT '首次来源系统',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '贷款合同中心表 (Hub)'
PARTITIONED BY (dt STRING COMMENT '数据日期')
CLUSTERED BY (contract_hash_key) INTO 32 BUCKETS
STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB');

-- Hub: 信用卡中心
CREATE TABLE IF NOT EXISTS dwd_hub_cc_card (
    card_hash_key       STRING      COMMENT '信用卡哈希主键',
    card_no             STRING      COMMENT '卡号',
    load_date           DATE        COMMENT '首次加载日期',
    record_source       STRING      COMMENT '首次来源系统',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '信用卡中心表 (Hub)'
PARTITIONED BY (dt STRING COMMENT '数据日期')
CLUSTERED BY (card_hash_key) INTO 32 BUCKETS
STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB');

-- Hub: 产品中心
CREATE TABLE IF NOT EXISTS dwd_hub_product (
    product_hash_key    STRING      COMMENT '产品哈希主键',
    product_id          STRING      COMMENT '产品编号',
    load_date           DATE        COMMENT '首次加载日期',
    record_source       STRING      COMMENT '首次来源系统',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '产品中心表 (Hub)'
PARTITIONED BY (dt STRING COMMENT '数据日期')
CLUSTERED BY (product_hash_key) INTO 32 BUCKETS
STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB');

-- Hub: 机构中心
CREATE TABLE IF NOT EXISTS dwd_hub_org (
    org_hash_key        STRING      COMMENT '机构哈希主键',
    org_id              STRING      COMMENT '机构业务编号',
    load_date           DATE        COMMENT '首次加载日期',
    record_source       STRING      COMMENT '首次来源系统',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '机构中心表 (Hub)'
PARTITIONED BY (dt STRING COMMENT '数据日期')
CLUSTERED BY (org_hash_key) INTO 32 BUCKETS
STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB');

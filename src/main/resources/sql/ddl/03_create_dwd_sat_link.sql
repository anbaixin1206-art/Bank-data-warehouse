-- ============================================================
-- DWD 层 Satellite + Link 表 DDL
-- ============================================================

USE bank_dwd;

-- Satellite: 客户属性 (SCD Type 2 拉链)
CREATE TABLE IF NOT EXISTS dwd_sat_customer_info (
    customer_hash_key   STRING      COMMENT '客户哈希主键',
    load_date           DATE        COMMENT '记录生效日期',
    load_end_date       DATE        COMMENT '记录失效日期 (9999-12-31=当前)',
    is_current          BOOLEAN     COMMENT '是否当前记录',
    cust_name           STRING      COMMENT '客户姓名',
    id_type             STRING      COMMENT '证件类型',
    id_no               STRING      COMMENT '证件号码',
    mobile              STRING      COMMENT '手机号码',
    phone               STRING      COMMENT '固定电话',
    email               STRING      COMMENT '电子邮箱',
    address             STRING      COMMENT '通讯地址',
    cust_type           STRING      COMMENT '客户类型',
    cust_level          STRING      COMMENT '客户等级',
    gender              STRING      COMMENT '性别',
    birth_date          DATE        COMMENT '出生日期',
    nationality         STRING      COMMENT '国籍',
    occupation          STRING      COMMENT '职业',
    annual_income       DECIMAL(18,2) COMMENT '年收入',
    education           STRING      COMMENT '学历',
    open_date           DATE        COMMENT '开户日期',
    status              STRING      COMMENT '客户状态',
    record_source       STRING      COMMENT '数据来源',
    hash_diff           STRING      COMMENT '属性MD5差异值',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '客户属性卫星表 (Satellite) — SCD Type 2'
PARTITIONED BY (dt STRING COMMENT '数据日期')
CLUSTERED BY (customer_hash_key) INTO 32 BUCKETS
STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB');

-- Satellite: 账户余额快照
CREATE TABLE IF NOT EXISTS dwd_sat_account_bal (
    account_hash_key    STRING      COMMENT '账户哈希主键',
    load_date           DATE        COMMENT '快照日期',
    balance             DECIMAL(18,2) COMMENT '账户余额',
    avail_balance       DECIMAL(18,2) COMMENT '可用余额',
    frozen_amt          DECIMAL(18,2) COMMENT '冻结金额',
    currency            STRING      COMMENT '币种',
    last_txn_time       TIMESTAMP   COMMENT '最近交易时间',
    record_source       STRING      COMMENT '数据来源',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '账户余额快照卫星表 (Satellite)'
PARTITIONED BY (dt STRING COMMENT '快照日期')
CLUSTERED BY (account_hash_key) INTO 32 BUCKETS
STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB');

-- Satellite: 交易事实
CREATE TABLE IF NOT EXISTS dwd_sat_transaction (
    transaction_hash_key STRING     COMMENT '交易哈希主键',
    load_date            DATE       COMMENT '加载日期',
    trans_date           DATE       COMMENT '交易日期',
    trans_time           TIMESTAMP  COMMENT '交易时间',
    trans_type           STRING     COMMENT '交易类型',
    trans_amt            DECIMAL(18,2) COMMENT '交易金额',
    currency             STRING     COMMENT '币种',
    dr_cr_flag           STRING     COMMENT '借贷标志',
    from_account_no      STRING     COMMENT '转出账号',
    to_account_no        STRING     COMMENT '转入账号',
    channel              STRING     COMMENT '渠道',
    teller_id            STRING     COMMENT '柜员号',
    opp_account_no       STRING     COMMENT '对手账号',
    memo                 STRING     COMMENT '摘要',
    trans_status         STRING     COMMENT '交易状态',
    is_cross_bank        BOOLEAN    COMMENT '是否跨行',
    record_source        STRING     COMMENT '数据来源',
    etl_time             TIMESTAMP  COMMENT 'ETL处理时间'
)
COMMENT '交易事实卫星表 (Satellite) — 追加型'
PARTITIONED BY (dt STRING COMMENT '交易日期')
CLUSTERED BY (transaction_hash_key) INTO 32 BUCKETS
STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB');

-- Link: 客户-账户关系
CREATE TABLE IF NOT EXISTS dwd_link_cust_acct (
    cust_acct_hash_key  STRING      COMMENT '客户-账户关系哈希主键',
    customer_hash_key   STRING      COMMENT '客户哈希主键 (FK→HUB_CUSTOMER)',
    account_hash_key    STRING      COMMENT '账户哈希主键 (FK→HUB_ACCOUNT)',
    rel_type            STRING      COMMENT '关系类型: PRIMARY/JOINT',
    rel_start_date      DATE        COMMENT '关系生效日期',
    rel_end_date        DATE        COMMENT '关系失效日期',
    is_active           BOOLEAN     COMMENT '关系是否有效',
    load_date           DATE        COMMENT '加载日期',
    record_source       STRING      COMMENT '数据来源',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '客户-账户关系链接表 (Link)'
PARTITIONED BY (dt STRING COMMENT '数据日期')
CLUSTERED BY (cust_acct_hash_key) INTO 32 BUCKETS
STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB');

-- DWS: 客户日汇总
CREATE TABLE IF NOT EXISTS bank_dws.dws_cust_daily_summary (
    customer_hash_key   STRING      COMMENT '客户哈希主键',
    total_asset_amt     DECIMAL(18,2) COMMENT '总资产(AUM)',
    deposit_amt         DECIMAL(18,2) COMMENT '存款余额',
    loan_amt            DECIMAL(18,2) COMMENT '贷款余额',
    daily_txn_cnt       BIGINT      COMMENT '当日交易笔数',
    daily_txn_amt       DECIMAL(18,2) COMMENT '当日交易金额',
    account_cnt         INT         COMMENT '持有账户数',
    product_holding_cnt INT         COMMENT '持有产品种类数',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '客户日汇总事实表'
PARTITIONED BY (dt STRING COMMENT '快照日期')
CLUSTERED BY (customer_hash_key) INTO 16 BUCKETS
STORED AS ORC;

-- ADS: 管理驾驶舱KPI
CREATE TABLE IF NOT EXISTS bank_ads.ads_mgmt_kpi (
    kpi_code            STRING      COMMENT '指标编码',
    kpi_name            STRING      COMMENT '指标名称',
    kpi_category        STRING      COMMENT '指标分类',
    kpi_value           DECIMAL(18,4) COMMENT '指标值',
    kpi_unit            STRING      COMMENT '单位',
    yoy_change          DECIMAL(5,2) COMMENT '同比变动%',
    mom_change          DECIMAL(5,2) COMMENT '环比变动%',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '管理驾驶舱核心KPI表'
PARTITIONED BY (dt STRING COMMENT '统计日期')
STORED AS ORC;

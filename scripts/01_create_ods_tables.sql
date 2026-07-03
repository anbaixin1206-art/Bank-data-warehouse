-- ============================================================
-- ODS 层建表 DDL — 核心源系统
-- ============================================================

USE bank_ods;

-- ODS 核心银行 - 客户表
CREATE TABLE IF NOT EXISTS ods_core_t_customer (
    CUSTOMER_ID         STRING      COMMENT '客户编号',
    CUST_NAME           STRING      COMMENT '客户姓名',
    ID_TYPE             STRING      COMMENT '证件类型',
    ID_NO               STRING      COMMENT '证件号码',
    MOBILE              STRING      COMMENT '手机号码',
    PHONE               STRING      COMMENT '固定电话',
    EMAIL               STRING      COMMENT '电子邮箱',
    ADDRESS             STRING      COMMENT '通讯地址',
    CUST_TYPE           STRING      COMMENT '客户类型: PERSONAL/CORPORATE',
    CUST_LEVEL          STRING      COMMENT '客户等级',
    GENDER              STRING      COMMENT '性别',
    BIRTH_DATE          DATE        COMMENT '出生日期',
    NATIONALITY         STRING      COMMENT '国籍',
    OCCUPATION          STRING      COMMENT '职业',
    ANNUAL_INCOME       DECIMAL(18,2) COMMENT '年收入',
    EDUCATION           STRING      COMMENT '学历',
    OPEN_DATE           DATE        COMMENT '开户日期',
    CLOSE_DATE          DATE        COMMENT '销户日期',
    STATUS              STRING      COMMENT '客户状态',
    CREATE_DATE         DATE        COMMENT '创建日期',
    UPDATE_TIME         TIMESTAMP   COMMENT '更新时间',
    ingest_time         TIMESTAMP   COMMENT '数据采集时间',
    source_system       STRING      COMMENT '来源系统',
    batch_id            STRING      COMMENT '采集批次号'
)
COMMENT 'ODS-核心银行客户表'
PARTITIONED BY (dt STRING COMMENT '业务日期 yyyy-MM-dd')
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- ODS 核心银行 - 账户表
CREATE TABLE IF NOT EXISTS ods_core_t_account (
    ACCOUNT_NO          STRING      COMMENT '账号',
    CUSTOMER_ID         STRING      COMMENT '客户编号',
    ACCT_TYPE           STRING      COMMENT '账户类型',
    CURRENCY            STRING      COMMENT '币种',
    OPEN_DATE           DATE        COMMENT '开户日期',
    STATUS              STRING      COMMENT '账户状态',
    BRANCH_ID           STRING      COMMENT '开户机构',
    PRODUCT_ID          STRING      COMMENT '产品编号',
    ingest_time         TIMESTAMP   COMMENT '数据采集时间',
    source_system       STRING      COMMENT '来源系统',
    batch_id            STRING      COMMENT '采集批次号'
)
COMMENT 'ODS-核心银行账户表'
PARTITIONED BY (dt STRING COMMENT '业务日期')
STORED AS ORC TBLPROPERTIES ('orc.compress'='SNAPPY');

-- ODS 核心银行 - 账户余额表
CREATE TABLE IF NOT EXISTS ods_core_t_account_balance (
    ACCOUNT_NO          STRING      COMMENT '账号',
    BALANCE             DECIMAL(18,2) COMMENT '账户余额',
    AVAIL_BALANCE       DECIMAL(18,2) COMMENT '可用余额',
    FROZEN_AMT          DECIMAL(18,2) COMMENT '冻结金额',
    LAST_UPDATE         TIMESTAMP   COMMENT '最后更新时间',
    ingest_time         TIMESTAMP   COMMENT '数据采集时间',
    source_system       STRING      COMMENT '来源系统',
    batch_id            STRING      COMMENT '采集批次号'
)
COMMENT 'ODS-核心银行账户余额表'
PARTITIONED BY (dt STRING COMMENT '业务日期')
STORED AS ORC TBLPROPERTIES ('orc.compress'='SNAPPY');

-- ODS 核心银行 - 交易流水表
CREATE TABLE IF NOT EXISTS ods_core_t_transaction (
    TRANS_ID            STRING      COMMENT '交易流水号',
    ACCOUNT_NO          STRING      COMMENT '账号',
    TRANS_TYPE          STRING      COMMENT '交易类型',
    TRANS_AMT           DECIMAL(18,2) COMMENT '交易金额',
    DR_CR_FLAG          STRING      COMMENT '借贷标志 D-借 C-贷',
    TRANS_TIME          TIMESTAMP   COMMENT '交易时间',
    CHANNEL             STRING      COMMENT '渠道',
    TELLER_ID           STRING      COMMENT '柜员号',
    OPP_ACCOUNT         STRING      COMMENT '对手账号',
    MEMO                STRING      COMMENT '摘要',
    ingest_time         TIMESTAMP   COMMENT '数据采集时间',
    source_system       STRING      COMMENT '来源系统',
    batch_id            STRING      COMMENT '采集批次号'
)
COMMENT 'ODS-核心银行交易流水表'
PARTITIONED BY (dt STRING COMMENT '业务日期')
STORED AS ORC TBLPROPERTIES ('orc.compress'='SNAPPY');

-- ODS 支付网关 - 支付流水表
CREATE TABLE IF NOT EXISTS ods_pay_t_payment_flow (
    pay_id              STRING      COMMENT '支付流水号',
    order_no            STRING      COMMENT '业务订单号',
    payer_acct          STRING      COMMENT '付款账号',
    payee_acct          STRING      COMMENT '收款账号',
    amount              DECIMAL(18,2) COMMENT '支付金额',
    pay_channel         STRING      COMMENT '支付渠道',
    pay_status          STRING      COMMENT '支付状态',
    create_time         TIMESTAMP   COMMENT '创建时间',
    complete_time       TIMESTAMP   COMMENT '完成时间',
    ingest_time         TIMESTAMP   COMMENT '数据采集时间',
    source_system       STRING      COMMENT '来源系统',
    batch_id            STRING      COMMENT '采集批次号'
)
COMMENT 'ODS-支付网关支付流水表'
PARTITIONED BY (dt STRING COMMENT '业务日期')
STORED AS ORC TBLPROPERTIES ('orc.compress'='SNAPPY');

-- ODS 信贷管理 - 贷款合同表
CREATE TABLE IF NOT EXISTS ods_loan_t_loan_contract (
    CONTRACT_NO         STRING      COMMENT '贷款合同号',
    CUSTOMER_ID         STRING      COMMENT '客户编号',
    LOAN_TYPE           STRING      COMMENT '贷款类型',
    LOAN_AMT            DECIMAL(18,2) COMMENT '贷款金额',
    RATE                DECIMAL(9,6) COMMENT '年利率',
    RATE_TYPE           STRING      COMMENT '利率类型',
    TERM                INT         COMMENT '期限(月)',
    REPAY_METHOD        STRING      COMMENT '还款方式',
    SIGN_DATE           DATE        COMMENT '签订日期',
    START_DATE          DATE        COMMENT '起期',
    END_DATE            DATE        COMMENT '止期',
    GUARANTEE_TYPE      STRING      COMMENT '担保方式',
    LOAN_PURPOSE        STRING      COMMENT '贷款用途',
    STATUS              STRING      COMMENT '合同状态',
    ingest_time         TIMESTAMP   COMMENT '数据采集时间',
    source_system       STRING      COMMENT '来源系统',
    batch_id            STRING      COMMENT '采集批次号'
)
COMMENT 'ODS-信贷管理贷款合同表'
PARTITIONED BY (dt STRING COMMENT '业务日期')
STORED AS ORC TBLPROPERTIES ('orc.compress'='SNAPPY');

-- ODS 信用卡 - 信用卡表
CREATE TABLE IF NOT EXISTS ods_cc_t_cc_card (
    card_no             STRING      COMMENT '卡号',
    customer_id         STRING      COMMENT '客户编号',
    card_type           STRING      COMMENT '卡片类型',
    card_level          STRING      COMMENT '卡等级',
    credit_limit        DECIMAL(18,2) COMMENT '信用额度',
    open_date           DATE        COMMENT '发卡日期',
    expire_date         DATE        COMMENT '有效期',
    status              STRING      COMMENT '卡片状态',
    ingest_time         TIMESTAMP   COMMENT '数据采集时间',
    source_system       STRING      COMMENT '来源系统',
    batch_id            STRING      COMMENT '采集批次号'
)
COMMENT 'ODS-信用卡卡片表'
PARTITIONED BY (dt STRING COMMENT '业务日期')
STORED AS ORC TBLPROPERTIES ('orc.compress'='SNAPPY');

-- ODS 网银 - 电子渠道交易表
CREATE TABLE IF NOT EXISTS ods_ebank_t_ebank_transaction (
    trans_id            STRING      COMMENT '交易流水号',
    customer_id         STRING      COMMENT '客户编号',
    trans_type          STRING      COMMENT '交易类型',
    amount              DECIMAL(18,2) COMMENT '交易金额',
    from_acct           STRING      COMMENT '转出账号',
    to_acct             STRING      COMMENT '转入账号',
    channel             STRING      COMMENT '渠道',
    trans_time          TIMESTAMP   COMMENT '交易时间',
    ingest_time         TIMESTAMP   COMMENT '数据采集时间',
    source_system       STRING      COMMENT '来源系统',
    batch_id            STRING      COMMENT '采集批次号'
)
COMMENT 'ODS-网银电子渠道交易表'
PARTITIONED BY (dt STRING COMMENT '业务日期')
STORED AS ORC TBLPROPERTIES ('orc.compress'='SNAPPY');

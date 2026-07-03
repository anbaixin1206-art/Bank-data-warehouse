# 08 信用卡域 DWD 层表设计

> 所属：03_数据仓库模型 → DWD层表设计
> 上一文档：[07_支付域_DWD](./07_支付域_DWD.md)
> 下一文档：[09_理财域_DWD](./09_理财域_DWD.md)

---

## 1 信用卡域模型概览

```
┌──────────────────────────────────────────────────────────────────┐
│                    信用卡域 DWD 模型                                │
│                                                                  │
│  ┌──────────────┐   ┌──────────────────┐   ┌──────────────┐      │
│  │  HUB_CC_CARD │◄──┤  LINK_CUST_CC    │──►│HUB_CUSTOMER  │      │
│  │  card_hash   │   │  cust_cc_hash    │   │              │      │
│  │  card_no     │   │  card_role       │   └──────────────┘      │
│  └──────┬───────┘   └──────────────────┘                         │
│         │                                                       │
│         ├── SAT_CC_CARD_INFO (卡片属性拉链)                        │
│         │   card_type, card_level, credit_limit, status         │
│         │                                                       │
│         └── LINK_CC_TRANSACTION ──► SAT_CC_TRANSACTION (消费事实) │
│               │                     trans_amt, mcc_code,         │
│               │                     merchant_id, installment     │
│               ▼                                                  │
│         HUB_MERCHANT (商户)                                       │
│                                                                  │
│  ┌──────────────────────────────────────────────┐                │
│  │ 信用卡账单和还款使用统一交易模型(支付域)     │                │
│  │ SAT_TRANSACTION (CC_REPAY / CC_CONSUME)     │                │
│  └──────────────────────────────────────────────┘                │
└──────────────────────────────────────────────────────────────────┘
```

---

## 2 Hub 表

### 2.1 dwd_hub_cc_card — 信用卡中心表

```sql
CREATE TABLE dwd_hub_cc_card (
    card_hash_key       STRING      COMMENT '信用卡哈希主键',
    card_no             STRING      COMMENT '卡号(脱敏后)',
    load_date           DATE        COMMENT '首次加载日期',
    record_source       STRING      COMMENT '首次来源系统',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '信用卡中心表 (Hub) — 所有信用卡'
PARTITIONED BY (dt STRING COMMENT '数据日期')
CLUSTERED BY (card_hash_key) INTO 32 BUCKETS
STORED AS ORC
TBLPROPERTIES ('orc.compress'='ZLIB');
```

### 2.2 dwd_hub_merchant — 商户中心表

```sql
CREATE TABLE dwd_hub_merchant (
    merchant_hash_key   STRING      COMMENT '商户哈希主键',
    merchant_id         STRING      COMMENT '商户编号',
    load_date           DATE        COMMENT '首次加载日期',
    record_source       STRING      COMMENT '首次来源系统',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '商户中心表 (Hub) — POS收单商户'
PARTITIONED BY (dt STRING COMMENT '数据日期')
CLUSTERED BY (merchant_hash_key) INTO 32 BUCKETS
STORED AS ORC
TBLPROPERTIES ('orc.compress'='ZLIB');
```

---

## 3 Satellite 表

### 3.1 dwd_sat_cc_card_info — 信用卡属性卫星表（拉链）

```sql
CREATE TABLE dwd_sat_cc_card_info (
    card_hash_key       STRING      COMMENT '信用卡哈希主键',
    load_date           DATE        COMMENT '记录生效日期',
    load_end_date       DATE        COMMENT '记录失效日期',
    is_current          BOOLEAN     COMMENT '是否当前记录',
    card_type           STRING      COMMENT '卡片类型: STANDARD/GOLD/PLATINUM/DIAMOND/COBRAND/INSTALLMENT',
    card_level           STRING      COMMENT '卡等级: CLASSIC/GOLD/PLATINUM/DIAMOND/INFINITE',
    credit_limit        DECIMAL(18,2) COMMENT '信用额度',
    temp_limit          DECIMAL(18,2) COMMENT '临时额度',
    cash_advance_limit  DECIMAL(18,2) COMMENT '取现额度',
    annual_fee           DECIMAL(18,2) COMMENT '年费',
    interest_rate        DECIMAL(9,6) COMMENT '透支年利率',
    late_fee_rate        DECIMAL(9,6) COMMENT '滞纳金比例',
    statement_date      INT         COMMENT '账单日(每月几号)',
    due_date_offset     INT         COMMENT '到期还款日偏移(账单日后几天)',
    grace_period_days   INT         COMMENT '免息期天数',
    open_date           DATE        COMMENT '发卡日期',
    expire_date         DATE        COMMENT '卡片有效期',
    status              STRING      COMMENT '卡片状态: ACTIVE/INACTIVE/FROZEN/LOST/CLOSED',
    product_hash_key    STRING      COMMENT '产品哈希主键',
    record_source       STRING      COMMENT '数据来源',
    hash_diff           STRING      COMMENT '属性MD5差异值',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '信用卡属性卫星表 (Satellite) — SCD Type 2'
PARTITIONED BY (dt STRING COMMENT '数据日期')
CLUSTERED BY (card_hash_key) INTO 32 BUCKETS
STORED AS ORC
TBLPROPERTIES ('orc.compress'='ZLIB');
```

### 3.2 dwd_sat_cc_transaction — 信用卡消费明细卫星表（事实型）

```sql
CREATE TABLE dwd_sat_cc_transaction (
    cc_trans_hash_key   STRING      COMMENT '信用卡交易哈希主键',
    card_hash_key       STRING      COMMENT '卡片哈希主键',
    merchant_hash_key   STRING      COMMENT '商户哈希主键',
    load_date           DATE        COMMENT '加载日期',
    trans_date          DATE        COMMENT '交易日期',
    trans_time          TIMESTAMP   COMMENT '交易时间',
    trans_amt           DECIMAL(18,2) COMMENT '交易金额(原币)',
    settle_amt          DECIMAL(18,2) COMMENT '结算金额(人民币)',
    currency            STRING      COMMENT '原币种',
    mcc_code            STRING      COMMENT 'MCC商户类别码',
    trans_type          STRING      COMMENT '交易类型: CONSUME/CASH_ADVANCE/INSTALLMENT/BALANCE_TRANSFER',
    installment_periods INT         COMMENT '分期期数(分期交易时)',
    auth_code           STRING      COMMENT '授权码',
    pos_entry_mode      STRING      COMMENT 'POS输入方式: CHIP/SWIPE/MANUAL/CONTACTLESS',
    is_overseas          BOOLEAN     COMMENT '是否境外交易',
    country_code         STRING      COMMENT '交易国家代码',
    record_source        STRING      COMMENT '数据来源',
    etl_time             TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '信用卡消费明细卫星表 (Satellite) — 事实型追加'
PARTITIONED BY (dt STRING COMMENT '交易日期')
CLUSTERED BY (cc_trans_hash_key) INTO 32 BUCKETS
STORED AS ORC
TBLPROPERTIES ('orc.compress'='ZLIB');
```

### 3.3 dwd_sat_cc_statement — 信用卡账单卫星表（事实型）

```sql
CREATE TABLE dwd_sat_cc_statement (
    card_hash_key       STRING      COMMENT '卡片哈希主键',
    load_date           DATE        COMMENT '加载日期',
    stmt_date           DATE        COMMENT '账单日',
    due_date            DATE        COMMENT '到期还款日',
    opening_bal         DECIMAL(18,2) COMMENT '期初余额',
    new_charges         DECIMAL(18,2) COMMENT '本期新增消费',
    payments            DECIMAL(18,2) COMMENT '本期还款',
    adjustments         DECIMAL(18,2) COMMENT '本期调整',
    closing_bal         DECIMAL(18,2) COMMENT '期末余额',
    min_payment         DECIMAL(18,2) COMMENT '最低还款额',
    is_paid_off         BOOLEAN     COMMENT '是否已全额还款',
    is_overdue          BOOLEAN     COMMENT '是否已逾期',
    overdue_days        INT         COMMENT '逾期天数',
    record_source       STRING      COMMENT '数据来源',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '信用卡账单卫星表 (Satellite) — 每月账单快照'
PARTITIONED BY (dt STRING COMMENT '账单日期')
CLUSTERED BY (card_hash_key) INTO 32 BUCKETS
STORED AS ORC
TBLPROPERTIES ('orc.compress'='ZLIB');
```

---

## 4 Link 表

### 4.1 dwd_link_cust_cc — 客户-信用卡关系表

```sql
CREATE TABLE dwd_link_cust_cc (
    cust_cc_hash_key    STRING      COMMENT '客户-信用卡关系哈希主键',
    customer_hash_key   STRING      COMMENT '客户哈希主键',
    card_hash_key       STRING      COMMENT '卡片哈希主键',
    card_role           STRING      COMMENT '持卡角色: PRIMARY/SUPPLEMENTARY',
    load_date           DATE        COMMENT '加载日期',
    record_source       STRING      COMMENT '数据来源',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '客户-信用卡关系链接表 (Link)'
PARTITIONED BY (dt STRING COMMENT '数据日期')
CLUSTERED BY (cust_cc_hash_key) INTO 32 BUCKETS
STORED AS ORC
TBLPROPERTIES ('orc.compress'='ZLIB');
```

---

## 5 核心指标定义

| 指标 | 定义 | 计算来源 |
|------|------|---------|
| 发卡量 | COUNT(DISTINCT card_hash_key) | HUB_CC_CARD |
| 活跃卡量 | 近3个月有交易的卡片数 | SAT_CC_TRANSACTION |
| 消费金额 | SUM(trans_amt) | SAT_CC_TRANSACTION |
| 透支余额 | SUM(closing_bal) WHERE is_paid_off=FALSE | SAT_CC_STATEMENT |
| 不良率 | 逾期90+天余额 / 总透支余额 | SAT_CC_STATEMENT |
| 分期业务占比 | 分期交易金额 / 总消费金额 | SAT_CC_TRANSACTION (installment_periods>0) |

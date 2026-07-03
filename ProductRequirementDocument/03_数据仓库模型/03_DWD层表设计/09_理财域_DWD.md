# 09 理财域 DWD 层表设计

> 所属：03_数据仓库模型 → DWD层表设计
> 上一文档：[08_信用卡域_DWD](./08_信用卡域_DWD.md)
> 下一文档：[10_国际结算域_DWD](./10_国际结算域_DWD.md)

---

## 1 理财域模型概览

```
┌──────────────────────────────────────────────────────────────┐
│                   理财域 DWD 模型                               │
│                                                              │
│  ┌──────────────────┐   ┌──────────────────┐                 │
│  │  HUB_PRODUCT     │◄──┤ LINK_CUST_WEALTH │──►HUB_CUSTOMER │
│  │  (理财产品)       │   │ cust_wealth_hash │                 │
│  └────────┬─────────┘   └──────────────────┘                 │
│           │                                                  │
│           └── SAT_PRODUCT_WEALTH (理财产品专属属性)            │
│               risk_level, nav, min_amount, term,            │
│               expected_return, product_maturity             │
│                                                              │
│  ┌──────────────────────────────────────────────┐            │
│  │  SAT_WEALTH_HOLDING (持仓快照 - 每日)         │            │
│  │  holding_shares, nav, market_value, profit   │            │
│  └──────────────────────────────────────────────┘            │
│                                                              │
│  ┌──────────────────────────────────────────────┐            │
│  │  SAT_WEALTH_TRANSACTION (交易明细 - 事实型)   │            │
│  │  trans_type: SUBSCRIBE/REDEEM/DIVIDEND/...   │            │
│  └──────────────────────────────────────────────┘            │
└──────────────────────────────────────────────────────────────┘
```

---

## 2 Hub 表（复用产品域 HUB_PRODUCT）

理财产品使用产品域的 `dwd_hub_product`，通过 `product_type = 'WEALTH'` 区分。

---

## 3 Satellite 表

### 3.1 dwd_sat_wealth_product — 理财产品属性卫星表

```sql
CREATE TABLE dwd_sat_wealth_product (
    product_hash_key    STRING      COMMENT '产品哈希主键',
    load_date           DATE        COMMENT '记录生效日期',
    load_end_date       DATE        COMMENT '记录失效日期',
    is_current          BOOLEAN     COMMENT '是否当前记录',
    product_name        STRING      COMMENT '产品名称',
    product_type        STRING      COMMENT '产品类型: MONEY_FUND/BOND_WM/NAV_WM/TRUST/INSURANCE',
    risk_level          STRING      COMMENT '风险等级: R1/R2/R3/R4/R5',
    min_subscribe_amt   DECIMAL(18,2) COMMENT '最低认购金额',
    min_redeem_shares   DECIMAL(18,2) COMMENT '最低赎回份额',
    subscribe_fee_rate  DECIMAL(9,6) COMMENT '认购费率',
    redeem_fee_rate     DECIMAL(9,6) COMMENT '赎回费率',
    manage_fee_rate     DECIMAL(9,6) COMMENT '管理费率',
    expected_return_low DECIMAL(9,6) COMMENT '预期收益下限',
    expected_return_high DECIMAL(9,6) COMMENT '预期收益上限',
    term_days           INT         COMMENT '产品期限(天), 0=开放式',
    is_open_end         BOOLEAN     COMMENT '是否开放式',
    nav                 DECIMAL(18,6) COMMENT '最新单位净值',
    nav_date            DATE        COMMENT '净值日期',
    total_scale         DECIMAL(18,2) COMMENT '产品总规模',
    launch_date         DATE        COMMENT '成立日期',
    maturity_date       DATE        COMMENT '到期日期',
    status              STRING      COMMENT '状态: PRE_SALE/OPEN/CLOSED/MATURED',
    record_source       STRING      COMMENT '数据来源',
    hash_diff           STRING      COMMENT '属性MD5差异值',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '理财产品属性卫星表 (Satellite) — SCD Type 2'
PARTITIONED BY (dt STRING COMMENT '数据日期')
CLUSTERED BY (product_hash_key) INTO 32 BUCKETS
STORED AS ORC
TBLPROPERTIES ('orc.compress'='ZLIB');
```

### 3.2 dwd_sat_wealth_holding — 理财持仓快照卫星表

```sql
CREATE TABLE dwd_sat_wealth_holding (
    customer_hash_key   STRING      COMMENT '客户哈希主键',
    product_hash_key    STRING      COMMENT '产品哈希主键',
    load_date           DATE        COMMENT '快照日期',
    holding_shares      DECIMAL(18,6) COMMENT '持有份额',
    nav                  DECIMAL(18,6) COMMENT '单位净值',
    market_value         DECIMAL(18,2) COMMENT '持仓市值',
    cost_amt             DECIMAL(18,2) COMMENT '成本金额',
    total_profit         DECIMAL(18,2) COMMENT '累计收益',
    daily_profit         DECIMAL(18,2) COMMENT '当日收益',
    holding_days         INT         COMMENT '持有天数',
    record_source        STRING      COMMENT '数据来源',
    etl_time             TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '理财持仓快照卫星表 (Satellite) — 每日快照'
PARTITIONED BY (dt STRING COMMENT '快照日期')
CLUSTERED BY (customer_hash_key) INTO 32 BUCKETS
STORED AS ORC
TBLPROPERTIES ('orc.compress'='ZLIB');
```

### 3.3 dwd_sat_wealth_transaction — 理财交易明细卫星表（事实型）

```sql
CREATE TABLE dwd_sat_wealth_transaction (
    customer_hash_key   STRING      COMMENT '客户哈希主键',
    product_hash_key    STRING      COMMENT '产品哈希主键',
    load_date           DATE        COMMENT '加载日期',
    trans_date          DATE        COMMENT '交易日期',
    trans_time          TIMESTAMP   COMMENT '交易时间',
    trans_type          STRING      COMMENT '交易类型: SUBSCRIBE/REDEEM/DIVIDEND/TRANSFER_IN/TRANSFER_OUT',
    trans_amt           DECIMAL(18,2) COMMENT '交易金额',
    trans_shares        DECIMAL(18,6) COMMENT '交易份额',
    nav_at_trans        DECIMAL(18,6) COMMENT '交易时净值',
    fee_amt             DECIMAL(18,2) COMMENT '手续费',
    channel_hash_key    STRING      COMMENT '交易渠道',
    trans_status        STRING      COMMENT '交易状态: CONFIRMED/PENDING/CANCELLED',
    record_source       STRING      COMMENT '数据来源',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '理财交易明细卫星表 (Satellite) — 事实型追加'
PARTITIONED BY (dt STRING COMMENT '交易日期')
CLUSTERED BY (customer_hash_key) INTO 32 BUCKETS
STORED AS ORC
TBLPROPERTIES ('orc.compress'='ZLIB');
```

---

## 4 Link 表

### 4.1 dwd_link_cust_wealth — 客户-理财产品关系表

```sql
CREATE TABLE dwd_link_cust_wealth (
    cust_wealth_hash_key STRING     COMMENT '客户-理财关系哈希主键',
    customer_hash_key   STRING      COMMENT '客户哈希主键',
    product_hash_key    STRING      COMMENT '产品哈希主键',
    load_date           DATE        COMMENT '加载日期',
    record_source       STRING      COMMENT '数据来源',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '客户-理财产品持有关系链接表 (Link)'
PARTITIONED BY (dt STRING COMMENT '数据日期')
CLUSTERED BY (cust_wealth_hash_key) INTO 32 BUCKETS
STORED AS ORC
TBLPROPERTIES ('orc.compress'='ZLIB');
```

---

## 5 核心指标定义

| 指标 | 定义 | 计算来源 |
|------|------|---------|
| AUM(管理资产) | 存款+理财+基金+保险市值之和 | 跨域聚合 |
| 理财规模 | SUM(market_value) | SAT_WEALTH_HOLDING |
| 理财渗透率 | 持有理财客户数 / 总客户数 | HUB_CUSTOMER |
| 人均持仓 | 理财规模 / 持有客户数 | SAT_WEALTH_HOLDING |
| 赎回率 | 当期赎回金额 / 期初持仓市值 | SAT_WEALTH_TRANSACTION |

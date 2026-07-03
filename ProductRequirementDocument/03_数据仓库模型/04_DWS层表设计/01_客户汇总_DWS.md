# 01 客户汇总 DWS 层表设计

> 所属：03_数据仓库模型 → DWS层表设计
> 上一文档：[13_财务域_DWD](../03_DWD层表设计/13_财务域_DWD.md)
> 下一文档：[02_存款汇总_DWS](./02_存款汇总_DWS.md)

---

## 1 维度表

### 1.1 dim_customer — 客户维度表

```sql
CREATE TABLE dim_customer (
    customer_key        BIGINT      COMMENT '客户维度代理键 (自增)',
    customer_hash_key   STRING      COMMENT '客户哈希主键 (对应DWD HUB)',
    customer_id         STRING      COMMENT '客户业务编号',
    cust_name           STRING      COMMENT '客户姓名',
    id_type             STRING      COMMENT '证件类型',
    id_no               STRING      COMMENT '证件号码(脱敏)',
    cust_type           STRING      COMMENT '客户类型: PERSONAL/CORPORATE',
    cust_level          STRING      COMMENT '客户等级',
    gender              STRING      COMMENT '性别',
    age_group           STRING      COMMENT '年龄段: 18-25/26-35/36-45/46-55/56+',
    province            STRING      COMMENT '省份',
    city                STRING      COMMENT '城市',
    open_date           DATE        COMMENT '开户日期',
    effective_date      DATE        COMMENT '维度生效日期',
    expiry_date         DATE        COMMENT '维度失效日期',
    is_current          BOOLEAN     COMMENT '是否当前记录',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '客户维度表 — SCD Type 2，每日快照'
PARTITIONED BY (dt STRING COMMENT '数据日期')
CLUSTERED BY (customer_key) INTO 16 BUCKETS
STORED AS ORC;
```

---

## 2 汇总事实表

### 2.1 dws_cust_daily_summary — 客户日汇总表

```sql
CREATE TABLE dws_cust_daily_summary (
    customer_hash_key   STRING      COMMENT '客户哈希主键',
    dt                  STRING      COMMENT '快照日期',
    -- 资产指标
    total_asset_amt     DECIMAL(18,2) COMMENT '总资产(AUM)',
    deposit_amt         DECIMAL(18,2) COMMENT '存款余额',
    loan_amt            DECIMAL(18,2) COMMENT '贷款余额',
    wealth_amt          DECIMAL(18,2) COMMENT '理财市值',
    -- 交易指标
    daily_txn_cnt       BIGINT      COMMENT '当日交易笔数',
    daily_txn_amt       DECIMAL(18,2) COMMENT '当日交易金额',
    daily_deposit_amt   DECIMAL(18,2) COMMENT '当日存入金额',
    daily_withdraw_amt  DECIMAL(18,2) COMMENT '当日支取金额',
    -- 账户指标
    account_cnt         INT         COMMENT '持有账户数',
    active_account_cnt  INT         COMMENT '活跃账户数(近30天有交易)',
    -- 产品持有
    product_holding_cnt INT         COMMENT '持有产品种类数',
    has_deposit         BOOLEAN     COMMENT '是否持有存款',
    has_loan            BOOLEAN     COMMENT '是否持有贷款',
    has_credit_card     BOOLEAN     COMMENT '是否持有信用卡',
    has_wealth          BOOLEAN     COMMENT '是否持有理财',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '客户日汇总事实表 — 一个客户一行'
PARTITIONED BY (dt STRING COMMENT '快照日期')
CLUSTERED BY (customer_hash_key) INTO 16 BUCKETS
STORED AS ORC;
```

### 2.2 dws_cust_acquisition — 客户获取汇总表

```sql
CREATE TABLE dws_cust_acquisition (
    dt                  STRING      COMMENT '统计日期',
    channel_hash_key    STRING      COMMENT '获客渠道',
    org_hash_key        STRING      COMMENT '开户机构',
    new_cust_cnt        BIGINT      COMMENT '新增客户数',
    new_cust_personal   BIGINT      COMMENT '新增个人客户数',
    new_cust_corp       BIGINT      COMMENT '新增对公客户数',
    closed_cust_cnt     BIGINT      COMMENT '销户客户数',
    net_growth_cnt      BIGINT      COMMENT '净增客户数',
    total_cust_cnt      BIGINT      COMMENT '累计客户数',
    churn_cust_cnt      BIGINT      COMMENT '流失客户数(近6月无交易)',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '客户获取汇总表'
PARTITIONED BY (dt STRING)
STORED AS ORC;
```

### 2.3 dws_cust_aum_distribution — 客户AUM分布表

```sql
CREATE TABLE dws_cust_aum_distribution (
    dt                  STRING      COMMENT '统计日期',
    aum_bucket          STRING      COMMENT 'AUM分层: <1万/1-10万/10-50万/50-100万/100-500万/500-1000万/>1000万',
    cust_cnt            BIGINT      COMMENT '客户数',
    total_aum           DECIMAL(18,2) COMMENT 'AUM合计',
    avg_aum             DECIMAL(18,2) COMMENT '人均AUM',
    aum_pct             DECIMAL(5,2) COMMENT 'AUM占比',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '客户AUM分层分布表'
PARTITIONED BY (dt STRING)
STORED AS ORC;
```

---

## 3 ETL 逻辑

```sql
-- 客户日汇总 DWS (从 DWD 层聚合)
INSERT OVERWRITE TABLE dws_cust_daily_summary PARTITION (dt='${yesterday}')
SELECT
    c.customer_hash_key,
    COALESCE(dep.deposit_amt, 0) +
    COALESCE(loan.loan_amt, 0) +
    COALESCE(wm.wealth_amt, 0) AS total_asset_amt,
    COALESCE(dep.deposit_amt, 0) AS deposit_amt,
    COALESCE(loan.loan_amt, 0) AS loan_amt,
    COALESCE(wm.wealth_amt, 0) AS wealth_amt,
    COALESCE(txn.txn_cnt, 0) AS daily_txn_cnt,
    COALESCE(txn.txn_amt, 0) AS daily_txn_amt,
    ...
FROM dwd_hub_customer c
LEFT JOIN (SELECT customer_hash_key, SUM(balance) AS deposit_amt
           FROM dwd_sat_account_bal bal
           JOIN dwd_link_cust_acct l ON bal.account_hash_key = l.account_hash_key
           WHERE bal.dt = '${yesterday}') dep ON c.customer_hash_key = dep.customer_hash_key
LEFT JOIN (...) loan ON ...
LEFT JOIN (...) wm ON ...
LEFT JOIN (SELECT from_account_hash_key, COUNT(*) AS txn_cnt, SUM(trans_amt) AS txn_amt
           FROM dwd_sat_transaction WHERE dt = '${yesterday}') txn ON ...
WHERE c.dt = '${yesterday}';
```

# 02 存款汇总 DWS 层表设计

> 所属：03_数据仓库模型 → DWS层表设计
> 上一文档：[01_客户汇总_DWS](./01_客户汇总_DWS.md)
> 下一文档：[03_贷款汇总_DWS](./03_贷款汇总_DWS.md)

---

## 1 汇总事实表

### 1.1 dws_dep_daily_bal — 存款日余额汇总表

```sql
CREATE TABLE dws_dep_daily_bal (
    dt                  STRING      COMMENT '快照日期',
    account_type        STRING      COMMENT '账户类型: DEMAND/TIME/NOTICE/CD/STRUCTURAL',
    currency            STRING      COMMENT '币种',
    org_hash_key        STRING      COMMENT '开户机构',
    product_hash_key    STRING      COMMENT '产品',
    -- 账户指标
    account_cnt         BIGINT      COMMENT '账户数',
    active_account_cnt  BIGINT      COMMENT '活跃账户数',
    -- 余额指标
    total_balance       DECIMAL(18,2) COMMENT '总余额',
    demand_balance      DECIMAL(18,2) COMMENT '活期余额',
    time_balance        DECIMAL(18,2) COMMENT '定期余额',
    notice_balance      DECIMAL(18,2) COMMENT '通知存款余额',
    cd_balance          DECIMAL(18,2) COMMENT '大额存单余额',
    -- 变动指标
    daily_deposit_amt   DECIMAL(18,2) COMMENT '当日存入金额',
    daily_withdraw_amt  DECIMAL(18,2) COMMENT '当日支取金额',
    net_flow_amt        DECIMAL(18,2) COMMENT '净流入',
    -- 利率指标
    avg_interest_rate   DECIMAL(9,6) COMMENT '加权平均利率',
    interest_expense    DECIMAL(18,2) COMMENT '当日利息支出',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '存款日余额汇总事实表'
PARTITIONED BY (dt STRING COMMENT '快照日期')
CLUSTERED BY (org_hash_key) INTO 16 BUCKETS
STORED AS ORC;
```

### 1.2 dws_dep_term_structure — 存款期限结构表

```sql
CREATE TABLE dws_dep_term_structure (
    dt                  STRING      COMMENT '快照日期',
    term_bucket         STRING      COMMENT '期限分档: 活期/3月内/3-6月/6-12月/1-3年/3-5年/5年以上',
    account_cnt         BIGINT      COMMENT '账户数',
    total_balance       DECIMAL(18,2) COMMENT '余额合计',
    balance_pct         DECIMAL(5,2) COMMENT '余额占比',
    avg_rate            DECIMAL(9,6) COMMENT '平均利率',
    mature_in_30d       DECIMAL(18,2) COMMENT '30天内到期金额',
    mature_in_90d       DECIMAL(18,2) COMMENT '90天内到期金额',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '存款期限结构分析表'
PARTITIONED BY (dt STRING)
STORED AS ORC;
```

### 1.3 dws_dep_customer_level — 客户存款分层表

```sql
CREATE TABLE dws_dep_customer_level (
    dt                  STRING      COMMENT '快照日期',
    deposit_bucket      STRING      COMMENT '存款分层: <1万/1-5万/5-20万/20-50万/50-100万/100-500万/>500万',
    cust_cnt            BIGINT      COMMENT '客户数',
    total_deposit       DECIMAL(18,2) COMMENT '存款余额合计',
    avg_deposit         DECIMAL(18,2) COMMENT '人均存款',
    cust_pct            DECIMAL(5,2) COMMENT '客户数占比',
    deposit_pct         DECIMAL(5,2) COMMENT '存款余额占比',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '客户存款分层分析表'
PARTITIONED BY (dt STRING)
STORED AS ORC;
```

---

## 2 加载逻辑

```sql
-- 存款日余额汇总 (从 DWD 表聚合)
INSERT OVERWRITE TABLE dws_dep_daily_bal PARTITION (dt='${yesterday}')
SELECT
    '${yesterday}' AS dt,
    sat_status.account_type,
    bal.currency,
    sat_status.org_hash_key,
    sat_status.product_hash_key,
    COUNT(DISTINCT bal.account_hash_key) AS account_cnt,
    COUNT(DISTINCT CASE WHEN txn.account_hash_key IS NOT NULL 
        THEN bal.account_hash_key END) AS active_account_cnt,
    SUM(bal.balance) AS total_balance,
    SUM(CASE WHEN sat_status.account_type = 'DEMAND' THEN bal.balance ELSE 0 END) AS demand_balance,
    SUM(CASE WHEN sat_status.account_type IN ('TIME_3M','TIME_6M','TIME_1Y','TIME_3Y','TIME_5Y') 
        THEN bal.balance ELSE 0 END) AS time_balance,
    ...
FROM dwd_sat_account_bal bal
JOIN dwd_sat_account_status sat_status 
    ON bal.account_hash_key = sat_status.account_hash_key
    AND sat_status.is_current = TRUE
    AND sat_status.dt = '${yesterday}'
LEFT JOIN (
    SELECT DISTINCT account_hash_key 
    FROM dwd_sat_transaction 
    WHERE dt = '${yesterday}'
) txn ON bal.account_hash_key = txn.account_hash_key
WHERE bal.dt = '${yesterday}'
GROUP BY sat_status.account_type, bal.currency, 
         sat_status.org_hash_key, sat_status.product_hash_key;
```

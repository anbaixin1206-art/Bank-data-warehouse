# 05 自助BI ADS 层表设计

> 所属：03_数据仓库模型 → ADS层表设计
> 上一文档：[04_风险管理_ADS](./04_风险管理_ADS.md)
> 下一文档：[04_实时数据处理](../04_实时数据处理/01_实时架构设计.md)

---

## 1 ads_bi_* 数据集市宽表

自助BI提供**按分析主题预聚合的大宽表**，BI工具（Tableau/SuperSet/Metabase）直接消费。

---

## 1.1 ads_bi_deposit_wide — 存款分析宽表

```sql
CREATE TABLE ads_bi_deposit_wide (
    dt                  STRING      COMMENT '统计日期',
    account_no          STRING      COMMENT '账号(脱敏)',
    account_type        STRING      COMMENT '账户类型',
    currency            STRING      COMMENT '币种',
    balance             DECIMAL(18,2) COMMENT '余额',
    avail_balance       DECIMAL(18,2) COMMENT '可用余额',
    interest_rate       DECIMAL(9,6) COMMENT '利率',
    open_date           DATE        COMMENT '开户日期',
    term_months         INT         COMMENT '期限(月)',
    maturity_date       DATE        COMMENT '到期日',
    status              STRING      COMMENT '账户状态',
    -- 关联维度(已JOIN)
    cust_name           STRING      COMMENT '客户姓名(脱敏)',
    cust_type           STRING      COMMENT '客户类型',
    cust_level          STRING      COMMENT '客户等级',
    province            STRING      COMMENT '省份',
    org_name            STRING      COMMENT '开户机构',
    product_name        STRING      COMMENT '产品名称',
    -- 日变动
    prev_day_balance    DECIMAL(18,2) COMMENT '上日余额',
    daily_change        DECIMAL(18,2) COMMENT '日变动',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '存款分析宽表 — 自助BI数据集'
PARTITIONED BY (dt STRING)
STORED AS ORC;
```

---

## 1.2 ads_bi_loan_wide — 贷款分析宽表

```sql
CREATE TABLE ads_bi_loan_wide (
    dt                  STRING      COMMENT '统计日期',
    contract_no         STRING      COMMENT '合同号',
    loan_type           STRING      COMMENT '贷款类型',
    loan_amt            DECIMAL(18,2) COMMENT '合同金额',
    outstanding_principal DECIMAL(18,2) COMMENT '剩余本金',
    interest_rate       DECIMAL(9,6) COMMENT '利率',
    repay_method        STRING      COMMENT '还款方式',
    sign_date           DATE        COMMENT '签订日期',
    start_date          DATE        COMMENT '起期',
    end_date            DATE        COMMENT '止期',
    guarantee_type      STRING      COMMENT '担保方式',
    class_level         STRING      COMMENT '五级分类',
    overdue_days        INT         COMMENT '逾期天数',
    -- 关联维度
    cust_name           STRING      COMMENT '借款人',
    cust_type           STRING      COMMENT '客户类型',
    org_name            STRING      COMMENT '经办机构',
    product_name        STRING      COMMENT '产品名称',
    -- 累计还款
    total_principal_paid DECIMAL(18,2) COMMENT '累计已还本金',
    total_interest_paid DECIMAL(18,2) COMMENT '累计已还利息',
    next_due_date       DATE        COMMENT '下期还款日',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '贷款分析宽表 — 自助BI数据集'
PARTITIONED BY (dt STRING)
STORED AS ORC;
```

---

## 1.3 ads_bi_channel_wide — 渠道分析宽表

```sql
CREATE TABLE ads_bi_channel_wide (
    dt                  STRING      COMMENT '统计日期',
    channel_name        STRING      COMMENT '渠道名称',
    channel_type        STRING      COMMENT '渠道类型',
    trans_type          STRING      COMMENT '交易类型',
    hour                INT         COMMENT '小时',
    txn_cnt             BIGINT      COMMENT '交易笔数',
    txn_amt             DECIMAL(18,2) COMMENT '交易金额',
    success_cnt         BIGINT      COMMENT '成功笔数',
    fail_cnt            BIGINT      COMMENT '失败笔数',
    avg_response_ms     DECIMAL(10,2) COMMENT '平均响应时间',
    cust_cnt            BIGINT      COMMENT '使用客户数',
    new_cust_cnt        BIGINT      COMMENT '首次使用客户数',
    fee_income          DECIMAL(18,2) COMMENT '手续费收入',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '渠道分析宽表 — 自助BI数据集'
PARTITIONED BY (dt STRING)
STORED AS ORC;
```

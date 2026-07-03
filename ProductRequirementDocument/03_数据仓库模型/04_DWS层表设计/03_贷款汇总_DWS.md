# 03 贷款汇总 DWS 层表设计

> 所属：03_数据仓库模型 → DWS层表设计
> 上一文档：[02_存款汇总_DWS](./02_存款汇总_DWS.md)
> 下一文档：[04_支付汇总_DWS](./04_支付汇总_DWS.md)

---

## 1 汇总事实表

### 1.1 dws_loan_daily_bal — 贷款日余额汇总表

```sql
CREATE TABLE dws_loan_daily_bal (
    dt                  STRING      COMMENT '快照日期',
    loan_type           STRING      COMMENT '贷款类型: MORTGAGE/CONSUMER/BIZ_LOAN/CORP_WORKING/SYNDICATED',
    org_hash_key        STRING      COMMENT '经办机构',
    product_hash_key    STRING      COMMENT '产品',
    currency            STRING      COMMENT '币种',
    -- 合同指标
    contract_cnt        BIGINT      COMMENT '合同数',
    customer_cnt        BIGINT      COMMENT '借款客户数',
    -- 余额指标
    total_principal     DECIMAL(18,2) COMMENT '贷款余额(本金)',
    new_drawdown_amt    DECIMAL(18,2) COMMENT '当日新发放金额',
    repay_principal_amt DECIMAL(18,2) COMMENT '当日偿还本金',
    repay_interest_amt  DECIMAL(18,2) COMMENT '当日偿还利息',
    net_loan_growth     DECIMAL(18,2) COMMENT '净增贷款',
    -- 利率指标
    avg_interest_rate   DECIMAL(9,6) COMMENT '加权平均利率',
    interest_income     DECIMAL(18,2) COMMENT '当日利息收入',
    -- 逾期指标
    overdue_principal   DECIMAL(18,2) COMMENT '逾期本金',
    overdue_interest    DECIMAL(18,2) COMMENT '逾期利息',
    overdue_contract_cnt BIGINT     COMMENT '逾期合同数',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '贷款日余额汇总事实表'
PARTITIONED BY (dt STRING COMMENT '快照日期')
CLUSTERED BY (org_hash_key) INTO 16 BUCKETS
STORED AS ORC;
```

### 1.2 dws_loan_repay_schedule — 贷款还款计划汇总表

```sql
CREATE TABLE dws_loan_repay_schedule (
    dt                  STRING      COMMENT '统计日期',
    schedule_month       STRING      COMMENT '应还月份 YYYY-MM',
    loan_type           STRING      COMMENT '贷款类型',
    -- 应还金额
    schedule_principal  DECIMAL(18,2) COMMENT '应还本金',
    schedule_interest   DECIMAL(18,2) COMMENT '应还利息',
    actual_principal    DECIMAL(18,2) COMMENT '实还本金',
    actual_interest     DECIMAL(18,2) COMMENT '实还利息',
    -- 还款率
    principal_repay_rate DECIMAL(5,2) COMMENT '本金回收率 %',
    interest_repay_rate  DECIMAL(5,2) COMMENT '利息回收率 %',
    prepay_amt          DECIMAL(18,2) COMMENT '提前还款金额',
    overdue_amt         DECIMAL(18,2) COMMENT '逾期未还金额',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '贷款还款计划汇总表 — 按应还月份聚合'
PARTITIONED BY (dt STRING)
STORED AS ORC;
```

### 1.3 dws_loan_classification_summary — 五级分类汇总表

```sql
CREATE TABLE dws_loan_classification_summary (
    dt                  STRING      COMMENT '快照日期',
    class_level         STRING      COMMENT '五级分类: NORMAL/SPECIAL_MENTION/SUBSTANDARD/DOUBTFUL/LOSS',
    loan_type           STRING      COMMENT '贷款类型',
    contract_cnt        BIGINT      COMMENT '合同数',
    principal_amt       DECIMAL(18,2) COMMENT '分类余额',
    provision_amt       DECIMAL(18,2) COMMENT '拨备金额',
    provision_ratio     DECIMAL(5,2) COMMENT '拨备率',
    npl_ratio           DECIMAL(5,2) COMMENT '不良率(SUBSTANDARD+DOUBTFUL+LOSS)/总余额',
    overdue_avg_days    DECIMAL(8,2) COMMENT '平均逾期天数',
    migration_from      STRING      COMMENT '分类迁徙来源(JSON)',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '贷款五级分类汇总表'
PARTITIONED BY (dt STRING)
STORED AS ORC;
```

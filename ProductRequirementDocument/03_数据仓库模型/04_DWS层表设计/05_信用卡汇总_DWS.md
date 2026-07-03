# 05 信用卡汇总 DWS 层表设计

> 所属：03_数据仓库模型 → DWS层表设计
> 上一文档：[04_支付汇总_DWS](./04_支付汇总_DWS.md)
> 下一文档：[06_理财汇总_DWS](./06_理财汇总_DWS.md)

---

## 1 dws_cc_daily_summary — 信用卡日汇总表

```sql
CREATE TABLE dws_cc_daily_summary (
    dt                  STRING      COMMENT '统计日期',
    card_type           STRING      COMMENT '卡片类型',
    card_level          STRING      COMMENT '卡等级',
    org_hash_key        STRING      COMMENT '发卡机构',
    -- 卡片指标
    total_card_cnt      BIGINT      COMMENT '累计发卡量',
    active_card_cnt     BIGINT      COMMENT '活跃卡数(近3月有交易)',
    new_card_cnt        BIGINT      COMMENT '当日新发卡',
    closed_card_cnt     BIGINT      COMMENT '当日销卡',
    -- 交易指标
    consume_cnt         BIGINT      COMMENT '消费笔数',
    consume_amt         DECIMAL(18,2) COMMENT '消费金额',
    cash_advance_cnt    BIGINT      COMMENT '取现笔数',
    cash_advance_amt    DECIMAL(18,2) COMMENT '取现金额',
    installment_amt     DECIMAL(18,2) COMMENT '分期金额',
    overseas_amt        DECIMAL(18,2) COMMENT '境外消费金额',
    -- 信贷指标
    total_credit_limit  DECIMAL(18,2) COMMENT '总授信额度',
    used_credit_amt     DECIMAL(18,2) COMMENT '已用额度',
    utilization_rate    DECIMAL(5,2) COMMENT '额度使用率 %',
    -- 还款指标
    total_repay_amt     DECIMAL(18,2) COMMENT '还款金额',
    overdue_card_cnt    BIGINT      COMMENT '逾期卡数',
    overdue_bal         DECIMAL(18,2) COMMENT '逾期余额',
    overdue_90d_bal     DECIMAL(18,2) COMMENT '逾期90天+余额',
    -- 收入指标
    fee_income          DECIMAL(18,2) COMMENT '手续费收入',
    interest_income     DECIMAL(18,2) COMMENT '利息收入(透支利息+分期手续费)',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '信用卡日汇总事实表'
PARTITIONED BY (dt STRING COMMENT '统计日期')
CLUSTERED BY (org_hash_key) INTO 16 BUCKETS
STORED AS ORC;
```

---

## 2 dws_cc_merchant_analysis — 商户消费分析表

```sql
CREATE TABLE dws_cc_merchant_analysis (
    dt                  STRING      COMMENT '统计日期',
    mcc_code            STRING      COMMENT 'MCC商户类别码',
    mcc_category        STRING      COMMENT 'MCC大类: 餐饮/零售/交通/教育/医疗/娱乐/房产/其他',
    consume_cnt         BIGINT      COMMENT '消费笔数',
    consume_amt         DECIMAL(18,2) COMMENT '消费金额',
    avg_consume_amt     DECIMAL(18,2) COMMENT '笔均消费金额',
    card_cnt            BIGINT      COMMENT '消费卡数',
    overseas_cnt        BIGINT      COMMENT '境外消费笔数',
    installment_cnt     BIGINT      COMMENT '分期交易笔数',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '信用卡商户消费分析表'
PARTITIONED BY (dt STRING)
STORED AS ORC;
```

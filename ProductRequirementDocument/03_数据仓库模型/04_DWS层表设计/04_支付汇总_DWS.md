# 04 支付汇总 DWS 层表设计

> 所属：03_数据仓库模型 → DWS层表设计
> 上一文档：[03_贷款汇总_DWS](./03_贷款汇总_DWS.md)
> 下一文档：[05_信用卡汇总_DWS](./05_信用卡汇总_DWS.md)

---

## 1 dws_pay_daily_channel — 支付渠道日汇总表

```sql
CREATE TABLE dws_pay_daily_channel (
    dt                  STRING      COMMENT '统计日期',
    channel_hash_key    STRING      COMMENT '渠道',
    trans_type          STRING      COMMENT '交易类型: INTERNAL/CROSS_BANK/QUICK_PAY/AGENT',
    -- 交易量指标
    txn_cnt             BIGINT      COMMENT '交易笔数',
    txn_amt             DECIMAL(18,2) COMMENT '交易金额',
    avg_txn_amt         DECIMAL(18,2) COMMENT '笔均金额',
    -- 成功率
    success_cnt         BIGINT      COMMENT '成功笔数',
    success_rate         DECIMAL(5,2) COMMENT '成功率 %',
    -- 时效
    avg_response_ms     DECIMAL(10,2) COMMENT '平均响应时间(ms)',
    p99_response_ms     DECIMAL(10,2) COMMENT 'P99响应时间(ms)',
    -- 跨行指标
    cross_bank_cnt      BIGINT      COMMENT '跨行笔数',
    cross_bank_amt      DECIMAL(18,2) COMMENT '跨行金额',
    fee_income          DECIMAL(18,2) COMMENT '手续费收入',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '支付渠道日汇总事实表'
PARTITIONED BY (dt STRING COMMENT '统计日期')
CLUSTERED BY (channel_hash_key) INTO 16 BUCKETS
STORED AS ORC;
```

---

## 2 dws_pay_settlement_daily — 清算日汇总表

```sql
CREATE TABLE dws_pay_settlement_daily (
    dt                  STRING      COMMENT '统计日期',
    settle_channel      STRING      COMMENT '清算渠道: HVPS/BEPS/IBPS/UNIONPAY/ALIPAY/WECHAT',
    -- 清算量
    total_txn_cnt       BIGINT      COMMENT '总笔数',
    total_txn_amt       DECIMAL(18,2) COMMENT '总金额',
    -- 对账结果
    recon_cnt           BIGINT      COMMENT '对账笔数',
    balanced_cnt        BIGINT      COMMENT '平账笔数',
    long_cnt            BIGINT      COMMENT '长款笔数(核心有、支付无)',
    short_cnt           BIGINT      COMMENT '短款笔数(支付有、核心无)',
    diff_cnt            BIGINT      COMMENT '金额不符笔数',
    recon_balanced_rate DECIMAL(5,2) COMMENT '对账平账率 %',
    -- 头寸
    net_position        DECIMAL(18,2) COMMENT '净头寸',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '支付清算日汇总表'
PARTITIONED BY (dt STRING)
STORED AS ORC;
```

---

## 3 dws_pay_hourly_trend — 支付小时趋势表

```sql
CREATE TABLE dws_pay_hourly_trend (
    dt                  STRING      COMMENT '统计日期',
    hour                INT         COMMENT '小时 0-23',
    trans_type          STRING      COMMENT '交易类型',
    txn_cnt             BIGINT      COMMENT '交易笔数',
    txn_amt             DECIMAL(18,2) COMMENT '交易金额',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '支付小时趋势汇总表 — 用于交易趋势分析'
PARTITIONED BY (dt STRING)
STORED AS ORC;
```

---

## 4 加载逻辑（渠道日汇总）

```sql
INSERT OVERWRITE TABLE dws_pay_daily_channel PARTITION (dt='${yesterday}')
SELECT
    '${yesterday}' AS dt,
    l.channel_hash_key,
    s.trans_type,
    COUNT(1) AS txn_cnt,
    SUM(s.trans_amt) AS txn_amt,
    AVG(s.trans_amt) AS avg_txn_amt,
    SUM(CASE WHEN s.trans_status = 'SUCCESS' THEN 1 ELSE 0 END) AS success_cnt,
    ROUND(SUM(CASE WHEN s.trans_status = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / COUNT(1), 2) AS success_rate,
    AVG(unix_timestamp(s.settlement_time) - unix_timestamp(s.trans_time)) * 1000 AS avg_response_ms,
    SUM(CASE WHEN s.is_cross_bank THEN 1 ELSE 0 END) AS cross_bank_cnt,
    SUM(CASE WHEN s.is_cross_bank THEN s.trans_amt ELSE 0 END) AS cross_bank_amt,
    ...
FROM dwd_sat_transaction s
JOIN dwd_link_transaction l ON s.transaction_hash_key = l.transaction_hash_key
WHERE s.dt = '${yesterday}'
GROUP BY l.channel_hash_key, s.trans_type;
```

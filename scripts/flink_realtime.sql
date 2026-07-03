-- ============================================================
-- Flink SQL 实时处理 — Kafka Source → 聚合/风控 → Print/MySQL
-- 提交方式: sql-client.sh -f flink_realtime.sql
-- ============================================================

-- 1. Kafka Source: 核心交易流水
CREATE TABLE IF NOT EXISTS txn_stream (
    trans_id      STRING,
    account_no    STRING,
    trans_type    STRING,
    trans_amt     DECIMAL(18,2),
    dr_cr_flag    STRING,
    trans_time    STRING,
    channel       STRING,
    opp_account   STRING,
    memo          STRING,
    producer_time STRING,
    -- 提取事件时间
    txn_ts AS TO_TIMESTAMP(trans_time, 'yyyy-MM-dd HH:mm:ss'),
    WATERMARK FOR txn_ts AS txn_ts - INTERVAL '10' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'bank.ods.core.t_transaction',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id' = 'flink-bank-realtime',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

-- 2. 实时聚合：每 10 秒窗口 — 交易笔数/金额（用于大屏）
SELECT
    TUMBLE_START(txn_ts, INTERVAL '10' SECOND) AS window_start,
    channel,
    COUNT(1) AS txn_cnt,
    SUM(trans_amt) AS txn_amt,
    COUNT(DISTINCT account_no) AS active_accounts
FROM txn_stream
GROUP BY TUMBLE(txn_ts, INTERVAL '10' SECOND), channel;

-- 3. 大额交易实时监控（>50万 即时告警）
SELECT
    trans_id,
    trans_time,
    account_no,
    trans_amt,
    channel,
    'LARGE_AMOUNT' AS alert_type,
    CASE
        WHEN trans_amt > 1000000 THEN 'HIGH'
        WHEN trans_amt > 500000 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS risk_level
FROM txn_stream
WHERE trans_amt > 500000;

-- 4. 渠道成功率监控（5分钟窗口）
-- SELECT
--     TUMBLE_START(txn_ts, INTERVAL '5' MINUTE) AS window_start,
--     channel,
--     COUNT(1) AS total_cnt,
--     COUNT(1) FILTER (WHERE trans_type NOT IN ('REVERSAL')) AS success_cnt
-- FROM txn_stream
-- GROUP BY TUMBLE(txn_ts, INTERVAL '5' MINUTE), channel;

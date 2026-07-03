# 02 Flink 作业设计

> 所属：04_实时数据处理
> 上一文档：[01_实时架构设计](./01_实时架构设计.md)
> 下一文档：[03_实时指标与告警设计](./03_实时指标与告警设计.md)

---

## 1 Flink CEP 风控规则引擎

### 1.1 规则清单

| 规则ID | 规则名称 | 检测模式 | 窗口 | 动作 |
|--------|---------|---------|------|------|
| `R001` | 大额交易 | 单笔 > 50万 | 实时 | 告警+人工审核 |
| `R002` | 频繁交易 | 同账户1min内>5笔 | 1min | 告警 |
| `R003` | 夜间异常 | 22:00-06:00 交易>5万 | 实时 | 阻断+告警 |
| `R004` | 快进快出 | 转入后10min转出≥90% | 10min | 告警 |
| `R005` | 分散转入集中转出 | 多→单→快速转出 | 30min | 告警+可疑上报 |
| `R006` | POS套现 | 大额整数+高频 | 1h | 告警 |
| `R007` | 地域异常 | 异地+境外消费 | 实时 | 告警 |

### 1.2 CEP规则示例

```java
// R004: 快进快出
Pattern<Transaction, ?> quickInOut = Pattern
    .<Transaction>begin("in")
    .where(tx -> tx.getDirection() == Direction.IN)
    .next("out")
    .where(tx -> tx.getDirection() == Direction.OUT
        && tx.getAmount().compareTo(inAmount.multiply(0.9)) >= 0)
    .within(Time.minutes(10));

// R001 + R003: 大额+夜间组合
Pattern<Transaction, ?> largeNight = Pattern
    .<Transaction>begin("large")
    .where(tx -> tx.getAmount().compareTo(new BigDecimal("500000")) >= 0)
    .next("night")
    .where(tx -> {
        LocalTime t = tx.getTransTime().toLocalTime();
        return t.isAfter(LocalTime.of(22,0)) || 
               t.isBefore(LocalTime.of(6,0));
    })
    .within(Time.minutes(30));
```

---

## 2 Flink SQL 实时对账作业

### 2.1 支付-核心双流对账

```sql
-- 创建Kafka源表
CREATE TABLE pay_flow (
    trans_id    STRING,
    amount      DECIMAL(18,2),
    status      STRING,
    trans_time  TIMESTAMP(3),
    WATERMARK FOR trans_time AS trans_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'bank.ods.pay.t_payment_flow',
    'properties.bootstrap.servers' = 'localhost:9092',
    'format' = 'json'
);

CREATE TABLE core_flow (
    trans_id    STRING,
    amount      DECIMAL(18,2),
    status      STRING,
    trans_time  TIMESTAMP(3),
    WATERMARK FOR trans_time AS trans_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'bank.ods.core.t_transaction',
    'properties.bootstrap.servers' = 'localhost:9092',
    'format' = 'json'
);

-- 对账结果写入MySQL
CREATE TABLE recon_result (
    trans_id    STRING,
    recon_status STRING,
    pay_amount  DECIMAL(18,2),
    core_amount DECIMAL(18,2),
    PRIMARY KEY (trans_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://localhost:3306/recon_db',
    'table-name' = 't_recon_result',
    'username' = 'root',
    'password' = 'root123'
);

-- 双流FULL OUTER JOIN对账
INSERT INTO recon_result
SELECT 
    COALESCE(p.trans_id, c.trans_id) AS trans_id,
    CASE 
        WHEN p.trans_id IS NULL THEN 'LONG'       -- 长款
        WHEN c.trans_id IS NULL THEN 'SHORT'      -- 短款
        WHEN p.amount <> c.amount THEN 'DIFF'     -- 金额不符
        ELSE 'BALANCED'
    END AS recon_status,
    p.amount AS pay_amount,
    c.amount AS core_amount
FROM pay_flow p
FULL OUTER JOIN core_flow c 
    ON p.trans_id = c.trans_id
WHERE p.amount <> c.amount 
   OR p.trans_id IS NULL 
   OR c.trans_id IS NULL;
```

---

## 3 Flink SQL 实时大屏指标

```sql
-- 5秒滚动窗口聚合
SELECT 
    TUMBLE_START(trans_time, INTERVAL '5' SECOND) AS window_start,
    channel,
    COUNT(1) AS txn_cnt,
    SUM(amount) AS txn_amt,
    COUNT(DISTINCT account_no) AS active_accounts
FROM pay_flow
GROUP BY TUMBLE(trans_time, INTERVAL '5' SECOND), channel;
```

输出: Redis (key=`screen:txn:latest`, TTL=10s, 前端WebSocket拉取)

---

## 4 作业部署

| 作业名称 | 模式 | 并行度 | Checkpoint |
|---------|------|--------|-----------|
| `flink-cep-risk` | Streaming | 8 | 60s, EXACTLY_ONCE |
| `flink-sql-recon` | Streaming | 4 | 120s, AT_LEAST_ONCE |
| `flink-sql-screen` | Streaming | 4 | 120s, AT_LEAST_ONCE |

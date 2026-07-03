# 07 支付域 DWD 层表设计

> 所属：03_数据仓库模型 → DWD层表设计
> 上一文档：[06_贷款域_DWD](./06_贷款域_DWD.md)
> 下一文档：[08_信用卡域_DWD](./08_信用卡域_DWD.md)

---

## 1 支付域模型概览

支付域的核心是**交易统一模型**——所有资金流动（存款交易、支付转账、信用卡消费、理财产品交易）统一建模。

```
┌──────────────────────────────────────────────────────────────────┐
│                    支付域/交易统一模型                              │
│                                                                  │
│  ┌──────────────────┐                                            │
│  │ HUB_TRANSACTION  │                                            │
│  │ transaction_hash │                                            │
│  │ trans_id         │                                            │
│  └────────┬─────────┘                                            │
│           │                                                      │
│           ├── SAT_TRANSACTION (交易事实卫星 - 追加型)              │
│           │   trans_amt, trans_type, dr_cr_flag,                 │
│           │   trans_time, trans_date, currency                   │
│           │                                                      │
│           └── LINK_TRANSACTION (交易参与方)                        │
│               ├── from_account_hash_key (转出方)                  │
│               ├── to_account_hash_key (转入方)                    │
│               ├── channel_hash_key (渠道)                         │
│               ├── teller_hash_key (柜员, 可选)                    │
│               └── opp_system (对手系统, 跨行场景)                  │
└──────────────────────────────────────────────────────────────────┘
```

---

## 2 Hub 表

### 2.1 dwd_hub_transaction — 交易中心表

```sql
CREATE TABLE dwd_hub_transaction (
    transaction_hash_key STRING     COMMENT '交易哈希主键',
    trans_id             STRING     COMMENT '交易流水号(源系统)',
    load_date            DATE       COMMENT '首次加载日期',
    record_source        STRING     COMMENT '首次来源系统',
    etl_time             TIMESTAMP  COMMENT 'ETL处理时间'
)
COMMENT '交易中心表 (Hub) — 所有资金交易统一标识'
PARTITIONED BY (dt STRING COMMENT '数据日期')
CLUSTERED BY (transaction_hash_key) INTO 32 BUCKETS
STORED AS ORC
TBLPROPERTIES ('orc.compress'='ZLIB');
```

---

## 3 Satellite 表

### 3.1 dwd_sat_transaction — 交易事实卫星表

```sql
CREATE TABLE dwd_sat_transaction (
    transaction_hash_key    STRING      COMMENT '交易哈希主键',
    load_date               DATE        COMMENT '加载日期',
    trans_date              DATE        COMMENT '交易日期',
    trans_time              TIMESTAMP   COMMENT '交易时间',
    trans_type              STRING      COMMENT '交易类型: DEPOSIT/WITHDRAW/TRANSFER/PAYMENT/CONSUME/...',
    trans_subtype           STRING      COMMENT '交易子类型',
    trans_amt               DECIMAL(18,2) COMMENT '交易金额',
    currency                STRING      COMMENT '币种: CNY/USD/...',
    dr_cr_flag              STRING      COMMENT '借贷标志: D(借-支出)/C(贷-收入)',
    from_account_hash_key   STRING      COMMENT '转出账户哈希主键',
    to_account_hash_key     STRING      COMMENT '转入账户哈希主键',
    channel_hash_key        STRING      COMMENT '渠道哈希主键 (FK→HUB_CHANNEL)',
    teller_hash_key         STRING      COMMENT '柜员哈希主键 (FK→HUB_EMPLOYEE, 柜面渠道时)',
    opp_account_no          STRING      COMMENT '对手账号(跨行场景)',
    opp_bank_code           STRING      COMMENT '对手行联行号',
    memo                    STRING      COMMENT '交易附言/摘要',
    trans_status            STRING      COMMENT '交易状态: SUCCESS/FAILED/PENDING/REVERSED',
    resp_code               STRING      COMMENT '交易响应码',
    is_cross_bank           BOOLEAN     COMMENT '是否跨行交易',
    settlement_status       STRING      COMMENT '清算状态: SETTLED/PENDING/FAILED',
    record_source           STRING      COMMENT '数据来源: CORE/PAY/EBANK/CC/ATM',
    etl_time                TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '交易事实卫星表 (Satellite) — 统一交易明细，直接追加'
PARTITIONED BY (dt STRING COMMENT '交易日期')
CLUSTERED BY (transaction_hash_key) INTO 32 BUCKETS
STORED AS ORC
TBLPROPERTIES ('orc.compress'='ZLIB');
```

---

## 4 Link 表

### 4.1 dwd_link_transaction — 交易参与方链接表

```sql
CREATE TABLE dwd_link_transaction (
    transaction_link_hash_key STRING  COMMENT '交易链接哈希主键',
    transaction_hash_key    STRING    COMMENT '交易哈希主键 (FK→HUB_TRANSACTION)',
    from_account_hash_key   STRING    COMMENT '转出方账户哈希主键',
    to_account_hash_key     STRING    COMMENT '转入方账户哈希主键',
    channel_hash_key        STRING    COMMENT '渠道哈希主键 (FK→HUB_CHANNEL)',
    teller_hash_key         STRING    COMMENT '柜员哈希主键 (FK→HUB_EMPLOYEE)',
    opp_system              STRING    COMMENT '对手系统: INTERNAL/HVPS/BEPS/IBPS/UNIONPAY/ALIPAY',
    load_date               DATE      COMMENT '加载日期',
    record_source           STRING    COMMENT '数据来源',
    etl_time                TIMESTAMP COMMENT 'ETL处理时间'
)
COMMENT '交易参与方链接表 (Link) — 关联交易的各个参与方'
PARTITIONED BY (dt STRING COMMENT '数据日期')
CLUSTERED BY (transaction_link_hash_key) INTO 32 BUCKETS
STORED AS ORC
TBLPROPERTIES ('orc.compress'='ZLIB');
```

---

## 5 交易类型编码规范

| 编码 | 名称 | 业务域 | 借贷方向 |
|------|------|--------|---------|
| `DEPOSIT_CASH` | 现金存入 | 存款 | C(收入) |
| `WITHDRAW_CASH` | 现金支取 | 存款 | D(支出) |
| `TRANSFER_IN` | 转账存入 | 存款/支付 | C(收入) |
| `TRANSFER_OUT` | 转账支出 | 存款/支付 | D(支出) |
| `INTERNAL_TRANSFER` | 行内转账 | 支付 | D/C |
| `CROSS_BANK_OUT` | 跨行转出 | 支付 | D(支出) |
| `CROSS_BANK_IN` | 跨行转入 | 支付 | C(收入) |
| `QUICK_PAY` | 快捷支付 | 支付 | D(支出) |
| `AGENT_COLLECT` | 代收 | 支付 | C(收入) |
| `AGENT_PAY` | 代付 | 支付 | D(支出) |
| `CC_CONSUME` | 信用卡消费 | 信用卡 | D(支出) |
| `CC_REPAY` | 信用卡还款 | 信用卡 | C(收入) |
| `LOAN_DRAW` | 贷款放款 | 贷款 | C(收入) |
| `LOAN_REPAY` | 贷款还款 | 贷款 | D(支出) |
| `INT_SETTLE` | 结息 | 存款 | C(收入) |
| `INT_TAX` | 利息税 | 存款 | D(支出) |
| `FEE_COLLECT` | 手续费收取 | 存款/支付 | D(支出) |
| `REVERSAL` | 冲正 | 通用 | 反向 |

---

## 6 多源交易整合逻辑

支付域需要整合来自多个源系统的交易流水：

```sql
-- 统一交易事实视图
CREATE VIEW dwd_v_transaction_unified AS
-- 核心银行交易
SELECT 'CORE' AS source_system, TRANS_ID, TRANS_TYPE, TRANS_AMT, ...
FROM ods_core.t_transaction WHERE dt = '${yesterday}'
UNION ALL
-- 支付网关交易
SELECT 'PAY' AS source_system, pay_id, ...
FROM ods_pay.t_payment_flow WHERE dt = '${yesterday}'
UNION ALL
-- 网银/手机银行交易
SELECT 'EBANK' AS source_system, trans_id, ...
FROM ods_ebank.t_ebank_transaction WHERE dt = '${yesterday}'
UNION ALL
-- 信用卡消费交易
SELECT 'CC' AS source_system, trans_id, ...
FROM ods_cc.t_cc_transaction WHERE dt = '${yesterday}'
UNION ALL
-- ATM 交易
SELECT 'ATM' AS source_system, ...
FROM ods_atmpos.atm_transaction WHERE dt = '${yesterday}';
```

---

## 7 实时与离线协同

| 数据链路 | 来源 | 频率 | DWD 写入 |
|---------|------|------|---------|
| 核心银行交易 | Oracle (DataX增量) | 15min | 离线追加 |
| 支付网关交易 | MySQL (Canal→Kafka→Flink) | 实时 | 实时追加 |
| 网银交易 | MySQL (Canal→Kafka→Flink) | 实时 | 实时追加 |
| ATM/POS交易 | 文件 (Flume→Kafka→Flink) | 准实时 | 准实时追加 |

> 交易表是 Lambda 架构的核心交汇点——实时和离线写入同一张 `dwd_sat_transaction` 表的不同分区。Flink 实时写入 `dt='2026-06-15'` 分区（逐条追加），Spark T+1 补跑同日分区（覆盖确保完整）。

---

## 8 核心指标定义

| 指标 | 定义 | 计算来源 |
|------|------|---------|
| 交易笔数 | COUNT(DISTINCT trans_id) | SAT_TRANSACTION |
| 交易金额 | SUM(trans_amt) | SAT_TRANSACTION |
| 渠道分布 | 按 channel_hash_key 分组笔数/金额 | SAT_TRANSACTION + LINK_TRANSACTION |
| 跨行成功率 | SUCCESS笔数 / 总跨行笔数 | SAT_TRANSACTION (is_cross_bank=TRUE) |
| 交易时效 | trans_time 到 settlement_time 差值 | SAT_TRANSACTION |

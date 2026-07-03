# 05 存款域 DWD 层表设计

> 所属：03_数据仓库模型 → DWD层表设计
> 上一文档：[04_渠道域_DWD](./04_渠道域_DWD.md)
> 下一文档：[06_贷款域_DWD](./06_贷款域_DWD.md)

---

## 1 存款域模型概览

```
┌──────────────────────────────────────────────────────────────────┐
│                      存款域 DWD 模型                                │
│                                                                  │
│  ┌──────────────┐   ┌──────────────────┐   ┌──────────────┐      │
│  │ HUB_ACCOUNT  │◄──┤ LINK_CUST_ACCT   │──►│HUB_CUSTOMER  │      │
│  │ account_hash │   │ cust_acct_hash   │   │(已在客户域)   │      │
│  │ account_no   │   └──────────────────┘   └──────────────┘      │
│  └──────┬───────┘                                                │
│         │                                                       │
│         ├── SAT_ACCOUNT_BAL (账户余额快照 - 每日)                  │
│         │   balance, avail_balance, frozen_amt, currency,       │
│         │   last_update_time                                     │
│         │                                                       │
│         ├── SAT_ACCOUNT_STATUS (账户状态变更 - 拉链)               │
│         │   status, from_status, to_status, change_reason       │
│         │                                                       │
│         └── SAT_INTEREST (计息明细 - 事实型)                      │
│             interest_amt, base_amt, rate, int_date              │
│                                                                  │
│  ┌──────────────────┐                                           │
│  │HUB_TRANSACTION   │── SAT_TRANSACTION (存款交易明细 - 事实型)     │
│  │transaction_hash  │   trans_amt, trans_type, dr_cr_flag,     │
│  │trans_id          │   channel, teller_id, opp_account        │
│  └──────────────────┘                                           │
└──────────────────────────────────────────────────────────────────┘
```

---

## 2 Hub 表

### 2.1 dwd_hub_account — 账户中心表

```sql
CREATE TABLE dwd_hub_account (
    account_hash_key    STRING      COMMENT '账户哈希主键',
    account_no          STRING      COMMENT '账号',
    load_date           DATE        COMMENT '首次加载日期',
    record_source       STRING      COMMENT '首次来源系统',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '账户中心表 (Hub) — 全行所有存款账户'
PARTITIONED BY (dt STRING COMMENT '数据日期')
CLUSTERED BY (account_hash_key) INTO 32 BUCKETS
STORED AS ORC
TBLPROPERTIES ('orc.compress'='ZLIB');
```

---

## 3 Satellite 表

### 3.1 dwd_sat_account_bal — 账户余额快照卫星表

```sql
CREATE TABLE dwd_sat_account_bal (
    account_hash_key    STRING      COMMENT '账户哈希主键',
    load_date           DATE        COMMENT '快照日期',
    balance              DECIMAL(18,2) COMMENT '账户余额',
    avail_balance        DECIMAL(18,2) COMMENT '可用余额',
    frozen_amt           DECIMAL(18,2) COMMENT '冻结金额',
    last_interest_amt    DECIMAL(18,2) COMMENT '上次结息金额',
    currency             STRING      COMMENT '币种: CNY/USD/EUR/JPY/...',
    last_txn_time        TIMESTAMP   COMMENT '最近交易时间',
    is_overdraft         BOOLEAN     COMMENT '是否透支',
    overdraft_amt        DECIMAL(18,2) COMMENT '透支金额',
    record_source        STRING      COMMENT '数据来源',
    etl_time             TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '账户余额快照卫星表 (Satellite) — 每日余额快照，不做拉链'
PARTITIONED BY (dt STRING COMMENT '快照日期')
CLUSTERED BY (account_hash_key) INTO 32 BUCKETS
STORED AS ORC
TBLPROPERTIES ('orc.compress'='ZLIB');

-- 每日全量快照覆盖写入
INSERT OVERWRITE TABLE dwd_sat_account_bal PARTITION (dt='${yesterday}')
SELECT
    h.account_hash_key,
    '${yesterday}' AS load_date,
    CAST(o.BALANCE AS DECIMAL(18,2)),
    CAST(o.AVAIL_BALANCE AS DECIMAL(18,2)),
    CAST(o.FROZEN_AMT AS DECIMAL(18,2)),
    ...
FROM ods_core.t_account_balance o
JOIN dwd_hub_account h ON o.ACCOUNT_NO = h.account_no;
```

### 3.2 dwd_sat_account_status — 账户状态变更卫星表（拉链）

```sql
CREATE TABLE dwd_sat_account_status (
    account_hash_key    STRING      COMMENT '账户哈希主键',
    load_date           DATE        COMMENT '状态生效日期',
    load_end_date       DATE        COMMENT '状态失效日期',
    is_current          BOOLEAN     COMMENT '是否当前状态',
    account_type        STRING      COMMENT '账户类型: DEMAND/TIME/NOTICE/CD/STRUCTURAL',
    status              STRING      COMMENT '账户状态: NORMAL/FROZEN/CLOSED/DORMANT/PENDING_CLOSE',
    open_date           DATE        COMMENT '开户日期',
    close_date          DATE        COMMENT '销户日期',
    product_hash_key    STRING      COMMENT '产品哈希主键 (FK→HUB_PRODUCT)',
    org_hash_key        STRING      COMMENT '开户机构哈希主键 (FK→HUB_ORG)',
    record_source       STRING      COMMENT '数据来源',
    hash_diff           STRING      COMMENT '属性MD5差异值',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '账户状态卫星表 (Satellite) — SCD Type 2 拉链'
PARTITIONED BY (dt STRING COMMENT '数据日期')
CLUSTERED BY (account_hash_key) INTO 32 BUCKETS
STORED AS ORC
TBLPROPERTIES ('orc.compress'='ZLIB');
```

### 3.3 dwd_sat_interest — 计息明细卫星表（事实型）

```sql
CREATE TABLE dwd_sat_interest (
    account_hash_key    STRING      COMMENT '账户哈希主键',
    load_date           DATE        COMMENT '加载日期',
    int_date            DATE        COMMENT '计息日期',
    base_amt            DECIMAL(18,2) COMMENT '计息基数(积数)',
    rate                 DECIMAL(9,6) COMMENT '年利率',
    interest_amt         DECIMAL(18,2) COMMENT '利息金额',
    interest_tax_amt     DECIMAL(18,2) COMMENT '利息税',
    net_interest_amt     DECIMAL(18,2) COMMENT '实付利息',
    record_source        STRING      COMMENT '数据来源',
    etl_time             TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '计息明细卫星表 (Satellite) — 事实型，直接追加'
PARTITIONED BY (dt STRING COMMENT '结息日期')
CLUSTERED BY (account_hash_key) INTO 32 BUCKETS
STORED AS ORC
TBLPROPERTIES ('orc.compress'='ZLIB');
```

---

## 4 存款交易明细 — 复用交易域

存款交易明细（存款/取款/转账）统一存入 `dwd_sat_transaction` 和 `dwd_link_transaction`，详见 [支付域 DWD 表设计](./07_支付域_DWD.md) 中的交易统一模型。

---

## 5 核心指标定义

| 指标 | 定义 | 计算来源 |
|------|------|---------|
| 存款余额 | 所有账户 balance 之和 | SAT_ACCOUNT_BAL |
| 活期存款占比 | 活期余额 / 总存款余额 | 关联 SAT_ACCOUNT_STATUS (account_type) |
| 定期存款到期分布 | 按到期月份分组余额 | SAT_ACCOUNT_STATUS (到期日) |
| 日均存款 | 月内日余额平均值 | SAT_ACCOUNT_BAL 按月聚合 |
| 存款付息率 | 利息支出 / 日均存款 | SAT_INTEREST / SAT_ACCOUNT_BAL |
| 存贷比 | 贷款余额 / 存款余额 | 跨域指标 (贷款域+存款域) |

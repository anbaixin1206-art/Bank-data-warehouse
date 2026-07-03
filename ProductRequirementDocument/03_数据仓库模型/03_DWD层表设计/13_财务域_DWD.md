# 13 财务域 DWD 层表设计

> 所属：03_数据仓库模型 → DWD层表设计
> 上一文档：[12_监管域_DWD](./12_监管域_DWD.md)
> 下一文档：[DWS层表设计 - 客户汇总](../04_DWS层表设计/01_客户汇总_DWS.md)

---

## 1 财务域模型概览

```
┌──────────────────────────────────────────────────────────────┐
│                   财务域 DWD 模型                               │
│                                                              │
│  ┌──────────────────┐                                        │
│  │ HUB_GL_ACCOUNT   │── SAT_GL_ACCOUNT_INFO (科目属性拉链)     │
│  │ (会计科目)        │   account_code, account_name,          │
│  └──────────────────┘   account_type, parent_code, level     │
│                                                              │
│  ┌──────────────────┐                                        │
│  │ HUB_GL_VOUCHER   │── SAT_GL_VOUCHER (凭证头)               │
│  │ (记账凭证)        │                                        │
│  └──────┬───────────┘                                        │
│         │                                                    │
│         └── SAT_GL_ENTRY (凭证明细 - 事实型)                  │
│             dr_amt, cr_amt, account_code, summary            │
│                                                              │
│  ┌──────────────────┐                                        │
│  │ HUB_GL_BALANCE   │── SAT_GL_BALANCE (科目余额快照)          │
│  │ (科目余额)        │   open_bal, dr_amt, cr_amt, close_bal  │
│  └──────────────────┘                                        │
└──────────────────────────────────────────────────────────────┘
```

---

## 2 Hub 表

### 2.1 dwd_hub_gl_account — 会计科目中心表

```sql
CREATE TABLE dwd_hub_gl_account (
    gl_account_hash_key STRING      COMMENT '科目哈希主键',
    account_code        STRING      COMMENT '科目代码',
    load_date           DATE        COMMENT '首次加载日期',
    record_source       STRING      COMMENT '来源系统: GL',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '会计科目中心表 (Hub) — 全行会计科目'
PARTITIONED BY (dt STRING COMMENT '数据日期')
CLUSTERED BY (gl_account_hash_key) INTO 32 BUCKETS
STORED AS ORC;
```

### 2.2 dwd_hub_gl_voucher — 记账凭证中心表

```sql
CREATE TABLE dwd_hub_gl_voucher (
    voucher_hash_key    STRING      COMMENT '凭证哈希主键',
    voucher_id          STRING      COMMENT '凭证编号',
    load_date           DATE        COMMENT '首次加载日期',
    record_source       STRING      COMMENT '来源系统: GL',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '记账凭证中心表 (Hub)'
PARTITIONED BY (dt STRING COMMENT '数据日期')
CLUSTERED BY (voucher_hash_key) INTO 32 BUCKETS
STORED AS ORC;
```

---

## 3 Satellite 表

### 3.1 dwd_sat_gl_account_info — 科目属性卫星表

```sql
CREATE TABLE dwd_sat_gl_account_info (
    gl_account_hash_key STRING      COMMENT '科目哈希主键',
    load_date           DATE        COMMENT '生效日期',
    load_end_date       DATE        COMMENT '失效日期',
    is_current          BOOLEAN     COMMENT '是否当前',
    account_code        STRING      COMMENT '科目代码',
    account_name        STRING      COMMENT '科目名称',
    account_type        STRING      COMMENT '科目类型: ASSET/LIABILITY/EQUITY/INCOME/EXPENSE',
    parent_code         STRING      COMMENT '上级科目代码',
    account_level       INT         COMMENT '科目级别: 1-一级 2-二级 3-三级',
    dc_type             STRING      COMMENT '借贷方向: D-借方 C-贷方',
    is_leaf             BOOLEAN     COMMENT '是否叶子科目',
    balance_type        STRING      COMMENT '余额方向: DEBIT/CREDIT/BOTH',
    status              STRING      COMMENT '状态: ACTIVE/INACTIVE',
    record_source       STRING      COMMENT '数据来源',
    hash_diff           STRING      COMMENT '属性MD5',
    etl_time            TIMESTAMP   COMMENT 'ETL时间'
)
COMMENT '会计科目属性卫星表 (Satellite) — SCD Type 2'
PARTITIONED BY (dt STRING) CLUSTERED BY (gl_account_hash_key) INTO 32 BUCKETS STORED AS ORC;
```

### 3.2 dwd_sat_gl_voucher — 记账凭证头卫星表

```sql
CREATE TABLE dwd_sat_gl_voucher (
    voucher_hash_key    STRING      COMMENT '凭证哈希主键',
    load_date           DATE        COMMENT '加载日期',
    voucher_date        DATE        COMMENT '凭证日期',
    voucher_type        STRING      COMMENT '凭证类型: RECEIPT/PAYMENT/TRANSFER/JOURNAL',
    voucher_no          STRING      COMMENT '凭证号',
    maker_hash_key      STRING      COMMENT '制单人哈希',
    checker_hash_key    STRING      COMMENT '复核人哈希',
    total_dr_amt        DECIMAL(18,2) COMMENT '借方合计',
    total_cr_amt        DECIMAL(18,2) COMMENT '贷方合计',
    entry_count         INT         COMMENT '分录数',
    status              STRING      COMMENT '凭证状态: DRAFT/CHECKED/POSTED/REVERSED',
    record_source       STRING      COMMENT '数据来源',
    etl_time            TIMESTAMP   COMMENT 'ETL时间'
)
COMMENT '记账凭证头卫星表 (Satellite)'
PARTITIONED BY (dt STRING) CLUSTERED BY (voucher_hash_key) INTO 32 BUCKETS STORED AS ORC;
```

### 3.3 dwd_sat_gl_entry — 凭证明细卫星表（事实型）

```sql
CREATE TABLE dwd_sat_gl_entry (
    voucher_hash_key    STRING      COMMENT '凭证哈希主键',
    load_date           DATE        COMMENT '加载日期',
    entry_seq           INT         COMMENT '分录序号',
    gl_account_hash_key STRING      COMMENT '科目哈希主键',
    dr_amt              DECIMAL(18,2) COMMENT '借方金额',
    cr_amt              DECIMAL(18,2) COMMENT '贷方金额',
    currency            STRING      COMMENT '币种',
    summary             STRING      COMMENT '摘要',
    org_hash_key        STRING      COMMENT '核算机构哈希',
    product_hash_key    STRING      COMMENT '产品哈希(如有)',
    customer_hash_key   STRING      COMMENT '客户哈希(如有)',
    record_source       STRING      COMMENT '数据来源',
    etl_time            TIMESTAMP   COMMENT 'ETL时间'
)
COMMENT '凭证明细卫星表 (Satellite) — 事实型追加'
PARTITIONED BY (dt STRING COMMENT '凭证日期')
CLUSTERED BY (voucher_hash_key) INTO 32 BUCKETS
STORED AS ORC;
```

### 3.4 dwd_sat_gl_balance — 科目余额快照卫星表

```sql
CREATE TABLE dwd_sat_gl_balance (
    gl_account_hash_key STRING      COMMENT '科目哈希主键',
    load_date           DATE        COMMENT '快照日期',
    balance_date        DATE        COMMENT '余额日期',
    open_bal            DECIMAL(18,2) COMMENT '期初余额',
    dr_amt              DECIMAL(18,2) COMMENT '本期借方发生额',
    cr_amt              DECIMAL(18,2) COMMENT '本期贷方发生额',
    close_bal           DECIMAL(18,2) COMMENT '期末余额',
    dr_cum_amt          DECIMAL(18,2) COMMENT '借方累计发生额(年初至今)',
    cr_cum_amt          DECIMAL(18,2) COMMENT '贷方累计发生额(年初至今)',
    currency            STRING      COMMENT '币种',
    org_hash_key        STRING      COMMENT '核算机构',
    record_source       STRING      COMMENT '数据来源',
    etl_time            TIMESTAMP   COMMENT 'ETL时间'
)
COMMENT '科目余额快照卫星表 (Satellite) — 每日快照'
PARTITIONED BY (dt STRING COMMENT '快照日期')
CLUSTERED BY (gl_account_hash_key) INTO 32 BUCKETS
STORED AS ORC;
```

---

## 4 核心指标定义

| 指标 | 定义 | 计算来源 |
|------|------|---------|
| 总资产 | 资产类科目期末余额合计 | SAT_GL_BALANCE |
| 总负债 | 负债类科目期末余额合计 | SAT_GL_BALANCE |
| 净资产 | 总资产 - 总负债 | SAT_GL_BALANCE |
| 营业收入 | 收入类科目发生额合计 | SAT_GL_ENTRY |
| 营业支出 | 支出类科目发生额合计 | SAT_GL_ENTRY |
| 净利润 | 营业收入 - 营业支出 | 聚合计算 |
| ROA | 净利润 / 平均总资产 | 跨表计算 |
| ROE | 净利润 / 平均净资产 | 跨表计算 |

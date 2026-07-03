# 06 贷款域 DWD 层表设计

> 所属：03_数据仓库模型 → DWD层表设计
> 上一文档：[05_存款域_DWD](./05_存款域_DWD.md)
> 下一文档：[07_支付域_DWD](./07_支付域_DWD.md)

---

## 1 贷款域模型概览

```
┌────────────────────────────────────────────────────────────────┐
│                     贷款域 DWD 模型                               │
│                                                                │
│  ┌──────────────────┐   ┌─────────────────────┐                │
│  │ HUB_LOAN_CONTRACT│◄──┤ LINK_CUST_LOAN      │──►HUB_CUSTOMER │
│  │ contract_hash    │   │ cust_loan_hash      │                │
│  │ contract_no      │   │ role_type           │                │
│  └──────┬───────────┘   └─────────────────────┘                │
│         │                                                      │
│         ├── SAT_LOAN_CONTRACT_INFO (合同属性拉链)                │
│         │   loan_type, loan_amt, rate, term, repay_method     │
│         │                                                      │
│         └── LINK_LOAN_DRAWDOWN ──► SAT_LOAN_DRAWDOWN (放款)    │
│                │                          SAT_LOAN_REPAYMENT  │
│                │                          SAT_LOAN_CLASSIFY   │
│                ▼                                               │
│         HUB_ACCOUNT (收款账户)                                   │
└────────────────────────────────────────────────────────────────┘
```

---

## 2 Hub 表

### 2.1 dwd_hub_loan_contract — 贷款合同中心表

```sql
CREATE TABLE dwd_hub_loan_contract (
    contract_hash_key   STRING      COMMENT '贷款合同哈希主键',
    contract_no         STRING      COMMENT '贷款合同号',
    load_date           DATE        COMMENT '首次加载日期',
    record_source       STRING      COMMENT '首次来源系统',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '贷款合同中心表 (Hub) — 所有贷款合同'
PARTITIONED BY (dt STRING COMMENT '数据日期')
CLUSTERED BY (contract_hash_key) INTO 32 BUCKETS
STORED AS ORC
TBLPROPERTIES ('orc.compress'='ZLIB');
```

---

## 3 Satellite 表

### 3.1 dwd_sat_loan_contract_info — 贷款合同属性卫星表（拉链）

```sql
CREATE TABLE dwd_sat_loan_contract_info (
    contract_hash_key   STRING      COMMENT '合同哈希主键',
    load_date           DATE        COMMENT '记录生效日期',
    load_end_date       DATE        COMMENT '记录失效日期',
    is_current          BOOLEAN     COMMENT '是否当前记录',
    loan_type            STRING      COMMENT '贷款类型: MORTGAGE/CONSUMER/BIZ_LOAN/CORP_WORKING/SYNDICATED',
    product_hash_key    STRING      COMMENT '产品哈希主键 (FK→HUB_PRODUCT)',
    customer_hash_key   STRING      COMMENT '借款人哈希主键',
    loan_amt             DECIMAL(18,2) COMMENT '贷款合同金额',
    currency             STRING      COMMENT '币种',
    rate                  DECIMAL(9,6) COMMENT '执行年利率',
    rate_type             STRING      COMMENT '利率类型: FIXED/LPR_BASED/LPR_FLOAT',
    rate_adjust_cycle    STRING      COMMENT '利率调整周期: MONTHLY/QUARTERLY/YEARLY',
    term_months          INT         COMMENT '贷款期限(月)',
    repay_method         STRING      COMMENT '还款方式: EQUAL_INSTALLMENT/EQUAL_PRINCIPAL/BULLET/INTEREST_ONLY',
    sign_date            DATE        COMMENT '合同签订日期',
    start_date           DATE        COMMENT '贷款起期',
    end_date             DATE        COMMENT '贷款止期',
    guarantee_type       STRING      COMMENT '担保方式: MORTGAGE/GUARANTEE/CREDIT/PLEDGE',
    loan_purpose         STRING      COMMENT '贷款用途',
    org_hash_key         STRING      COMMENT '经办机构哈希主键',
    approver_hash_key    STRING      COMMENT '审批人哈希主键',
    status               STRING      COMMENT '合同状态: DRAFT/APPROVED/ACTIVE/SETTLED/CANCELLED',
    record_source        STRING      COMMENT '数据来源',
    hash_diff            STRING      COMMENT '属性MD5差异值',
    etl_time             TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '贷款合同属性卫星表 (Satellite) — SCD Type 2'
PARTITIONED BY (dt STRING COMMENT '数据日期')
CLUSTERED BY (contract_hash_key) INTO 32 BUCKETS
STORED AS ORC
TBLPROPERTIES ('orc.compress'='ZLIB');
```

### 3.2 dwd_sat_loan_drawdown — 放款记录卫星表（事实型）

```sql
CREATE TABLE dwd_sat_loan_drawdown (
    drawdown_hash_key   STRING      COMMENT '放款-合同关系哈希主键 (FK→LINK_LOAN_DRAWDOWN)',
    contract_hash_key   STRING      COMMENT '合同哈希主键',
    account_hash_key    STRING      COMMENT '收款账户哈希主键',
    load_date           DATE        COMMENT '加载日期',
    drawdown_date       DATE        COMMENT '放款日期',
    drawdown_amt        DECIMAL(18,2) COMMENT '放款金额',
    drawdown_type       STRING      COMMENT '放款类型: FIRST/DRAW_DOWN',
    record_source       STRING      COMMENT '数据来源',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '放款记录卫星表 (Satellite) — 事实型'
PARTITIONED BY (dt STRING COMMENT '放款日期')
CLUSTERED BY (contract_hash_key) INTO 32 BUCKETS
STORED AS ORC
TBLPROPERTIES ('orc.compress'='ZLIB');
```

### 3.3 dwd_sat_loan_repayment — 还款记录卫星表（事实型）

```sql
CREATE TABLE dwd_sat_loan_repayment (
    contract_hash_key   STRING      COMMENT '合同哈希主键',
    load_date           DATE        COMMENT '加载日期',
    repay_date          DATE        COMMENT '还款日期',
    schedule_date       DATE        COMMENT '应还日期',
    principal_amt       DECIMAL(18,2) COMMENT '偿还本金',
    interest_amt        DECIMAL(18,2) COMMENT '偿还利息',
    penalty_amt         DECIMAL(18,2) COMMENT '罚息金额',
    total_repay_amt     DECIMAL(18,2) COMMENT '还款总额',
    repay_type          STRING      COMMENT '还款类型: NORMAL/PREPAY/OVERDUE/SETTLE',
    repay_channel       STRING      COMMENT '还款渠道',
    outstanding_principal DECIMAL(18,2) COMMENT '剩余本金',
    record_source       STRING      COMMENT '数据来源',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '还款记录卫星表 (Satellite) — 事实型'
PARTITIONED BY (dt STRING COMMENT '还款日期')
CLUSTERED BY (contract_hash_key) INTO 32 BUCKETS
STORED AS ORC
TBLPROPERTIES ('orc.compress'='ZLIB');
```

### 3.4 dwd_sat_loan_classification — 五级分类卫星表（快照）

```sql
CREATE TABLE dwd_sat_loan_classification (
    contract_hash_key   STRING      COMMENT '合同哈希主键',
    load_date           DATE        COMMENT '快照日期',
    class_level         STRING      COMMENT '五级分类: NORMAL/SPECIAL_MENTION/SUBSTANDARD/DOUBTFUL/LOSS',
    class_date          DATE        COMMENT '分类认定日期',
    overdue_days        INT         COMMENT '逾期天数',
    overdue_principal   DECIMAL(18,2) COMMENT '逾期本金',
    overdue_interest    DECIMAL(18,2) COMMENT '逾期利息',
    provision_amt       DECIMAL(18,2) COMMENT '拨备金额',
    provision_ratio     DECIMAL(5,4) COMMENT '拨备比例',
    record_source       STRING      COMMENT '数据来源',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '五级分类卫星表 (Satellite) — 每日快照'
PARTITIONED BY (dt STRING COMMENT '快照日期')
CLUSTERED BY (contract_hash_key) INTO 32 BUCKETS
STORED AS ORC
TBLPROPERTIES ('orc.compress'='ZLIB');
```

---

## 4 Link 表

### 4.1 dwd_link_cust_loan — 客户-贷款关系表

```sql
CREATE TABLE dwd_link_cust_loan (
    cust_loan_hash_key  STRING      COMMENT '客户-贷款关系哈希主键',
    customer_hash_key   STRING      COMMENT '客户哈希主键 (FK→HUB_CUSTOMER)',
    contract_hash_key   STRING      COMMENT '合同哈希主键 (FK→HUB_LOAN_CONTRACT)',
    role_type           STRING      COMMENT '角色: BORROWER/CO_BORROWER/GUARANTOR',
    load_date           DATE        COMMENT '加载日期',
    record_source       STRING      COMMENT '数据来源',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '客户-贷款关系链接表 (Link)'
PARTITIONED BY (dt STRING COMMENT '数据日期')
CLUSTERED BY (cust_loan_hash_key) INTO 32 BUCKETS
STORED AS ORC
TBLPROPERTIES ('orc.compress'='ZLIB');
```

### 4.2 dwd_link_loan_drawdown — 合同-放款-账户关系表

```sql
CREATE TABLE dwd_link_loan_drawdown (
    loan_drawdown_hash_key STRING   COMMENT '合同-放款关系哈希主键',
    contract_hash_key   STRING      COMMENT '合同哈希主键',
    account_hash_key    STRING      COMMENT '收款账户哈希主键',
    load_date           DATE        COMMENT '加载日期',
    record_source       STRING      COMMENT '数据来源',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '合同-放款-账户链接表 (Link)'
PARTITIONED BY (dt STRING COMMENT '数据日期')
CLUSTERED BY (loan_drawdown_hash_key) INTO 32 BUCKETS
STORED AS ORC
TBLPROPERTIES ('orc.compress'='ZLIB');
```

---

## 5 核心指标定义

| 指标 | 定义 | 计算来源 |
|------|------|---------|
| 贷款余额 | 所有合同剩余本金之和 | SAT_LOAN_CONTRACT_INFO + SAT_LOAN_REPAYMENT |
| 不良贷款率 | (次级+可疑+损失)余额 / 总贷款余额 | SAT_LOAN_CLASSIFICATION |
| 拨备覆盖率 | 拨备金额 / 不良贷款余额 | SAT_LOAN_CLASSIFICATION |
| 逾期率 | 逾期贷款余额 / 总贷款余额 | SAT_LOAN_CLASSIFICATION (overdue_days>0) |
| 新投放贷款 | 当期新发放贷款金额 | SAT_LOAN_DRAWDOWN |
| 回收率 | 当期回收本金 / 期初贷款余额 | SAT_LOAN_REPAYMENT |

# 10 国际结算域 DWD 层表设计

> 所属：03_数据仓库模型 → DWD层表设计
> 上一文档：[09_理财域_DWD](./09_理财域_DWD.md)
> 下一文档：[11_风控域_DWD](./11_风控域_DWD.md)

---

## 1 国际结算域模型概览

```
┌──────────────────────────────────────────────────────────────┐
│                 国际结算域 DWD 模型                             │
│                                                              │
│  ┌──────────────────┐                                        │
│  │ HUB_LC           │── SAT_LC_INFO (信用证属性)              │
│  │ (信用证)          │                                        │
│  └──────────────────┘                                        │
│                                                              │
│  ┌──────────────────┐                                        │
│  │ HUB_REMITTANCE   │── SAT_REMITTANCE_INFO (汇款属性)         │
│  │ (汇款)            │                                        │
│  └──────────────────┘                                        │
│                                                              │
│  ┌──────────────────┐                                        │
│  │ HUB_COLLECTION   │── SAT_COLLECTION_INFO (托收属性)         │
│  │ (托收)            │                                        │
│  └──────────────────┘                                        │
│                                                              │
│  ┌──────────────────┐                                        │
│  │ HUB_GUARANTEE    │── SAT_GUARANTEE_INFO (保函属性)          │
│  │ (保函)            │                                        │
│  └──────────────────┘                                        │
│                                                              │
│  所有业务关联 HUB_CUSTOMER 和 HUB_ACCOUNT                      │
│  通过 LINK_CUST_* 建立关系                                     │
└──────────────────────────────────────────────────────────────┘
```

---

## 2 Hub 表

### 2.1 信用证中心表

```sql
CREATE TABLE dwd_hub_lc (
    lc_hash_key         STRING      COMMENT '信用证哈希主键',
    lc_no               STRING      COMMENT '信用证编号',
    load_date           DATE        COMMENT '首次加载日期',
    record_source       STRING      COMMENT '首次来源系统',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '信用证中心表 (Hub) — 进口/出口信用证'
PARTITIONED BY (dt STRING COMMENT '数据日期')
CLUSTERED BY (lc_hash_key) INTO 32 BUCKETS
STORED AS ORC;
```

### 2.2 汇款中心表

```sql
CREATE TABLE dwd_hub_remittance (
    remit_hash_key      STRING      COMMENT '汇款哈希主键',
    remit_ref_no        STRING      COMMENT '汇款业务编号',
    load_date           DATE        COMMENT '首次加载日期',
    record_source       STRING      COMMENT '首次来源系统',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '汇款中心表 (Hub) — SWIFT汇出/汇入'
PARTITIONED BY (dt STRING COMMENT '数据日期')
CLUSTERED BY (remit_hash_key) INTO 32 BUCKETS
STORED AS ORC;
```

### 2.3 托收中心表

```sql
CREATE TABLE dwd_hub_collection (
    collection_hash_key STRING      COMMENT '托收哈希主键',
    collection_no       STRING      COMMENT '托收编号',
    load_date           DATE        COMMENT '首次加载日期',
    record_source       STRING      COMMENT '首次来源系统',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '托收中心表 (Hub) — 进口代收/出口托收'
PARTITIONED BY (dt STRING COMMENT '数据日期')
CLUSTERED BY (collection_hash_key) INTO 32 BUCKETS
STORED AS ORC;
```

---

## 3 Satellite 表（核心字段）

### 3.1 dwd_sat_lc_info — 信用证属性卫星表

```sql
CREATE TABLE dwd_sat_lc_info (
    lc_hash_key         STRING      COMMENT '信用证哈希主键',
    load_date           DATE        COMMENT '生效日期',
    load_end_date       DATE        COMMENT '失效日期',
    is_current          BOOLEAN     COMMENT '是否当前',
    lc_type             STRING      COMMENT '类型: IMPORT/EXPORT/STANDBY',
    lc_amt              DECIMAL(18,2) COMMENT '信用证金额',
    currency            STRING      COMMENT '币种',
    issue_date          DATE        COMMENT '开证日期',
    expiry_date         DATE        COMMENT '效期',
    applicant_hash_key  STRING      COMMENT '申请人(客户)哈希',
    beneficiary_name    STRING      COMMENT '受益人名称',
    issuing_bank        STRING      COMMENT '开证行',
    advising_bank       STRING      COMMENT '通知行',
    lc_status           STRING      COMMENT '状态: ISSUED/AMENDED/PRESENTED/SETTLED/CLOSED',
    record_source       STRING      COMMENT '数据来源',
    hash_diff           STRING      COMMENT '属性MD5',
    etl_time            TIMESTAMP   COMMENT 'ETL时间'
)
COMMENT '信用证属性卫星表'
PARTITIONED BY (dt STRING) CLUSTERED BY (lc_hash_key) INTO 32 BUCKETS STORED AS ORC;
```

### 3.2 dwd_sat_remittance_info — 汇款属性卫星表

```sql
CREATE TABLE dwd_sat_remittance_info (
    remit_hash_key      STRING      COMMENT '汇款哈希主键',
    load_date           DATE        COMMENT '生效日期',
    load_end_date       DATE        COMMENT '失效日期',
    is_current          BOOLEAN     COMMENT '是否当前',
    remit_direction     STRING      COMMENT '方向: INWARD/OUTWARD',
    remit_amt           DECIMAL(18,2) COMMENT '汇款金额',
    currency            STRING      COMMENT '币种',
    remit_date          DATE        COMMENT '汇款日期',
    value_date          DATE        COMMENT '起息日',
    remitter_name       STRING      COMMENT '汇款人',
    beneficiary_name    STRING      COMMENT '收款人',
    intermediary_bank   STRING      COMMENT '中间行',
    swift_msg_type      STRING      COMMENT 'SWIFT报文类型: MT103/MT202',
    charge_bearer       STRING      COMMENT '费用承担: OUR/SHA/BEN',
    remit_status        STRING      COMMENT '状态: RECEIVED/PROCESSED/SETTLED',
    record_source       STRING      COMMENT '数据来源',
    hash_diff           STRING      COMMENT '属性MD5',
    etl_time            TIMESTAMP   COMMENT 'ETL时间'
)
COMMENT '汇款属性卫星表'
PARTITIONED BY (dt STRING) CLUSTERED BY (remit_hash_key) INTO 32 BUCKETS STORED AS ORC;
```

---

## 4 核心指标定义

| 指标 | 定义 | 计算来源 |
|------|------|---------|
| 国际结算量 | SUM(信用证+汇款+托收+保函金额) | 各SAT表 |
| 汇入汇款量 | SUM(remit_amt) WHERE direction=INWARD | SAT_REMITTANCE_INFO |
| 汇出汇款量 | SUM(remit_amt) WHERE direction=OUTWARD | SAT_REMITTANCE_INFO |
| 信用证余额 | SUM(lc_amt) WHERE status <> CLOSED | SAT_LC_INFO |
| 结算手续费收入 | SUM(fee_amt) | 关联交易表 |

# 02 Data Vault 建模规范

> 所属：03_数据仓库模型
> 上一文档：[01_数仓分层设计](./01_数仓分层设计.md)
> 下一文档：[DWD层表设计 - 客户域](./03_DWD层表设计/01_客户域_DWD.md)

---

## 1 Data Vault 2.0 核心概念

### 1.1 三大核心组件

```
Data Vault 2.0 数据模型由三种基本表类型组成：

┌─────────────────────────────────────────────────────────────────┐
│                                                                 │
│    Hub (中心表)           Link (链接表)        Satellite (卫星表) │
│    ┌──────────┐          ┌──────────┐         ┌──────────┐       │
│    │ 业务实体  │◄────────┤ 实体关系  │────────►│ 属性/事实 │       │
│    │ 主键+哈希 │         │ 多Hub关联 │         │ 时间拉链  │       │
│    └──────────┘          └──────────┘         └──────────┘       │
│                                                                 │
│  示例：                   示例：                 示例：            │
│  HUB_CUSTOMER            LINK_CUST_ACCT        SAT_CUSTOMER_INFO │
│  HUB_ACCOUNT             LINK_LOAN_DRAWDOWN    SAT_ACCOUNT_BAL   │
│  HUB_PRODUCT             LINK_TRANSACTION      SAT_TRANSACTION   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 1.2 为什么在本项目中使用 Data Vault

| 银行数仓痛点 | Data Vault 优势 | 具体体现 |
|------------|----------------|---------|
| 多源异构 | 业务主键与源系统解耦 | 核心银行+ECIF 的客户归并 |
| 频繁变更 | 新增 Satellite 不影响现有结构 | 监管新要求→新增属性卫星表 |
| 历史追溯 | 内置时间维度 | SAT 表天然支持 SCD Type 2 |
| 审计合规 | RECORD_SOURCE + LOAD_DATE | 每行可追溯到来源系统和时间 |
| 团队并行 | 组件独立，可独立开发 | Hub/Link/Sat 可由不同团队并行建模 |

---

## 2 Hub 表设计规范

### 2.1 定义

Hub 表存储**业务实体**的核心标识，是 Data Vault 的最基础组件。

### 2.2 表结构规范

```sql
-- Hub 模板
CREATE TABLE dwd_hub_{entity} (
    {entity}_hash_key   STRING  COMMENT '{实体}哈希主键，MD5(source_system|business_key|HUB_{ENTITY})',
    {entity}_id         STRING  COMMENT '{实体}业务主键',
    load_date           DATE    COMMENT '数据加载日期',
    record_source       STRING  COMMENT '数据来源系统',
    etl_time            TIMESTAMP COMMENT 'ETL 处理时间'
)
COMMENT '{实体}中心表 — Data Vault Hub'
PARTITIONED BY (dt STRING COMMENT '数据日期')
CLUSTERED BY ({entity}_hash_key) INTO 32 BUCKETS
STORED AS ORC
TBLPROPERTIES ('orc.compress'='ZLIB');
```

### 2.3 HASH_KEY 生成规则

```sql
-- HASH_KEY = MD5(source_system + '|' + business_key + '|' + entity_type)
-- 保证跨系统、跨时间可复现

-- 示例：客户 Hub
SELECT MD5(CONCAT(
    COALESCE(source_system, 'UNKNOWN'),
    '|',
    COALESCE(TRIM(customer_id), ''),
    '|',
    'HUB_CUSTOMER'
)) AS customer_hash_key
FROM (
    SELECT 'CORE' AS source_system, CUSTOMER_ID AS customer_id FROM ods_core.t_customer
    UNION
    SELECT 'ECIF' AS source_system, PARTY_ID AS customer_id FROM ods_ecif.t_ecif_party
);
```

### 2.4 本项目的 Hub 表清单

| Hub 表 | 业务实体 | 业务主键 | 来源系统 |
|--------|---------|---------|---------|
| `dwd_hub_customer` | 客户 | customer_id | CORE, ECIF, CC, E-BANK |
| `dwd_hub_account` | 账户 | account_no | CORE |
| `dwd_hub_loan_contract` | 贷款合同 | contract_no | LOAN |
| `dwd_hub_cc_card` | 信用卡 | card_no | CC |
| `dwd_hub_product` | 产品 | product_id | CORE, WEALTH |
| `dwd_hub_org` | 机构 | org_id | CORE, GL |
| `dwd_hub_employee` | 员工 | employee_id | CORE, CRM |
| `dwd_hub_transaction` | 交易 | trans_id | CORE, PAY, CC, E-BANK |
| `dwd_hub_merchant` | 商户 | merchant_id | CC(POS), ATM/POS |
| `dwd_hub_gl_account` | 科目 | account_code | GL |

---

## 3 Link 表设计规范

### 3.1 定义

Link 表存储**两个或多个 Hub 实体之间的关联关系**。

### 3.2 表结构规范

```sql
-- Link 模板
CREATE TABLE dwd_link_{relation} (
    {relation}_hash_key  STRING  COMMENT '关系哈希主键',
    {hub1}_hash_key      STRING  COMMENT 'Hub1 外键',
    {hub2}_hash_key      STRING  COMMENT 'Hub2 外键',
    {hub3}_hash_key      STRING  COMMENT 'Hub3 外键 (如有)',
    {relation}_type      STRING  COMMENT '关系类型',
    load_date            DATE    COMMENT '数据加载日期',
    record_source        STRING  COMMENT '数据来源系统',
    etl_time             TIMESTAMP COMMENT 'ETL 处理时间'
)
COMMENT '{关系}链接表 — Data Vault Link'
PARTITIONED BY (dt STRING COMMENT '数据日期')
CLUSTERED BY ({relation}_hash_key) INTO 32 BUCKETS
STORED AS ORC
TBLPROPERTIES ('orc.compress'='ZLIB');
```

### 3.3 关系 HASH_KEY 生成规则

```sql
-- LINK_HASH_KEY = MD5(hub1_hash_key + '|' + hub2_hash_key + '|' + relation_type + '|' + LINK_{RELATION})
SELECT MD5(CONCAT(
    c.customer_hash_key, '|',
    a.account_hash_key, '|',
    COALESCE(rel_type, ''), '|',
    'LINK_CUST_ACCT'
)) AS cust_acct_hash_key
FROM ...
```

### 3.4 本项目的 Link 表清单

| Link 表 | 关联的 Hub | 关系类型 |
|---------|-------------------|---------|
| `dwd_link_cust_acct` | CUSTOMER + ACCOUNT | 主账户/附属账户/担保 |
| `dwd_link_cust_loan` | CUSTOMER + LOAN_CONTRACT | 借款人/共同借款人/担保人 |
| `dwd_link_loan_drawdown` | LOAN_CONTRACT + ACCOUNT | 放款-收款账户 |
| `dwd_link_transaction` | ACCOUNT + ACCOUNT + CHANNEL | 转出-转入-渠道 |
| `dwd_link_cust_cc` | CUSTOMER + CC_CARD | 主卡/附属卡 |
| `dwd_link_cc_transaction` | CC_CARD + MERCHANT | 消费交易 |
| `dwd_link_cust_wealth` | CUSTOMER + PRODUCT | 理财持有 |
| `dwd_link_org_emp` | ORG + EMPLOYEE | 机构-员工归属 |
| `dwd_link_cust_ecif` | CUSTOMER(HUB) + CUSTOMER(HUB) | 跨系统客户归并映射 |

---

## 4 Satellite 表设计规范

### 4.1 定义

Satellite 表存储 Hub 或 Link 的**描述性属性**和**事实数据**，通过时间维度支持历史追溯。

### 4.2 属性型 Satellite (SCD Type 2)

```sql
-- 属性型 Satellite 模板
CREATE TABLE dwd_sat_{entity}_{content} (
    {entity}_hash_key   STRING  COMMENT '{实体}哈希主键 (FK→Hub)',
    load_date           DATE    COMMENT '数据加载日期 (PK部分)',
    load_end_date       DATE    COMMENT '数据失效日期 (拉链用，默认 9999-12-31)',
    is_current          BOOLEAN COMMENT '是否当前记录',
    -- 业务属性字段
    {attribute_1}       STRING  COMMENT '业务属性',
    {attribute_2}       STRING  COMMENT '业务属性',
    ...
    record_source       STRING  COMMENT '数据来源系统',
    hash_diff           STRING  COMMENT '属性MD5，用于变更检测',
    etl_time            TIMESTAMP COMMENT 'ETL 处理时间'
)
COMMENT '{实体}-{内容}属性卫星表 — Data Vault Satellite'
PARTITIONED BY (dt STRING COMMENT '数据日期')
CLUSTERED BY ({entity}_hash_key) INTO 32 BUCKETS
STORED AS ORC
TBLPROPERTIES ('orc.compress'='ZLIB');
```

### 4.3 属性变更检测（HASH_DIFF）

```sql
-- HASH_DIFF = MD5(所有业务属性字段拼接)
-- 用于检测属性是否发生变化，避免全字段比对

SELECT MD5(CONCAT_WS('|',
    COALESCE(cust_name, ''),
    COALESCE(id_type, ''),
    COALESCE(id_no, ''),
    COALESCE(mobile, ''),
    COALESCE(address, ''),
    COALESCE(cust_type, ''),
    COALESCE(cust_level, '')
)) AS hash_diff
FROM ...
```

### 4.4 拉链处理逻辑

```sql
-- Satellite 拉链更新 (SCD Type 2)
-- Step 1: 识别变更记录 (HASH_DIFF 变化)
CREATE TEMPORARY TABLE sat_changes AS
SELECT
    n.customer_hash_key,
    n.cust_name, n.id_type, n.id_no, n.mobile, n.address,
    n.cust_type, n.cust_level,
    '${yesterday}' AS load_date,
    '9999-12-31' AS load_end_date,
    TRUE AS is_current,
    n.record_source,
    n.hash_diff
FROM new_data n
LEFT JOIN dwd_sat_customer_info c
    ON n.customer_hash_key = c.customer_hash_key
    AND c.is_current = TRUE
WHERE c.customer_hash_key IS NULL           -- 新客户
   OR n.hash_diff <> c.hash_diff;          -- 属性变更

-- Step 2: 关闭旧记录
UPDATE dwd_sat_customer_info
SET load_end_date = '${yesterday}',
    is_current = FALSE
WHERE customer_hash_key IN (
    SELECT customer_hash_key FROM sat_changes
)
AND is_current = TRUE;

-- Step 3: 插入新记录
INSERT INTO dwd_sat_customer_info
SELECT * FROM sat_changes;
```

### 4.5 事实型 Satellite

与属性型 Satellite 不同，事实型 Satellite **不做拉链**，直接追加新行：

```sql
-- 事实型 Satellite 模板（交易流水）
CREATE TABLE dwd_sat_transaction (
    transaction_hash_key STRING  COMMENT '交易哈希主键 (FK→Hub)',
    load_date            DATE    COMMENT '加载日期',
    {hub1}_hash_key      STRING  COMMENT '关联 Hub',
    {hub2}_hash_key      STRING  COMMENT '关联 Hub',
    trans_date           DATE    COMMENT '交易日期',
    trans_time           TIMESTAMP COMMENT '交易时间',
    trans_amt            DECIMAL(18,2) COMMENT '交易金额',
    trans_type           STRING  COMMENT '交易类型',
    channel              STRING  COMMENT '交易渠道',
    ...
    record_source        STRING  COMMENT '数据来源',
    etl_time             TIMESTAMP COMMENT 'ETL 时间'
)
COMMENT '交易明细事实卫星表'
PARTITIONED BY (dt STRING COMMENT '交易日期')
CLUSTERED BY (transaction_hash_key) INTO 32 BUCKETS
STORED AS ORC
TBLPROPERTIES ('orc.compress'='ZLIB');
```

### 4.6 本项目的 Satellite 表清单

| Satellite 表 | 关联 Hub/Link | 类型 | 主要内容 |
|-------------|--------------|------|---------|
| `dwd_sat_customer_info` | HUB_CUSTOMER | 属性 | 姓名、证件、联系信息、客户等级（拉链） |
| `dwd_sat_customer_label` | HUB_CUSTOMER | 属性 | 客户标签、画像标签（拉链） |
| `dwd_sat_account_bal` | HUB_ACCOUNT | 快照 | 账户每日余额快照 |
| `dwd_sat_account_status` | HUB_ACCOUNT | 属性 | 账户状态变更（拉链） |
| `dwd_sat_loan_contract_info` | HUB_LOAN_CONTRACT | 属性 | 合同金额、利率、期限（拉链） |
| `dwd_sat_loan_repayment` | LINK_LOAN_DRAWDOWN | 事实 | 还款明细 |
| `dwd_sat_transaction` | LINK_TRANSACTION | 事实 | 交易明细（追加） |
| `dwd_sat_cc_card_info` | HUB_CC_CARD | 属性 | 卡类型、额度、状态（拉链） |
| `dwd_sat_cc_transaction` | LINK_CC_TRANSACTION | 事实 | 信用卡消费明细 |
| `dwd_sat_product_info` | HUB_PRODUCT | 属性 | 产品名称、类型、风险等级（拉链） |
| `dwd_sat_org_info` | HUB_ORG | 属性 | 机构名称、类型、层级（拉链） |

---

## 5 加载顺序约束

Data Vault 的加载有严格的依赖顺序：

```
Step 1: Hub 表 (独立，可并行)
         ├─ dwd_hub_customer
         ├─ dwd_hub_account
         ├─ dwd_hub_product
         ├─ dwd_hub_org
         └─ ...

Step 2: Link 表 (依赖 Hub)
         ├─ dwd_link_cust_acct    (依赖 HUB_CUSTOMER + HUB_ACCOUNT)
         ├─ dwd_link_cust_loan    (依赖 HUB_CUSTOMER + HUB_LOAN_CONTRACT)
         └─ dwd_link_transaction  (依赖 HUB_ACCOUNT + HUB_TRANSACTION)

Step 3: Satellite 表 (依赖 Hub 或 Link)
         ├─ dwd_sat_customer_info (依赖 HUB_CUSTOMER)
         ├─ dwd_sat_account_bal   (依赖 HUB_ACCOUNT)
         ├─ dwd_sat_transaction   (依赖 LINK_TRANSACTION)
         └─ ...
```

---

## 6 Data Vault 与传统数仓组件对照

| Data Vault 组件 | 传统数仓对应 | 相似点 | 差异点 |
|----------------|------------|--------|--------|
| Hub | 维度表（业务主键部分） | 都存储实体标识 | Hub 只存标识，属性在 Satellite |
| Link | 事实表（外键部分） | 都存储关联关系 | Link 只是关系，度量在 Satellite |
| Satellite (属性) | 维度表（属性部分）+ SCD Type 2 | 都存储描述性属性 | Satellite 按内容拆分，更灵活 |
| Satellite (事实) | 事务事实表 | 都存储业务过程度量 | Satellite 直接挂 Link，粒度更细 |
| Hub+Sat(属性) 组合 | 完整维度表 | 最终使用时 JOIN 得到 | Data Vault 解耦存储 |
| Link+Sat(事实) 组合 | 完整事实表 | 最终使用时 JOIN 得到 | Data Vault 解耦存储 |

---

## 7 命名规范速查

```
Hub:       dwd_hub_{entity}
Link:      dwd_link_{relation_description}
Satellite: dwd_sat_{entity}_{content_description}
Fact:      dwd_fact_{business_process}
Zip:       dwd_zip_{entity}_{content}   (拉链表)

字段:
  哈希主键: {entity}_hash_key
  业务主键: {entity}_id / {entity}_no
  加载日期: load_date
  失效日期: load_end_date
  来源系统: record_source
  哈希差异: hash_diff
  是否当前: is_current
```

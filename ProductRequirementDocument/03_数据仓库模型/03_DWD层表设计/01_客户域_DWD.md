# 01 客户域 DWD 层表设计

> 所属：03_数据仓库模型 → DWD层表设计
> 上一文档：[02_Data_Vault建模规范](../02_Data_Vault建模规范.md)
> 下一文档：[02_机构域_DWD](./02_机构域_DWD.md)

---

## 1 客户域模型概览

```
┌─────────────────────────────────────────────────────────────────┐
│                    客户域 DWD 模型                                 │
│                                                                 │
│  ┌──────────────────┐    ┌──────────────────┐                   │
│  │  HUB_CUSTOMER    │◄───┤  LINK_CUST_ACCT  │──► HUB_ACCOUNT    │
│  │  customer_hash   │    │  cust_acct_hash  │                   │
│  │  customer_id     │    │  rel_type        │                   │
│  └──────┬───────────┘    └──────────────────┘                   │
│         │                                                       │
│         ├── SAT_CUSTOMER_INFO (属性拉链)                          │
│         │   cust_name, id_type, id_no, mobile, address,         │
│         │   cust_type, cust_level, occupation, income...        │
│         │                                                       │
│         ├── SAT_CUSTOMER_LABEL (标签拉链)                         │
│         │   aum_level, risk_level, value_score, lifecycle,      │
│         │   active_level, channel_preference...                 │
│         │                                                       │
│         └── LINK_CUST_ECIF (客户跨系统映射)                        │
│             customer_hash_key_1, customer_hash_key_2,           │
│             match_score, merge_status                           │
└─────────────────────────────────────────────────────────────────┘
```

---

## 2 Hub 表

### 2.1 dwd_hub_customer — 客户中心表

```sql
CREATE TABLE dwd_hub_customer (
    customer_hash_key   STRING      COMMENT '客户哈希主键',
    customer_id         STRING      COMMENT '客户业务编号(源系统原始ID)',
    load_date           DATE        COMMENT '首次加载日期',
    record_source       STRING      COMMENT '首次来源系统',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '客户中心表 (Hub) — 银行业务中唯一标识的个人或企业客户'
PARTITIONED BY (dt STRING COMMENT '数据日期')
CLUSTERED BY (customer_hash_key) INTO 32 BUCKETS
STORED AS ORC
TBLPROPERTIES ('orc.compress'='ZLIB');

-- HASH_KEY 生成
-- MD5(source_system|TRIM(customer_id)|HUB_CUSTOMER)
```

**来源系统与业务主键映射**：

| 来源系统 | 业务主键字段 | 说明 |
|---------|------------|------|
| CORE (核心银行) | `CUSTOMER_ID` | 核心系统客户号 |
| ECIF | `PARTY_ID` | ECIF 统一客户号 |
| CC (信用卡) | `customer_id` | 信用卡系统客户号 |
| E-BANK (网银) | `customer_id` | 网银客户号 |

**Deduplication 逻辑**：同一客户可能存在于多个源系统，通过 ECIF 映射归并。

---

## 3 Satellite 表

### 3.1 dwd_sat_customer_info — 客户属性卫星表（拉链）

```sql
CREATE TABLE dwd_sat_customer_info (
    customer_hash_key   STRING      COMMENT '客户哈希主键',
    load_date           DATE        COMMENT '记录生效日期',
    load_end_date       DATE        COMMENT '记录失效日期 (9999-12-31=当前)',
    is_current          BOOLEAN     COMMENT '是否当前记录',
    cust_name           STRING      COMMENT '客户姓名/企业名称',
    id_type             STRING      COMMENT '证件类型: ID_CARD/PASSPORT/BIZ_LIC',
    id_no               STRING      COMMENT '证件号码',
    mobile              STRING      COMMENT '手机号码',
    phone               STRING      COMMENT '固定电话',
    email               STRING      COMMENT '电子邮箱',
    address             STRING      COMMENT '通讯地址',
    cust_type            STRING      COMMENT '客户类型: PERSONAL/CORPORATE/INSTITUTION',
    cust_level           STRING      COMMENT '客户等级: HIGH_NET/AFFLUENT/MASS/LONG_TAIL',
    gender               STRING      COMMENT '性别: M/F/U',
    birth_date           DATE        COMMENT '出生日期/成立日期',
    nationality          STRING      COMMENT '国籍/注册地',
    occupation           STRING      COMMENT '职业/行业',
    annual_income        DECIMAL(18,2) COMMENT '年收入/年营收',
    education            STRING      COMMENT '学历: HIGH_SCHOOL/BACHELOR/MASTER/PHD',
    open_date            DATE        COMMENT '开户日期',
    close_date           DATE        COMMENT '销户日期',
    status               STRING      COMMENT '客户状态: ACTIVE/INACTIVE/CLOSED/FROZEN',
    record_source        STRING      COMMENT '数据来源',
    hash_diff            STRING      COMMENT '属性MD5差异值',
    etl_time             TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '客户属性卫星表 (Satellite) — SCD Type 2 拉链'
PARTITIONED BY (dt STRING COMMENT '数据日期')
CLUSTERED BY (customer_hash_key) INTO 32 BUCKETS
STORED AS ORC
TBLPROPERTIES ('orc.compress'='ZLIB');

-- HASH_DIFF = MD5(CONCAT_WS('|', cust_name, id_type, id_no, mobile, phone, email,
--                           address, cust_type, cust_level, gender, birth_date,
--                           nationality, occupation, annual_income, education, status))
```

### 3.2 dwd_sat_customer_label — 客户标签卫星表（拉链）

```sql
CREATE TABLE dwd_sat_customer_label (
    customer_hash_key   STRING      COMMENT '客户哈希主键',
    load_date           DATE        COMMENT '标签生效日期',
    load_end_date       DATE        COMMENT '标签失效日期',
    is_current          BOOLEAN     COMMENT '是否当前标签',
    aum_level           STRING      COMMENT 'AUM分层: UHNW(超高净值)/HNW(高净值)/AFFLUENT(富裕)/MASS(大众)/LONG_TAIL(长尾)',
    risk_level           STRING      COMMENT '风险等级: CONSERVATIVE/MODERATE/AGGRESSIVE',
    value_score          INT         COMMENT '客户价值评分 1-100',
    life_cycle           STRING      COMMENT '生命周期: NEW/ACTIVE/MATURE/DECLINE/CHURN',
    active_level         STRING      COMMENT '活跃度: HIGH/MEDIUM/LOW/SILENT',
    channel_preference   STRING      COMMENT '渠道偏好: COUNTER/ONLINE/MOBILE/ATM/POS',
    transaction_habit    STRING      COMMENT '交易习惯: DAY/NIGHT/WEEKEND/REGULAR',
    credit_score         INT         COMMENT '行内信用评分 300-850',
    cross_sell_potential STRING      COMMENT '交叉销售潜力: HIGH/MEDIUM/LOW',
    next_best_product    STRING      COMMENT '下一步推荐产品',
    churn_probability    DECIMAL(5,4) COMMENT '流失概率 0-1',
    record_source        STRING      COMMENT '数据来源',
    hash_diff            STRING      COMMENT '属性MD5差异值',
    etl_time             TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '客户标签卫星表 (Satellite) — 客户画像标签体系'
PARTITIONED BY (dt STRING COMMENT '数据日期')
CLUSTERED BY (customer_hash_key) INTO 32 BUCKETS
STORED AS ORC
TBLPROPERTIES ('orc.compress'='ZLIB');
```

---

## 4 Link 表

### 4.1 dwd_link_cust_acct — 客户-账户关系表

```sql
CREATE TABLE dwd_link_cust_acct (
    cust_acct_hash_key  STRING      COMMENT '客户-账户关系哈希主键',
    customer_hash_key   STRING      COMMENT '客户哈希主键 (FK→HUB_CUSTOMER)',
    account_hash_key    STRING      COMMENT '账户哈希主键 (FK→HUB_ACCOUNT)',
    rel_type            STRING      COMMENT '关系类型: PRIMARY/JOINT/GUARANTOR/AUTHORIZED',
    rel_start_date      DATE        COMMENT '关系生效日期',
    rel_end_date        DATE        COMMENT '关系失效日期',
    is_active           BOOLEAN     COMMENT '关系是否有效',
    load_date           DATE        COMMENT '加载日期',
    record_source       STRING      COMMENT '数据来源',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '客户-账户关系链接表 (Link)'
PARTITIONED BY (dt STRING COMMENT '数据日期')
CLUSTERED BY (cust_acct_hash_key) INTO 32 BUCKETS
STORED AS ORC
TBLPROPERTIES ('orc.compress'='ZLIB');
```

### 4.2 dwd_link_cust_ecif — 客户跨系统映射表

```sql
CREATE TABLE dwd_link_cust_ecif (
    cust_map_hash_key   STRING      COMMENT '客户映射哈希主键',
    party_hash_key      STRING      COMMENT 'ECIF统一客户哈希主键',
    src_cust_hash_key   STRING      COMMENT '源系统客户哈希主键',
    source_system       STRING      COMMENT '源系统编码: CORE/CC/E-BANK/WEALTH',
    source_cust_id      STRING      COMMENT '源系统客户ID',
    match_score         DECIMAL(5,4) COMMENT '匹配置信度 0-1',
    match_type          STRING      COMMENT '匹配类型: AUTO/MANUAL/UNMATCHED',
    merge_status        STRING      COMMENT '归并状态: MERGED/UNIQUE/SPLIT',
    load_date           DATE        COMMENT '加载日期',
    record_source       STRING      COMMENT '数据来源 (ECIF)',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '客户跨系统映射链接表 (Link) — ECIF客户统一视图基础'
PARTITIONED BY (dt STRING COMMENT '数据日期')
CLUSTERED BY (cust_map_hash_key) INTO 32 BUCKETS
STORED AS ORC
TBLPROPERTIES ('orc.compress'='ZLIB');
```

---

## 5 ETL 逻辑

### 5.1 HUB_CUSTOMER 加载

```sql
-- 从多个源系统抽取客户标识，生成 HASH_KEY 去重
INSERT OVERWRITE TABLE dwd_hub_customer PARTITION (dt='${yesterday}')
SELECT
    MD5(CONCAT(COALESCE(src.source_system, 'UNKNOWN'), '|',
               COALESCE(TRIM(src.customer_id), ''), '|',
               'HUB_CUSTOMER')) AS customer_hash_key,
    src.customer_id,
    '${yesterday}' AS load_date,
    src.source_system AS record_source,
    CURRENT_TIMESTAMP() AS etl_time
FROM (
    SELECT 'CORE' AS source_system, CAST(CUSTOMER_ID AS STRING) AS customer_id
    FROM ods_core.t_customer WHERE dt = '${yesterday}'
    UNION
    SELECT 'ECIF' AS source_system, CAST(PARTY_ID AS STRING) AS customer_id
    FROM ods_ecif.t_ecif_party WHERE dt = '${yesterday}'
    UNION
    SELECT 'CC' AS source_system, CAST(customer_id AS STRING) AS customer_id
    FROM ods_cc.t_cc_card WHERE dt = '${yesterday}'
    UNION
    SELECT 'E-BANK' AS source_system, CAST(customer_id AS STRING) AS customer_id
    FROM ods_ebank.t_login_event WHERE dt = '${yesterday}'
) src
LEFT JOIN dwd_hub_customer h ON MD5(...) = h.customer_hash_key AND h.dt = '${yesterday}'
WHERE h.customer_hash_key IS NULL;  -- 只加载新实体
```

### 5.2 SAT_CUSTOMER_INFO 拉链更新

```sql
-- Step 1: 计算新数据的 HASH_DIFF
CREATE TEMPORARY TABLE tmp_cust_hashdiff AS
SELECT
    h.customer_hash_key,
    o.CUST_NAME, o.ID_TYPE, o.ID_NO, o.MOBILE, o.PHONE, o.EMAIL,
    o.ADDRESS, o.CUST_TYPE, o.CUST_LEVEL, o.GENDER, o.BIRTH_DATE,
    o.NATIONALITY, o.OCCUPATION, o.ANNUAL_INCOME, o.EDUCATION, o.STATUS,
    'CORE' AS record_source,
    MD5(CONCAT_WS('|',
        COALESCE(o.CUST_NAME, ''), COALESCE(o.ID_TYPE, ''), COALESCE(o.ID_NO, ''),
        COALESCE(o.MOBILE, ''), COALESCE(o.PHONE, ''), COALESCE(o.EMAIL, ''),
        COALESCE(o.ADDRESS, ''), COALESCE(o.CUST_TYPE, ''), COALESCE(o.CUST_LEVEL, ''),
        COALESCE(o.GENDER, ''), CAST(COALESCE(o.BIRTH_DATE, '1900-01-01') AS STRING),
        COALESCE(o.NATIONALITY, ''), COALESCE(o.OCCUPATION, ''),
        CAST(COALESCE(o.ANNUAL_INCOME, 0) AS STRING),
        COALESCE(o.EDUCATION, ''), COALESCE(o.STATUS, '')
    )) AS hash_diff
FROM ods_core.t_customer o
JOIN dwd_hub_customer h
    ON o.CUSTOMER_ID = h.customer_id AND h.record_source = 'CORE'
WHERE o.dt = '${yesterday}';

-- Step 2: 关闭已变更的记录
UPDATE dwd_sat_customer_info
SET load_end_date = '${yesterday}', is_current = FALSE
WHERE customer_hash_key IN (
    SELECT t.customer_hash_key FROM tmp_cust_hashdiff t
    JOIN dwd_sat_customer_info s ON t.customer_hash_key = s.customer_hash_key
    WHERE s.is_current = TRUE AND t.hash_diff <> s.hash_diff
);

-- Step 3: 插入新记录
INSERT INTO dwd_sat_customer_info
SELECT
    customer_hash_key,
    '${yesterday}' AS load_date,
    '9999-12-31' AS load_end_date,
    TRUE AS is_current,
    CUST_NAME, ID_TYPE, ID_NO, MOBILE, PHONE, EMAIL, ADDRESS,
    CUST_TYPE, CUST_LEVEL, GENDER, BIRTH_DATE, NATIONALITY,
    OCCUPATION, ANNUAL_INCOME, EDUCATION,
    NULL AS open_date, NULL AS close_date, STATUS,
    record_source, hash_diff, CURRENT_TIMESTAMP()
FROM tmp_cust_hashdiff;
```

---

## 6 数据质量规则

| 规则 | 检查内容 | 严重级别 | SQL |
|------|---------|---------|-----|
| HUB_CUSTOMER 唯一性 | customer_hash_key 无重复 | CRITICAL | `SELECT customer_hash_key, COUNT(*) cnt FROM dwd_hub_customer WHERE dt='${dt}' GROUP BY 1 HAVING cnt > 1` |
| SAT 属性完整性 | cust_name NULL率 | HIGH | `SELECT SUM(CASE WHEN cust_name IS NULL THEN 1 END) * 100.0 / COUNT(*) FROM dwd_sat_customer_info WHERE is_current=TRUE AND dt='${dt}'` |
| LINK 参照完整性 | cust_acct_hash_key 引用的 Hub 存在 | CRITICAL | `SELECT l.cust_acct_hash_key FROM dwd_link_cust_acct l LEFT JOIN dwd_hub_customer h ON l.customer_hash_key = h.customer_hash_key WHERE h.customer_hash_key IS NULL` |

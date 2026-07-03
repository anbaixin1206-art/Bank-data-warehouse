# 03 产品域 DWD 层表设计

> 所属：03_数据仓库模型 → DWD层表设计
> 上一文档：[02_机构域_DWD](./02_机构域_DWD.md)
> 下一文档：[04_渠道域_DWD](./04_渠道域_DWD.md)

---

## 1 产品域模型概览

```
┌──────────────────────────────────────────────────────────────┐
│                    产品域 DWD 模型                              │
│                                                              │
│  ┌──────────────────┐                                        │
│  │   HUB_PRODUCT    │                                        │
│  │   product_hash   │                                        │
│  │   product_id     │                                        │
│  └────────┬─────────┘                                        │
│           │                                                  │
│           └── SAT_PRODUCT_INFO (产品属性拉链)                   │
│               product_name, product_type, product_subtype,   │
│               risk_level, min_amount, term, rate_info...     │
│                                                              │
│  产品分类树 (通过 parent_product_hash_key 自关联):              │
│  金融产品                                                    │
│  ├─ 存款产品                                                 │
│  │   ├─ 活期存款                                             │
│  │   ├─ 定期存款 (3月/6月/1年/2年/3年/5年)                   │
│  │   ├─ 通知存款 (1天/7天)                                   │
│  │   ├─ 大额存单                                             │
│  │   └─ 结构性存款                                           │
│  ├─ 贷款产品                                                 │
│  ├─ 支付产品                                                 │
│  ├─ 信用卡产品                                               │
│  ├─ 理财产品                                                 │
│  └─ 国际结算产品                                             │
└──────────────────────────────────────────────────────────────┘
```

---

## 2 Hub 表

### 2.1 dwd_hub_product — 产品中心表

```sql
CREATE TABLE dwd_hub_product (
    product_hash_key    STRING      COMMENT '产品哈希主键',
    product_id          STRING      COMMENT '产品编号',
    load_date           DATE        COMMENT '首次加载日期',
    record_source       STRING      COMMENT '首次来源系统',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '产品中心表 (Hub) — 全行金融产品目录'
PARTITIONED BY (dt STRING COMMENT '数据日期')
CLUSTERED BY (product_hash_key) INTO 32 BUCKETS
STORED AS ORC
TBLPROPERTIES ('orc.compress'='ZLIB');
```

---

## 3 Satellite 表

### 3.1 dwd_sat_product_info — 产品属性卫星表（拉链）

```sql
CREATE TABLE dwd_sat_product_info (
    product_hash_key        STRING      COMMENT '产品哈希主键',
    load_date               DATE        COMMENT '记录生效日期',
    load_end_date           DATE        COMMENT '记录失效日期',
    is_current              BOOLEAN     COMMENT '是否当前记录',
    product_name            STRING      COMMENT '产品名称',
    product_type            STRING      COMMENT '产品大类: DEPOSIT/LOAN/PAYMENT/CREDIT_CARD/WEALTH/INTL_SETTLE',
    product_subtype         STRING      COMMENT '产品子类: DEMAND/TIME/NOTICE/CD/STRUCTURAL...',
    parent_product_hash_key STRING      COMMENT '父产品哈希主键(产品层级)',
    risk_level              STRING      COMMENT '风险等级: R1/R2/R3/R4/R5',
    min_amount              DECIMAL(18,2) COMMENT '起存/起购/起贷金额',
    max_amount              DECIMAL(18,2) COMMENT '上限金额',
    term_months             INT         COMMENT '期限(月)',
    rate_type               STRING      COMMENT '利率类型: FIXED/FLOATING/LPR_BASED',
    base_rate               DECIMAL(9,6) COMMENT '基准年利率',
    penalty_rate            DECIMAL(9,6) COMMENT '罚息利率',
    fee_desc                STRING      COMMENT '收费说明(JSON)',
    product_desc            STRING      COMMENT '产品描述',
    launch_date             DATE        COMMENT '推出日期',
    expire_date             DATE        COMMENT '到期/停售日期',
    status                  STRING      COMMENT '状态: ACTIVE/INACTIVE/SUSPENDED',
    record_source           STRING      COMMENT '数据来源',
    hash_diff               STRING      COMMENT '属性MD5差异值',
    etl_time                TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '产品属性卫星表 (Satellite) — SCD Type 2 拉链'
PARTITIONED BY (dt STRING COMMENT '数据日期')
CLUSTERED BY (product_hash_key) INTO 32 BUCKETS
STORED AS ORC
TBLPROPERTIES ('orc.compress'='ZLIB');
```

---

## 4 产品编码规范

### 4.1 产品大类与子类

| 大类编码 | 大类名称 | 子类编码示例 | 子类名称 |
|---------|---------|------------|---------|
| `DEPOSIT` | 存款 | `DEMAND` | 活期存款 |
| | | `TIME_3M` | 定期3个月 |
| | | `TIME_6M` | 定期6个月 |
| | | `TIME_1Y` | 定期1年 |
| | | `TIME_3Y` | 定期3年 |
| | | `NOTICE_1D` | 通知存款1天 |
| | | `NOTICE_7D` | 通知存款7天 |
| | | `CD` | 大额存单 |
| | | `STRUCTURAL` | 结构性存款 |
| `LOAN` | 贷款 | `MORTGAGE` | 个人住房贷款 |
| | | `CONSUMER` | 个人消费贷款 |
| | | `BIZ_LOAN` | 个人经营贷款 |
| | | `CORP_WORKING` | 对公流动资金贷款 |
| | | `SYNDICATED` | 银团贷款 |
| `PAYMENT` | 支付 | `INTERNAL_TRANSFER` | 行内转账 |
| | | `CROSS_BANK` | 跨行转账 |
| | | `QUICK_PAY` | 快捷支付 |
| | | `AGENT_COLLECT` | 代收 |
| | | `AGENT_PAY` | 代付 |
| `CREDIT_CARD` | 信用卡 | `CC_STANDARD` | 标准卡 |
| | | `CC_GOLD` | 金卡 |
| | | `CC_PLATINUM` | 白金卡 |
| | | `CC_INSTALLMENT` | 分期卡 |
| | | `CC_COBRAND` | 联名卡 |
| `WEALTH` | 理财 | `MONEY_FUND` | 货币基金 |
| | | `BOND_WM` | 债券理财 |
| | | `NAV_WM` | 净值型理财 |
| | | `TRUST` | 信托计划 |
| | | `INSURANCE` | 保险代销 |
| `INTL_SETTLE` | 国际结算 | `LETTER_CREDIT` | 信用证 |
| | | `COLLECTION` | 托收 |
| | | `REMIT` | 汇款 |
| | | `GUARANTEE` | 保函 |

---

## 5 来源映射

| 来源系统 | 来源表 | 产品类型 | 说明 |
|---------|--------|---------|------|
| CORE (核心银行) | `T_ACCOUNT` (ACCT_TYPE) | DEPOSIT | 存款产品通过账户类型定义 |
| CORE | 利率配置表 | DEPOSIT/LOAN | 利率与产品关联 |
| LOAN (信贷) | `T_LOAN_CONTRACT` (LOAN_TYPE) | LOAN | 贷款产品类型 |
| CC (信用卡) | `t_cc_card` (card_type/card_level) | CREDIT_CARD | 信用卡产品 |
| WEALTH (理财) | `t_wm_product` | WEALTH | 理财产品主表 |
| CORE (核心银行) | 支付渠道配置 | PAYMENT | 支付产品 |

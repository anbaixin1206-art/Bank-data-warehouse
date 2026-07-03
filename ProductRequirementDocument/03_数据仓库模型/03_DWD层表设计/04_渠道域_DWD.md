# 04 渠道域 DWD 层表设计

> 所属：03_数据仓库模型 → DWD层表设计
> 上一文档：[03_产品域_DWD](./03_产品域_DWD.md)
> 下一文档：[05_存款域_DWD](./05_存款域_DWD.md)

---

## 1 渠道域模型概览

```
┌──────────────────────────────────────────────────────────┐
│                   渠道域 DWD 模型                           │
│                                                          │
│  ┌──────────────────┐                                    │
│  │   HUB_CHANNEL    │                                    │
│  │   channel_hash   │                                    │
│  │   channel_id     │                                    │
│  └────────┬─────────┘                                    │
│           │                                              │
│           └── SAT_CHANNEL_INFO (渠道属性)                   │
│               channel_name, channel_type,                │
│               is_electronic, support_biz_types...         │
│                                                          │
│  渠道分类:                                                │
│  PHYSICAL (物理渠道)                                       │
│  ├─ COUNTER_HIGH (高柜-现金)                               │
│  ├─ COUNTER_LOW (低柜-非现金)                              │
│  ├─ ATM (自动取款机)                                       │
│  ├─ CRS (存取款一体机)                                     │
│  └─ VTM/STM (智能柜员机)                                  │
│                                                          │
│  ELECTRONIC (电子渠道)                                     │
│  ├─ MOBILE_BANK (手机银行)                                │
│  ├─ PERSONAL_IB (个人网银)                                │
│  ├─ CORPORATE_IB (企业网银)                               │
│  └─ WECHAT_MINI (微信小程序)                              │
│                                                          │
│  POS (收单终端)                                            │
│  ├─ POS_TRADITIONAL (传统POS)                             │
│  ├─ POS_SMART (智能POS)                                   │
│  └─ MPOS (移动POS)                                        │
│                                                          │
│  THIRD_PARTY (第三方渠道)                                  │
│  ├─ ALIPAY (支付宝)                                       │
│  ├─ WECHAT_PAY (微信支付)                                 │
│  └─ UNIONPAY (银联)                                       │
└──────────────────────────────────────────────────────────┘
```

---

## 2 Hub 表

### 2.1 dwd_hub_channel — 渠道中心表

```sql
CREATE TABLE dwd_hub_channel (
    channel_hash_key    STRING      COMMENT '渠道哈希主键',
    channel_id           STRING      COMMENT '渠道编号',
    load_date            DATE        COMMENT '首次加载日期',
    record_source        STRING      COMMENT '首次来源系统',
    etl_time             TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '渠道中心表 (Hub) — 所有客户触达渠道'
PARTITIONED BY (dt STRING COMMENT '数据日期')
CLUSTERED BY (channel_hash_key) INTO 32 BUCKETS
STORED AS ORC
TBLPROPERTIES ('orc.compress'='ZLIB');
```

---

## 3 Satellite 表

### 3.1 dwd_sat_channel_info — 渠道属性卫星表

```sql
CREATE TABLE dwd_sat_channel_info (
    channel_hash_key    STRING      COMMENT '渠道哈希主键',
    load_date           DATE        COMMENT '记录生效日期',
    load_end_date       DATE        COMMENT '记录失效日期',
    is_current          BOOLEAN     COMMENT '是否当前记录',
    channel_name        STRING      COMMENT '渠道名称',
    channel_type        STRING      COMMENT '渠道大类: PHYSICAL/ELECTRONIC/POS/THIRD_PARTY',
    channel_subtype     STRING      COMMENT '渠道子类: COUNTER_HIGH/ATM/MOBILE_BANK/...',
    is_electronic       BOOLEAN     COMMENT '是否电子渠道',
    is_self_service     BOOLEAN     COMMENT '是否自助渠道',
    support_biz_types   STRING      COMMENT '支持的业务类型(JSON数组)',
    daily_limit         DECIMAL(18,2) COMMENT '日累计限额',
    single_limit        DECIMAL(18,2) COMMENT '单笔限额',
    auth_methods        STRING      COMMENT '认证方式: PASSWORD/SMS/BIOMETRIC/TOKEN',
    working_hours       STRING      COMMENT '营业时间(物理渠道): 09:00-17:00 / 7*24',
    status              STRING      COMMENT '状态: ACTIVE/CLOSED/MAINTENANCE',
    record_source       STRING      COMMENT '数据来源',
    hash_diff           STRING      COMMENT '属性MD5差异值',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '渠道属性卫星表 (Satellite) — 渠道主数据'
PARTITIONED BY (dt STRING COMMENT '数据日期')
CLUSTERED BY (channel_hash_key) INTO 32 BUCKETS
STORED AS ORC
TBLPROPERTIES ('orc.compress'='ZLIB');
```

---

## 4 渠道编码规范

| 渠道编码 | 渠道名称 | 大类 | 是否电子 | 典型认证 |
|---------|---------|------|---------|---------|
| `COUNTER_HIGH` | 高柜(现金) | PHYSICAL | ❌ | 身份证+密码 |
| `COUNTER_LOW` | 低柜(非现金) | PHYSICAL | ❌ | 身份证+密码 |
| `ATM` | 自动取款机 | PHYSICAL | ❌ | 卡+密码 |
| `CRS` | 存取款一体机 | PHYSICAL | ❌ | 卡+密码 |
| `VTM` | 智能柜员机 | PHYSICAL | ❌ | 身份证+人脸+密码 |
| `MOBILE_BANK` | 手机银行 | ELECTRONIC | ✅ | 指纹/人脸/密码 |
| `PERSONAL_IB` | 个人网银 | ELECTRONIC | ✅ | U盾/密码 |
| `CORPORATE_IB` | 企业网银 | ELECTRONIC | ✅ | U盾+多级授权 |
| `POS_TRAD` | 传统POS | POS | ❌ | 卡+密码+签名 |
| `POS_SMART` | 智能POS | POS | ✅ | 卡+密码/扫码 |
| `ALIPAY` | 支付宝 | THIRD_PARTY | ✅ | 支付宝认证 |
| `WECHAT_PAY` | 微信支付 | THIRD_PARTY | ✅ | 微信认证 |
| `UNIONPAY` | 银联 | THIRD_PARTY | ✅ | 银联认证 |

---

## 5 来源映射

| 来源系统 | 渠道信息 | 说明 |
|---------|---------|------|
| CORE (核心银行) | CHANNEL 字段 | 交易表中有渠道编码 |
| E-BANK (网银) | `channel` 字段 | 电子渠道类型 |
| ATM/POS | `atm_id`/`pos_id` | 终端编号关联渠道 |
| 渠道主数据 | 渠道配置表 | 统一管理渠道属性 |

> **注**：渠道域主要作为维度表使用，不产生独立的业务事实。渠道的交易数据通过各业务域的 Link 表（如 `LINK_TRANSACTION` 中关联 `HUB_CHANNEL`）体现。

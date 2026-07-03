# 11 风控域 DWD 层表设计

> 所属：03_数据仓库模型 → DWD层表设计
> 上一文档：[10_国际结算域_DWD](./10_国际结算域_DWD.md)
> 下一文档：[12_监管域_DWD](./12_监管域_DWD.md)

---

## 1 风控域特点

风控域与其他业务域不同——它**不产生新的业务数据**，而是基于其他业务域的数据进行风险分析和识别。

```
┌──────────────────────────────────────────────────────────────┐
│                   风控域 DWD 模型                               │
│                                                              │
│  输入 (来自其他域):                                             │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐     │
│  │ 交易明细  │  │ 客户属性  │  │ 贷款数据  │  │ 渠道日志  │     │
│  │ SAT_TRANS│  │ SAT_CUST │  │ SAT_LOAN │  │ E-BANK   │     │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘     │
│       └──────────────┼─────────────┼─────────────┘           │
│                      ▼                                        │
│  ┌────────────────────────────────────────────────────────┐  │
│  │              风控规则引擎 (Flink CEP + Spark SQL)        │  │
│  └────────────────────────┬───────────────────────────────┘  │
│                           ▼                                   │
│  输出 (风控域自有表):                                          │
│  ┌──────────────────┐  ┌──────────────────┐                  │
│  │HUB_RISK_ALERT    │  │HUB_RISK_CASE     │                  │
│  │(风险告警)         │  │(风险案例)         │                  │
│  └──────────────────┘  └──────────────────┘                  │
│                                                              │
│  ┌──────────────────┐  ┌──────────────────┐                  │
│  │SAT_RISK_SCORE    │  │SAT_AML_ALERT     │                  │
│  │(客户风险评分)      │  │(反洗钱告警)       │                  │
│  └──────────────────┘  └──────────────────┘                  │
└──────────────────────────────────────────────────────────────┘
```

---

## 2 Hub 表

### 2.1 dwd_hub_risk_alert — 风险告警中心表

```sql
CREATE TABLE dwd_hub_risk_alert (
    alert_hash_key      STRING      COMMENT '告警哈希主键',
    alert_id            STRING      COMMENT '告警编号',
    load_date           DATE        COMMENT '加载日期',
    record_source       STRING      COMMENT '来源系统: FLINK_CEP/SPARK_BATCH/AML_SYS',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '风险告警中心表 (Hub) — 所有风险告警统一标识'
PARTITIONED BY (dt STRING COMMENT '数据日期')
CLUSTERED BY (alert_hash_key) INTO 32 BUCKETS
STORED AS ORC;
```

### 2.2 dwd_hub_risk_case — 风险案例中心表

```sql
CREATE TABLE dwd_hub_risk_case (
    case_hash_key       STRING      COMMENT '案例哈希主键',
    case_id             STRING      COMMENT '案例编号',
    load_date           DATE        COMMENT '加载日期',
    record_source       STRING      COMMENT '来源系统',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '风险案例中心表 (Hub) — 反洗钱案例/欺诈案例'
PARTITIONED BY (dt STRING COMMENT '数据日期')
CLUSTERED BY (case_hash_key) INTO 32 BUCKETS
STORED AS ORC;
```

---

## 3 Satellite 表

### 3.1 dwd_sat_risk_alert — 风险告警属性卫星表

```sql
CREATE TABLE dwd_sat_risk_alert (
    alert_hash_key      STRING      COMMENT '告警哈希主键',
    load_date           DATE        COMMENT '加载日期',
    alert_time          TIMESTAMP   COMMENT '告警时间',
    alert_type          STRING      COMMENT '告警类型: LARGE_AMOUNT/FREQUENT/NIGHT_ABNORMAL/QUICK_IN_OUT/AML_SUSPICIOUS',
    risk_level          STRING      COMMENT '风险等级: HIGH/MEDIUM/LOW',
    customer_hash_key   STRING      COMMENT '关联客户哈希',
    transaction_hash_key STRING     COMMENT '关联交易哈希 (如有)',
    rule_id             STRING      COMMENT '触发的规则编号',
    rule_name           STRING      COMMENT '触发的规则名称',
    alert_detail        STRING      COMMENT '告警详情(JSON)',
    alert_status        STRING      COMMENT '告警状态: NEW/ACKNOWLEDGED/ESCALATED/RESOLVED/FALSE_POSITIVE',
    handled_by          STRING      COMMENT '处理人',
    handle_time         TIMESTAMP   COMMENT '处理时间',
    handle_comment      STRING      COMMENT '处理意见',
    record_source       STRING      COMMENT '数据来源',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '风险告警属性卫星表 (Satellite)'
PARTITIONED BY (dt STRING COMMENT '数据日期')
CLUSTERED BY (alert_hash_key) INTO 32 BUCKETS
STORED AS ORC;
```

### 3.2 dwd_sat_risk_score — 客户风险评分卫星表

```sql
CREATE TABLE dwd_sat_risk_score (
    customer_hash_key   STRING      COMMENT '客户哈希主键',
    load_date           DATE        COMMENT '评分日期',
    credit_score        DECIMAL(5,1) COMMENT '信用评分 300-850',
    fraud_score         DECIMAL(5,1) COMMENT '欺诈评分 0-100',
    aml_score           DECIMAL(5,1) COMMENT '反洗钱风险评分 0-100',
    operation_risk_score DECIMAL(5,1) COMMENT '操作风险评分 0-100',
    overall_risk_level  STRING      COMMENT '综合风险等级: LOW/MEDIUM/MEDIUM_HIGH/HIGH/FORBIDDEN',
    score_model_version STRING      COMMENT '评分模型版本',
    record_source       STRING      COMMENT '数据来源',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '客户风险评分卫星表 (Satellite) — 每日评分快照'
PARTITIONED BY (dt STRING COMMENT '评分日期')
CLUSTERED BY (customer_hash_key) INTO 32 BUCKETS
STORED AS ORC;
```

---

## 4 风控规则配置表（辅助表）

```sql
CREATE TABLE dwd_config_risk_rule (
    rule_id             STRING      COMMENT '规则编号',
    rule_name           STRING      COMMENT '规则名称',
    rule_category       STRING      COMMENT '规则分类: LARGE_AMT/FREQUENT/NIGHT/AML/PATTERN',
    rule_description    STRING      COMMENT '规则描述',
    threshold_value     STRING      COMMENT '阈值(JSON)',
    time_window_sec     INT         COMMENT '时间窗口(秒)',
    is_active           BOOLEAN     COMMENT '是否启用',
    severity            STRING      COMMENT '严重级别: CRITICAL/HIGH/MEDIUM/LOW',
    action_type         STRING      COMMENT '动作类型: ALERT/BLOCK/MANUAL_REVIEW',
    effective_date      DATE        COMMENT '生效日期',
    expire_date         DATE        COMMENT '失效日期',
    etl_time            TIMESTAMP   COMMENT 'ETL时间'
)
COMMENT '风控规则配置表'
STORED AS ORC;
```

---

## 5 核心指标定义

| 指标 | 定义 | 计算来源 |
|------|------|---------|
| 实时告警数 | COUNT(alert_id) WHERE alert_time 在统计期内 | SAT_RISK_ALERT |
| 告警确认率 | ACKNOWLEDGED+RESOLVED / 总告警 | SAT_RISK_ALERT |
| 有效告警率 | 1 - FALSE_POSITIVE / 总告警 | SAT_RISK_ALERT |
| 高风险客户数 | COUNT(customer) WHERE overall_risk_level IN ('HIGH','FORBIDDEN') | SAT_RISK_SCORE |
| 反洗钱案例数 | COUNT(case_id) | HUB_RISK_CASE |
| 大额交易笔数 | COUNT WHERE alert_type='LARGE_AMOUNT' | SAT_RISK_ALERT |

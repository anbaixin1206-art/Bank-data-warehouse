# 04 风险管理 ADS 层表设计

> 所属：03_数据仓库模型 → ADS层表设计
> 上一文档：[03_客户画像_ADS](./03_客户画像_ADS.md)
> 下一文档：[05_自助BI_ADS](./05_自助BI_ADS.md)

---

## 1 ads_risk_monitor — 风险监控实时宽表

```sql
CREATE TABLE ads_risk_monitor (
    dt                  STRING      COMMENT '统计日期',
    alert_id            STRING      COMMENT '告警编号',
    alert_time          TIMESTAMP   COMMENT '告警时间',
    alert_type          STRING      COMMENT '告警类型',
    risk_level          STRING      COMMENT '风险等级',
    customer_hash_key   STRING      COMMENT '关联客户',
    customer_name       STRING      COMMENT '客户姓名(脱敏)',
    transaction_hash_key STRING     COMMENT '关联交易',
    trans_amt           DECIMAL(18,2) COMMENT '交易金额',
    trans_type          STRING      COMMENT '交易类型',
    channel_name        STRING      COMMENT '交易渠道',
    rule_name           STRING      COMMENT '触发规则',
    alert_detail        STRING      COMMENT '告警详情(JSON)',
    alert_status        STRING      COMMENT '处理状态',
    handler             STRING      COMMENT '处理人',
    handle_time         TIMESTAMP   COMMENT '处理时间',
    handle_result       STRING      COMMENT '处理结果',
    is_escalated        BOOLEAN     COMMENT '是否已升级',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '风险监控实时宽表 — 风控大屏 + 告警列表'
PARTITIONED BY (dt STRING)
STORED AS ORC;
```

---

## 2 ads_risk_customer_rating — 客户风险评级宽表

```sql
CREATE TABLE ads_risk_customer_rating (
    dt                  STRING      COMMENT '评估日期',
    customer_hash_key   STRING      COMMENT '客户哈希主键',
    customer_name       STRING      COMMENT '客户姓名(脱敏)',
    customer_type       STRING      COMMENT '客户类型',
    -- 评级结果
    final_rating        STRING      COMMENT '最终评级: AAA/AA/A/BBB/BB/B/C/D',
    credit_score        DECIMAL(5,1) COMMENT '信用评分 300-850',
    pd                  DECIMAL(7,6) COMMENT '违约概率 PD',
    lgd                 DECIMAL(5,4) COMMENT '违约损失率 LGD',
    ead                 DECIMAL(18,2) COMMENT '违约风险暴露 EAD',
    expected_loss       DECIMAL(18,2) COMMENT '预期损失 EL = PD*LGD*EAD',
    -- 评级因素
    financial_score     DECIMAL(5,1) COMMENT '财务状况评分',
    behavior_score      DECIMAL(5,1) COMMENT '行为评分',
    industry_score      DECIMAL(5,1) COMMENT '行业评分',
    guarantee_score     DECIMAL(5,1) COMMENT '担保评分',
    -- 限制
    rating_model        STRING      COMMENT '评级模型版本',
    rating_date         DATE        COMMENT '评级日期',
    next_review_date    DATE        COMMENT '下次重评日期',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '客户风险评级宽表 — 信用风险管理'
PARTITIONED BY (dt STRING)
STORED AS ORC;
```

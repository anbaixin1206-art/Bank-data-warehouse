# 07 风控汇总 DWS 层表设计

> 所属：03_数据仓库模型 → DWS层表设计
> 上一文档：[06_理财汇总_DWS](./06_理财汇总_DWS.md)
> 下一文档：[08_财务汇总_DWS](./08_财务汇总_DWS.md)

---

## 1 dws_risk_daily_monitor — 风险日监控汇总表

```sql
CREATE TABLE dws_risk_daily_monitor (
    dt                  STRING      COMMENT '统计日期',
    alert_type          STRING      COMMENT '告警类型',
    risk_level          STRING      COMMENT '风险等级',
    -- 告警指标
    alert_cnt           BIGINT      COMMENT '告警数',
    high_risk_cnt       BIGINT      COMMENT '高风险告警数',
    handled_cnt         BIGINT      COMMENT '已处理告警数',
    false_positive_cnt  BIGINT      COMMENT '误报告警数',
    avg_handle_minutes  DECIMAL(8,2) COMMENT '平均处理时长(分钟)',
    unhandled_cnt       BIGINT      COMMENT '未处理告警数',
    -- 案例指标
    new_case_cnt        BIGINT      COMMENT '新创建案例数',
    escalated_case_cnt  BIGINT      COMMENT '升级案例数',
    closed_case_cnt     BIGINT      COMMENT '关闭案例数',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '风险日监控汇总表'
PARTITIONED BY (dt STRING)
STORED AS ORC;
```

---

## 2 dws_risk_large_exposure — 大额风险暴露表

```sql
CREATE TABLE dws_risk_large_exposure (
    dt                  STRING      COMMENT '统计日期',
    customer_hash_key   STRING      COMMENT '客户',
    customer_type       STRING      COMMENT '客户类型',
    -- 风险暴露
    total_exposure      DECIMAL(18,2) COMMENT '总风险暴露(贷款+债券+表外)',
    loan_exposure       DECIMAL(18,2) COMMENT '贷款风险暴露',
    off_balance_exposure DECIMAL(18,2) COMMENT '表外风险暴露',
    -- 集中度
    single_exposure_ratio DECIMAL(5,2) COMMENT '单一客户集中度(占总资产%)',
    top10_ratio         DECIMAL(5,2) COMMENT '最大十家客户集中度',
    -- 关联
    related_group_cnt   INT         COMMENT '关联方数量',
    related_group_exposure DECIMAL(18,2) COMMENT '关联方合计暴露',
    -- 缓释
    collateral_amt      DECIMAL(18,2) COMMENT '合格抵质押品金额',
    net_exposure        DECIMAL(18,2) COMMENT '净风险暴露(总暴露-缓释)',
    regulation_limit    DECIMAL(18,2) COMMENT '监管限额',
    is_breached         BOOLEAN     COMMENT '是否突破限额',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '大额风险暴露汇总表'
PARTITIONED BY (dt STRING)
STORED AS ORC;
```

---

## 3 dws_risk_liquidity — 流动性风险指标表

```sql
CREATE TABLE dws_risk_liquidity (
    dt                  STRING      COMMENT '统计日期',
    -- 流动性比率
    current_ratio       DECIMAL(5,2) COMMENT '流动性比率(流动资产/流动负债)',
    lcr                 DECIMAL(5,2) COMMENT '流动性覆盖率 LCR %',
    nsfr                DECIMAL(5,2) COMMENT '净稳定资金比率 NSFR %',
    -- 缺口
    liquidity_gap_1d    DECIMAL(18,2) COMMENT '1日内流动性缺口',
    liquidity_gap_7d    DECIMAL(18,2) COMMENT '7日内流动性缺口',
    liquidity_gap_30d   DECIMAL(18,2) COMMENT '30日内流动性缺口',
    -- 集中度
    top10_deposit_ratio DECIMAL(5,2) COMMENT '前十大存款客户占比',
    interbank_ratio     DECIMAL(5,2) COMMENT '同业负债依赖度 %',
    -- 优质流动性资产
    hqla_amt            DECIMAL(18,2) COMMENT '优质流动性资产(HQLA)',
    cash_inflow_30d     DECIMAL(18,2) COMMENT '30日内现金流入',
    cash_outflow_30d    DECIMAL(18,2) COMMENT '30日内现金流出',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '流动性风险指标汇总表'
PARTITIONED BY (dt STRING)
STORED AS ORC;
```

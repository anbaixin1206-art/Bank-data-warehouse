# 01 管理驾驶舱 ADS 层表设计

> 所属：03_数据仓库模型 → ADS层表设计
> 上一文档：[08_财务汇总_DWS](../04_DWS层表设计/08_财务汇总_DWS.md)
> 下一文档：[02_监管报送_ADS](./02_监管报送_ADS.md)

---

## 1 ads_mgmt_kpi — 管理驾驶舱核心KPI表

```sql
CREATE TABLE ads_mgmt_kpi (
    dt                  STRING      COMMENT '统计日期',
    kpi_code            STRING      COMMENT '指标编码',
    kpi_name            STRING      COMMENT '指标名称',
    kpi_category        STRING      COMMENT '指标分类: MANAGEMENT/DEPOSIT/LOAN/PAYMENT/CUSTOMER/RISK',
    kpi_value           DECIMAL(18,4) COMMENT '指标值',
    kpi_unit            STRING      COMMENT '单位: YUAN/WAN_YUAN/YI_YUAN/PERCENT/COUNT',
    yoy_change          DECIMAL(5,2) COMMENT '同比变动 %',
    mom_change          DECIMAL(5,2) COMMENT '环比变动 %',
    target_value        DECIMAL(18,4) COMMENT '目标值',
    complete_rate       DECIMAL(5,2) COMMENT '完成率 %',
    trend_direction     STRING      COMMENT '趋势方向: UP/DOWN/FLAT',
    is_alert            BOOLEAN     COMMENT '是否告警(未达目标)',
    display_order       INT         COMMENT '显示顺序',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '管理驾驶舱核心KPI表 — 经营总览大宽表'
PARTITIONED BY (dt STRING COMMENT '统计日期')
STORED AS ORC;
```

**KPI 指标体系**：

| KPI编码 | KPI名称 | 来源DWS | 刷新 |
|--------|--------|---------|------|
| `TOTAL_ASSET` | 总资产 | `dws_fin_daily_balance_sheet` | T+1 |
| `TOTAL_LIABILITY` | 总负债 | `dws_fin_daily_balance_sheet` | T+1 |
| `NET_PROFIT` | 净利润 | `dws_fin_daily_pnl` | T+1 |
| `DEPOSIT_BALANCE` | 存款余额 | `dws_dep_daily_bal` | T+1 |
| `LOAN_BALANCE` | 贷款余额 | `dws_loan_daily_bal` | T+1 |
| `LDR` | 存贷比 | 计算: LOAN/DEPOSIT | T+1 |
| `NPL_RATIO` | 不良贷款率 | `dws_loan_classification_summary` | T+1 |
| `PROVISION_COVERAGE` | 拨备覆盖率 | `dws_loan_classification_summary` | T+1 |
| `TOTAL_CUSTOMER` | 总客户数 | `dws_cust_daily_summary` | T+1 |
| `NEW_CUSTOMER` | 新增客户数 | `dws_cust_acquisition` | T+1 |
| `DAILY_TXN_CNT` | 当日交易笔数 | `dws_pay_daily_channel` | 实时(Redis) |
| `DAILY_TXN_AMT` | 当日交易金额 | `dws_pay_daily_channel` | 实时(Redis) |
| `RISK_ALERT_CNT` | 风控实时告警数 | `dws_risk_daily_monitor` | 实时(Redis) |
| `COST_INCOME_RATIO` | 成本收入比 | `dws_fin_org_profit` | T+1 |
| `ROA` | 资产回报率 | `dws_fin_org_profit` | T+1 |
| `LCR` | 流动性覆盖率 | `dws_risk_liquidity` | T+1 |

---

## 2 ads_mgmt_branch_ranking — 分支机构排名表

```sql
CREATE TABLE ads_mgmt_branch_ranking (
    dt                  STRING      COMMENT '统计日期',
    org_hash_key        STRING      COMMENT '机构',
    org_level           INT         COMMENT '机构层级',
    -- 排名指标
    deposit_rank        INT         COMMENT '存款排名',
    loan_rank           INT         COMMENT '贷款排名',
    profit_rank         INT         COMMENT '利润排名',
    customer_rank       INT         COMMENT '客户数排名',
    comprehensive_score DECIMAL(8,2) COMMENT '综合评分',
    comprehensive_rank  INT         COMMENT '综合排名',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '分支机构排名表'
PARTITIONED BY (dt STRING)
STORED AS ORC;
```

---

## 3 ads_mgmt_trend — 趋势分析表

```sql
CREATE TABLE ads_mgmt_trend (
    dt                  STRING      COMMENT '统计日期',
    kpi_code            STRING      COMMENT '指标编码',
    kpi_value           DECIMAL(18,4) COMMENT '当日值',
    ma_7d               DECIMAL(18,4) COMMENT '7日移动平均',
    ma_30d              DECIMAL(18,4) COMMENT '30日移动平均',
    max_30d             DECIMAL(18,4) COMMENT '30日最高',
    min_30d             DECIMAL(18,4) COMMENT '30日最低',
    std_30d             DECIMAL(18,4) COMMENT '30日标准差',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '趋势分析表 — 支持折线图展示'
PARTITIONED BY (dt STRING)
STORED AS ORC;
```

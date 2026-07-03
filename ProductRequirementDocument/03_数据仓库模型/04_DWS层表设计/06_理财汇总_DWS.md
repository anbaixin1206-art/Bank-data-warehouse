# 06 理财汇总 DWS 层表设计

> 所属：03_数据仓库模型 → DWS层表设计
> 上一文档：[05_信用卡汇总_DWS](./05_信用卡汇总_DWS.md)
> 下一文档：[07_风控汇总_DWS](./07_风控汇总_DWS.md)

---

## 1 dws_wealth_daily_summary — 理财日汇总表

```sql
CREATE TABLE dws_wealth_daily_summary (
    dt                  STRING      COMMENT '统计日期',
    product_type        STRING      COMMENT '产品类型: MONEY_FUND/BOND_WM/NAV_WM/TRUST/INSURANCE',
    risk_level          STRING      COMMENT '风险等级: R1/R2/R3/R4/R5',
    is_open_end         BOOLEAN     COMMENT '是否开放式',
    -- 规模指标
    product_cnt         INT         COMMENT '在售产品数',
    total_scale         DECIMAL(18,2) COMMENT '产品总规模(市值)',
    holding_cust_cnt    BIGINT      COMMENT '持仓客户数',
    avg_holding         DECIMAL(18,2) COMMENT '人均持仓市值',
    -- 交易指标
    subscribe_cnt       BIGINT      COMMENT '认购/申购笔数',
    subscribe_amt       DECIMAL(18,2) COMMENT '认购/申购金额',
    redeem_cnt          BIGINT      COMMENT '赎回笔数',
    redeem_amt          DECIMAL(18,2) COMMENT '赎回金额',
    net_subscribe       DECIMAL(18,2) COMMENT '净申购(申购-赎回)',
    dividend_amt        DECIMAL(18,2) COMMENT '分红金额',
    -- 收益指标
    total_return        DECIMAL(18,2) COMMENT '当日总收益',
    avg_return_rate     DECIMAL(9,6) COMMENT '平均年化收益率',
    -- 收入指标
    fee_income          DECIMAL(18,2) COMMENT '手续费收入(认购费+赎回费+管理费)',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '理财日汇总事实表'
PARTITIONED BY (dt STRING COMMENT '统计日期')
STORED AS ORC;
```

---

## 2 dws_wealth_product_performance — 理财产品表现表

```sql
CREATE TABLE dws_wealth_product_performance (
    dt                  STRING      COMMENT '统计日期',
    product_hash_key    STRING      COMMENT '产品',
    nav                 DECIMAL(18,6) COMMENT '最新净值',
    nav_change_daily    DECIMAL(18,6) COMMENT '日净值变化',
    return_rate_daily   DECIMAL(9,6) COMMENT '日收益率',
    return_rate_1m      DECIMAL(9,6) COMMENT '近1月收益率',
    return_rate_3m      DECIMAL(9,6) COMMENT '近3月收益率',
    return_rate_1y      DECIMAL(9,6) COMMENT '近1年收益率',
    return_rate_ytd     DECIMAL(9,6) COMMENT '年初至今收益率',
    total_scale         DECIMAL(18,2) COMMENT '产品规模',
    scale_change_daily  DECIMAL(18,2) COMMENT '规模日变动',
    holding_cust_cnt    BIGINT      COMMENT '持仓客户数',
    benchmark_return    DECIMAL(9,6) COMMENT '业绩基准收益率',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '理财产品表现分析表'
PARTITIONED BY (dt STRING)
STORED AS ORC;
```

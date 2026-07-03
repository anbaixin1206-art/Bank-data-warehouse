# 08 财务汇总 DWS 层表设计

> 所属：03_数据仓库模型 → DWS层表设计
> 上一文档：[07_风控汇总_DWS](./07_风控汇总_DWS.md)
> 下一文档：[ADS层表设计 - 管理驾驶舱](../05_ADS层表设计/01_管理驾驶舱_ADS.md)

---

## 1 dws_fin_daily_balance_sheet — 资产负债日报表

```sql
CREATE TABLE dws_fin_daily_balance_sheet (
    dt                  STRING      COMMENT '报表日期',
    item_code           STRING      COMMENT '项目代码(对应监管1104 G01)',
    item_name           STRING      COMMENT '项目名称',
    item_category       STRING      COMMENT '项目大类: ASSET/LIABILITY/EQUITY',
    parent_code         STRING      COMMENT '上级项目代码',
    item_level          INT         COMMENT '项目层级',
    balance             DECIMAL(18,2) COMMENT '余额',
    balance_prev_day    DECIMAL(18,2) COMMENT '上日余额',
    balance_prev_month  DECIMAL(18,2) COMMENT '上月余额',
    balance_prev_year   DECIMAL(18,2) COMMENT '上年余额',
    change_daily        DECIMAL(18,2) COMMENT '日变动',
    change_monthly      DECIMAL(18,2) COMMENT '月变动',
    change_yearly       DECIMAL(18,2) COMMENT '年变动',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '资产负债日报表 — 按1104 G01结构组织'
PARTITIONED BY (dt STRING COMMENT '报表日期')
STORED AS ORC;
```

---

## 2 dws_fin_daily_pnl — 损益日报表

```sql
CREATE TABLE dws_fin_daily_pnl (
    dt                  STRING      COMMENT '报表日期',
    item_code           STRING      COMMENT '项目代码',
    item_name           STRING      COMMENT '损益项目名称',
    item_category       STRING      COMMENT '损益大类: INCOME/EXPENSE',
    sub_category        STRING      COMMENT '子类: INTEREST/FEE/TRADING/OTHER',
    current_day_amt     DECIMAL(18,2) COMMENT '当日发生额',
    current_month_amt   DECIMAL(18,2) COMMENT '本月累计',
    current_quarter_amt DECIMAL(18,2) COMMENT '本季累计',
    current_year_amt    DECIMAL(18,2) COMMENT '本年累计',
    prev_year_same_period DECIMAL(18,2) COMMENT '上年同期',
    yoy_change_pct      DECIMAL(5,2) COMMENT '同比变动 %',
    budget_amt          DECIMAL(18,2) COMMENT '预算额',
    budget_complete_pct DECIMAL(5,2) COMMENT '预算完成率 %',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '损益日报表'
PARTITIONED BY (dt STRING COMMENT '报表日期')
STORED AS ORC;
```

---

## 3 dws_fin_org_profit — 机构盈利分析表

```sql
CREATE TABLE dws_fin_org_profit (
    dt                  STRING      COMMENT '统计日期',
    org_hash_key        STRING      COMMENT '机构',
    -- 收入
    interest_income     DECIMAL(18,2) COMMENT '利息收入',
    fee_income          DECIMAL(18,2) COMMENT '手续费及佣金收入',
    trading_income      DECIMAL(18,2) COMMENT '交易性收入',
    other_income        DECIMAL(18,2) COMMENT '其他收入',
    total_income        DECIMAL(18,2) COMMENT '总收入',
    -- 支出
    interest_expense    DECIMAL(18,2) COMMENT '利息支出',
    fee_expense         DECIMAL(18,2) COMMENT '手续费及佣金支出',
    operating_expense   DECIMAL(18,2) COMMENT '营业费用',
    provision_expense   DECIMAL(18,2) COMMENT '资产减值损失',
    total_expense       DECIMAL(18,2) COMMENT '总支出',
    -- 利润
    net_profit          DECIMAL(18,2) COMMENT '净利润',
    cost_income_ratio   DECIMAL(5,2) COMMENT '成本收入比 %',
    -- 效率
    avg_asset           DECIMAL(18,2) COMMENT '平均资产',
    roa                 DECIMAL(5,2) COMMENT '资产回报率 ROA %',
    ftp_transfer_amt    DECIMAL(18,2) COMMENT 'FTP转移定价调整',
    economic_profit     DECIMAL(18,2) COMMENT '经济利润(FTP调整后)',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '机构盈利分析表'
PARTITIONED BY (dt STRING)
STORED AS ORC;
```

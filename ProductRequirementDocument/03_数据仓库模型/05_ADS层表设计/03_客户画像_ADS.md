# 03 客户画像 ADS 层表设计

> 所属：03_数据仓库模型 → ADS层表设计
> 上一文档：[02_监管报送_ADS](./02_监管报送_ADS.md)
> 下一文档：[04_风险管理_ADS](./04_风险管理_ADS.md)

---

## 1 ads_cust_360_view — 客户360°统一视图宽表

```sql
CREATE TABLE ads_cust_360_view (
    dt                  STRING      COMMENT '快照日期',
    customer_hash_key   STRING      COMMENT '客户哈希主键',
    customer_id         STRING      COMMENT '客户号',
    -- 基础属性
    cust_name           STRING      COMMENT '姓名/企业名称',
    cust_type           STRING      COMMENT '客户类型',
    gender              STRING      COMMENT '性别',
    age                 INT         COMMENT '年龄',
    age_group           STRING      COMMENT '年龄段',
    cust_level          STRING      COMMENT '客户等级',
    open_years          INT         COMMENT '开户年限',
    province            STRING      COMMENT '省份',
    city                STRING      COMMENT '城市',
    -- 资产维度
    total_aum           DECIMAL(18,2) COMMENT '总AUM',
    deposit_bal         DECIMAL(18,2) COMMENT '存款余额',
    loan_bal            DECIMAL(18,2) COMMENT '贷款余额',
    wealth_market_value DECIMAL(18,2) COMMENT '理财市值',
    aum_level           STRING      COMMENT 'AUM分层: UHNW/HNW/AFFLUENT/MASS/LONG_TAIL',
    -- 交易行为维度
    ytd_txn_cnt         BIGINT      COMMENT '本年累计交易笔数',
    ytd_txn_amt         DECIMAL(18,2) COMMENT '本年累计交易金额',
    active_level        STRING      COMMENT '活跃度: HIGH/MEDIUM/LOW/SILENT',
    channel_preference  STRING      COMMENT '渠道偏好',
    transaction_habit   STRING      COMMENT '交易习惯: DAY/NIGHT/WEEKEND/REGULAR',
    is_cross_bank_user  BOOLEAN     COMMENT '是否使用跨行服务',
    is_overseas_user    BOOLEAN     COMMENT '是否有境外交易',
    -- 产品持有维度
    has_deposit         BOOLEAN     COMMENT '是否持有存款',
    has_loan            BOOLEAN     COMMENT '是否持有贷款',
    has_credit_card     BOOLEAN     COMMENT '是否持有信用卡',
    has_wealth          BOOLEAN     COMMENT '是否持有理财',
    product_holding_cnt INT         COMMENT '产品持有数',
    -- 风险维度
    credit_score        DECIMAL(5,1) COMMENT '信用评分',
    risk_level          STRING      COMMENT '风险等级',
    has_overdue         BOOLEAN     COMMENT '是否有逾期记录',
    has_aml_alert       BOOLEAN     COMMENT '是否有反洗钱告警',
    -- 价值维度
    value_score         INT         COMMENT '客户价值评分',
    life_cycle          STRING      COMMENT '生命周期: NEW/ACTIVE/MATURE/DECLINE/CHURN',
    churn_probability   DECIMAL(5,4) COMMENT '流失概率',
    next_best_product   STRING      COMMENT '下一最佳产品推荐',
    lifetime_value      DECIMAL(18,2) COMMENT '客户生命周期价值(LTV)',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '客户360°统一视图宽表 — 画像与营销核心数据'
PARTITIONED BY (dt STRING COMMENT '快照日期')
CLUSTERED BY (customer_hash_key) INTO 16 BUCKETS
STORED AS ORC;
```

---

## 2 ads_cust_label — 客户标签表

```sql
CREATE TABLE ads_cust_label (
    dt                  STRING      COMMENT '快照日期',
    customer_hash_key   STRING      COMMENT '客户哈希主键',
    label_category      STRING      COMMENT '标签分类: BASIC/ASSET/BEHAVIOR/CREDIT/VALUE',
    label_name          STRING      COMMENT '标签名称',
    label_value         STRING      COMMENT '标签值',
    label_weight        DECIMAL(5,2) COMMENT '标签权重',
    label_source        STRING      COMMENT '标签来源(计算规则)',
    effective_date      DATE        COMMENT '标签生效日期',
    expire_date         DATE        COMMENT '标签失效日期',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '客户标签表 — 标签明细(一个客户多行)'
PARTITIONED BY (dt STRING)
STORED AS ORC;
```

---

## 3 ads_cust_segment — 客户分层分析表

```sql
CREATE TABLE ads_cust_segment (
    dt                  STRING      COMMENT '统计日期',
    segment_type        STRING      COMMENT '分层维度: AUM/AGE/RISK/ACTIVITY/VALUE',
    segment_name        STRING      COMMENT '分层名称',
    cust_cnt            BIGINT      COMMENT '客户数',
    cust_pct            DECIMAL(5,2) COMMENT '客户数占比',
    total_aum           DECIMAL(18,2) COMMENT 'AUM合计',
    aum_pct             DECIMAL(5,2) COMMENT 'AUM占比',
    avg_deposit         DECIMAL(18,2) COMMENT '人均存款',
    avg_loan            DECIMAL(18,2) COMMENT '人均贷款',
    avg_product_cnt     DECIMAL(8,2) COMMENT '人均产品持有数',
    avg_active_days     DECIMAL(8,2) COMMENT '月均活跃天数',
    churn_rate          DECIMAL(5,2) COMMENT '流失率 %',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '客户分层分析表 — 用于透视分析'
PARTITIONED BY (dt STRING)
STORED AS ORC;
```

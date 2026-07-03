# 12 监管域 DWD 层表设计

> 所属：03_数据仓库模型 → DWD层表设计
> 上一文档：[11_风控域_DWD](./11_风控域_DWD.md)
> 下一文档：[13_财务域_DWD](./13_财务域_DWD.md)

---

## 1 监管域特点

监管域在 DWD 层**不创建新的业务实体 Hub**，而是提供**面向报送的统一明细视图**。报送数据直接基于其他业务域的 DWD 表加工。

```
┌──────────────────────────────────────────────────────────────┐
│                  监管域数据加工路径                              │
│                                                              │
│  DWD 业务域────→ DWS 汇总层────→ ADS 监管报送                   │
│                                                              │
│  EAST 报送示例:                                                │
│  dwd_hub_customer + dwd_sat_customer_info                    │
│    ──→ ads_reg_east_t_01 (个人基础信息)                        │
│                                                              │
│  dwd_sat_account_bal + dwd_sat_account_status                │
│    ──→ ads_reg_east_t_02_1 (个人活期存款分户账)                 │
│                                                              │
│  dwd_sat_transaction                                         │
│    ──→ ads_reg_east_t_50_1 (个人活期存款交易明细)               │
│                                                              │
│  1104 报送示例:                                                │
│  dws_dep_daily_bal + dws_loan_daily_bal + dws_fin_daily_pnl  │
│    ──→ ads_reg_1104_g01 (资产负债表)                           │
└──────────────────────────────────────────────────────────────┘
```

---

## 2 监管报送配置表（DWD 辅助表）

### 2.1 dwd_config_reg_report — 监管报送定义表

```sql
CREATE TABLE dwd_config_reg_report (
    report_code         STRING      COMMENT '报表编码: 1104_G01/EAST_T_01/AML_LARGE/...',
    report_name         STRING      COMMENT '报表名称',
    reg_body            STRING      COMMENT '监管机构: CBIRC/PBOC/SAFE',
    report_frequency    STRING      COMMENT '报送频次: DAILY/MONTHLY/QUARTERLY/SEMI_ANNUAL/ANNUAL/AD_HOC',
    due_day             STRING      COMMENT '报送日期规则: "次月10日"/"T+1"',
    data_period_type    STRING      COMMENT '数据期间: CURRENT_MONTH/LAST_MONTH/CURRENT_QUARTER/YTD',
    is_active           BOOLEAN     COMMENT '是否启用',
    report_desc         STRING      COMMENT '报表说明',
    etl_time            TIMESTAMP   COMMENT 'ETL时间'
)
COMMENT '监管报送定义配置表'
STORED AS ORC;
```

### 2.2 dwd_config_reg_field_mapping — 报送字段映射表

```sql
CREATE TABLE dwd_config_reg_field_mapping (
    report_code         STRING      COMMENT '报表编码',
    field_seq           INT         COMMENT '字段序号',
    field_name          STRING      COMMENT '报送字段名',
    field_cn_name       STRING      COMMENT '字段中文名',
    field_type          STRING      COMMENT '字段类型: STRING/DECIMAL/DATE/INT',
    field_length        INT         COMMENT '字段长度',
    is_required         BOOLEAN     COMMENT '是否必填',
    source_table        STRING      COMMENT '来源DWD/DWS表',
    source_column       STRING      COMMENT '来源字段',
    transform_rule      STRING      COMMENT '转换规则(默认直接映射)',
    code_mapping        STRING      COMMENT '码值映射(JSON): {"01":"活期","02":"定期"}',
    default_value       STRING      COMMENT '默认值',
    validation_rule     STRING      COMMENT '校验规则',
    etl_time            TIMESTAMP   COMMENT 'ETL时间'
)
COMMENT '监管报送字段映射配置表 — 源表字段→报送字段'
STORED AS ORC;
```

---

## 3 重点报送类型与数据映射

### 3.1 EAST 监管标准化数据

| EAST表编码 | 报送内容 | 主要来源DWD表 | 报送粒度 |
|-----------|---------|-------------|---------|
| T_01 | 个人基础信息 | `dwd_hub_customer` + `dwd_sat_customer_info` | 客户级 |
| T_02_1 | 个人活期存款分户账 | `dwd_sat_account_bal` | 账户级 |
| T_03_1 | 个人定期存款分户账 | `dwd_sat_account_bal` + `dwd_sat_account_status` | 账户级 |
| T_04_1 | 个人贷款合同 | `dwd_sat_loan_contract_info` | 合同级 |
| T_05_1 | 个人贷款借据 | `dwd_sat_loan_drawdown` | 借据级 |
| T_06 | 信用卡账户信息 | `dwd_sat_cc_card_info` | 卡片级 |
| T_50_1 | 个人活期存款交易明细 | `dwd_sat_transaction` | 交易级 |
| T_51_1 | 个人贷款还款明细 | `dwd_sat_loan_repayment` | 交易级 |
| T_52 | 信用卡消费明细 | `dwd_sat_cc_transaction` | 交易级 |
| T_53 | 理财交易明细 | `dwd_sat_wealth_transaction` | 交易级 |

### 3.2 1104 非现场监管报表

| 报表编码 | 报表名称 | 主要来源DWS表 | 频次 |
|---------|---------|-------------|------|
| G01 | 资产负债项目统计表 | `dws_fin_daily_balance_sheet` | 月 |
| G01_I | 表外业务情况表 | `dws_fin_off_balance` | 季 |
| G03 | 各项贷款情况统计表 | `dws_loan_daily_bal` | 月 |
| G04 | 各项存款情况统计表 | `dws_dep_daily_bal` | 月 |
| G05 | 个人贷款情况统计表 | `dws_loan_daily_bal` (个人) | 月 |
| G11 | 资产质量五级分类情况表 | `dws_risk_loan_classification` | 月 |
| G12 | 贷款质量迁徙情况表 | `dws_risk_class_migration` | 季 |
| G14 | 大额风险暴露统计表 | `dws_risk_large_exposure` | 季 |
| G21 | 流动性比例监测表 | `dws_risk_liquidity` | 月 |
| G25 | 流动性期限缺口统计表 | `dws_risk_liquidity_gap` | 季 |

---

## 4 核心报送流程

```
数据准备:
  T-1日  DWD层日批完成
         ↓
  报送日  DWS层汇总完毕
         ↓
         ADS监管报送生成 (Spark SQL)
         │
         ├─ 数据校验 (参照完整性/码值有效性/必填字段)
         │    ├─ PASS → 导出文件
         │    └─ FAIL → 修复后重跑
         │
         ├─ 导出 (CSV/TXT/XML 按监管要求格式)
         │
         └─ 报送记录 (meta_reg_submit_log)
              submit_id, report_code, submit_date, record_count, status
```

---

## 5 数据校验规则（通用）

| 校验类型 | 说明 | 处理方式 |
|---------|------|---------|
| 必填字段非空 | 监管要求必填字段不能为 NULL | 告警 + 填充默认值 |
| 码值合法性 | 字段取值必须在监管码值范围内 | 告警 + 映射为"其他" |
| 金额非负 | 余额/金额类字段不能为负 | 告警 + 人工确认 |
| 主键唯一性 | 报送文件主键不能重复 | 阻断 + 去重重跑 |
| 跨表一致性 | 同一实体在不同报送表中属性一致 | 告警 |
| 时间有效性 | 日期字段在合理范围内 | 告警 |

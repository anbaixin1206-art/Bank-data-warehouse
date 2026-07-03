# 02 监管报送 ADS 层表设计

> 所属：03_数据仓库模型 → ADS层表设计
> 上一文档：[01_管理驾驶舱_ADS](./01_管理驾驶舱_ADS.md)
> 下一文档：[03_客户画像_ADS](./03_客户画像_ADS.md)

---

## 1 ads_reg_1104 — 1104非现场监管报表

### 1.1 ads_reg_1104_g01 — G01 资产负债项目统计表

```sql
CREATE TABLE ads_reg_1104_g01 (
    dt                  STRING      COMMENT '报表日期(数据日期)',
    report_month         STRING      COMMENT '报表月份 YYYYMM',
    item_seq            INT         COMMENT '项目序号',
    item_code           STRING      COMMENT '项目代码(A/B/C列)',
    item_name           STRING      COMMENT '项目名称',
    item_level          INT         COMMENT '层级',
    parent_item_seq     INT         COMMENT '上级序号',
    -- 核心数据
    rmb_bal             DECIMAL(18,2) COMMENT '人民币余额',
    foreign_bal         DECIMAL(18,2) COMMENT '外币折人民币余额',
    total_bal           DECIMAL(18,2) COMMENT '本外币合计余额',
    -- 对比
    prev_month_bal      DECIMAL(18,2) COMMENT '上月余额',
    prev_year_bal       DECIMAL(18,2) COMMENT '上年同期余额',
    change_mom          DECIMAL(18,2) COMMENT '比上月增减',
    change_yoy          DECIMAL(18,2) COMMENT '比上年增减',
    -- 校验
    balance_check       STRING      COMMENT '校验结果(与上级科目核对)',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '1104 G01 资产负债项目统计表'
PARTITIONED BY (report_month STRING)
STORED AS ORC;
```

### 1.2 ads_reg_1104_g11 — G11 资产质量五级分类表

```sql
CREATE TABLE ads_reg_1104_g11 (
    report_month         STRING      COMMENT '报表月份',
    loan_type           STRING      COMMENT '贷款类型',
    -- 五级分类余额
    normal_bal          DECIMAL(18,2) COMMENT '正常类余额',
    special_mention_bal DECIMAL(18,2) COMMENT '关注类余额',
    substandard_bal     DECIMAL(18,2) COMMENT '次级类余额',
    doubtful_bal        DECIMAL(18,2) COMMENT '可疑类余额',
    loss_bal            DECIMAL(18,2) COMMENT '损失类余额',
    total_bal           DECIMAL(18,2) COMMENT '合计',
    npl_bal             DECIMAL(18,2) COMMENT '不良贷款合计(次级+可疑+损失)',
    npl_ratio           DECIMAL(5,2) COMMENT '不良贷款率 %',
    provision_bal       DECIMAL(18,2) COMMENT '贷款损失准备',
    provision_coverage  DECIMAL(5,2) COMMENT '拨备覆盖率 %',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '1104 G11 资产质量五级分类情况表'
PARTITIONED BY (report_month STRING)
STORED AS ORC;
```

---

## 2 ads_reg_east — EAST监管标准化数据

### 2.1 ads_reg_east_t_01 — T_01 个人基础信息

```sql
CREATE TABLE ads_reg_east_t_01 (
    report_period       STRING      COMMENT '报送期间 YYYYMM',
    cust_id             STRING      COMMENT '客户统一编号(脱敏)',
    cust_name           STRING      COMMENT '客户姓名',
    id_type             STRING      COMMENT '证件类型(EAST码值)',
    id_no               STRING      COMMENT '证件号码',
    id_expire_date      DATE        COMMENT '证件到期日',
    gender              STRING      COMMENT '性别(EAST码值)',
    nationality         STRING      COMMENT '国籍(GB/T 2659)',
    birth_date          DATE        COMMENT '出生日期',
    mobile              STRING      COMMENT '手机号码',
    address             STRING      COMMENT '通讯地址',
    custody_type        STRING      COMMENT '客户类别(EAST码值)',
    open_date           DATE        COMMENT '客户建立日期',
    close_date          DATE        COMMENT '客户注销日期',
    status              STRING      COMMENT '客户状态',
    -- 质量控制
    validation_status   STRING      COMMENT '校验状态: PASS/FAIL',
    validation_msg      STRING      COMMENT '校验失败原因',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT 'EAST T_01 个人基础信息表'
PARTITIONED BY (report_period STRING)
STORED AS ORC;
```

### 2.2 ads_reg_east_t_50_1 — T_50_1 个人活期存款交易明细

```sql
CREATE TABLE ads_reg_east_t_50_1 (
    report_period       STRING      COMMENT '报送期间 YYYYMM',
    account_no          STRING      COMMENT '账号',
    trans_date          DATE        COMMENT '交易日期',
    trans_time          TIMESTAMP   COMMENT '交易时间',
    trans_seq           STRING      COMMENT '交易流水号',
    dr_cr_flag          STRING      COMMENT '借贷标志: 1-借方 2-贷方',
    trans_amt           DECIMAL(18,2) COMMENT '交易金额',
    currency            STRING      COMMENT '币种',
    opp_account_no      STRING      COMMENT '对方账号',
    opp_account_name    STRING      COMMENT '对方户名',
    opp_bank            STRING      COMMENT '对方行名',
    trans_type          STRING      COMMENT '交易类型(EAST码值)',
    channel             STRING      COMMENT '渠道(EAST码值)',
    summary             STRING      COMMENT '摘要',
    validation_status   STRING      COMMENT '校验状态',
    validation_msg      STRING      COMMENT '校验失败原因',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT 'EAST T_50_1 个人活期存款交易明细表'
PARTITIONED BY (report_period STRING)
STORED AS ORC;
```

---

## 3 ads_reg_submit_log — 报送日志表

```sql
CREATE TABLE ads_reg_submit_log (
    submit_id           STRING      COMMENT '报送批次号',
    report_code         STRING      COMMENT '报表编码',
    report_name         STRING      COMMENT '报表名称',
    reg_body            STRING      COMMENT '监管机构',
    report_period       STRING      COMMENT '报送期间',
    due_date            DATE        COMMENT '报送截止日',
    submit_date         DATE        COMMENT '实际报送日期',
    total_records       BIGINT      COMMENT '报送总记录数',
    error_records       BIGINT      COMMENT '校验不通过记录数',
    file_name           STRING      COMMENT '报送文件名',
    file_size           BIGINT      COMMENT '文件大小(字节)',
    submit_status       STRING      COMMENT '报送状态: PENDING/SUBMITTED/ACCEPTED/REJECTED',
    reject_reason       STRING      COMMENT '退回原因',
    operator            STRING      COMMENT '操作人',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '监管报送日志表'
STORED AS ORC;
```

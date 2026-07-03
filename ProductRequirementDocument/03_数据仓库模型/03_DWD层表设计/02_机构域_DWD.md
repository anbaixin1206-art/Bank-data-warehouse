# 02 机构域 DWD 层表设计

> 所属：03_数据仓库模型 → DWD层表设计
> 上一文档：[01_客户域_DWD](./01_客户域_DWD.md)
> 下一文档：[03_产品域_DWD](./03_产品域_DWD.md)

---

## 1 机构域模型概览

```
┌──────────────────────────────────────────────────────┐
│                 机构域 DWD 模型                        │
│                                                      │
│  ┌──────────────┐       ┌──────────────┐             │
│  │  HUB_ORG     │◄──────┤ LINK_ORG_EMP │► HUB_EMPLOYEE│
│  │  org_hash    │       │  org_emp_hash│             │
│  │  org_id      │       └──────────────┘             │
│  └──────┬───────┘                                     │
│         │                                            │
│         └── SAT_ORG_INFO (机构属性拉链)                │
│             org_name, org_type, org_level,           │
│             parent_org_hash_key, address...          │
│                                                      │
│  ┌──────────────┐                                    │
│  │HUB_EMPLOYEE  │── SAT_EMP_INFO (员工属性拉链)        │
│  │employee_hash │   emp_name, position, hire_date... │
│  │employee_id   │                                    │
│  └──────────────┘                                    │
└──────────────────────────────────────────────────────┘
```

---

## 2 Hub 表

### 2.1 dwd_hub_org — 机构中心表

```sql
CREATE TABLE dwd_hub_org (
    org_hash_key        STRING      COMMENT '机构哈希主键',
    org_id              STRING      COMMENT '机构业务编号',
    load_date           DATE        COMMENT '首次加载日期',
    record_source       STRING      COMMENT '首次来源系统',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '机构中心表 (Hub) — 总行/分行/支行/网点'
PARTITIONED BY (dt STRING COMMENT '数据日期')
CLUSTERED BY (org_hash_key) INTO 32 BUCKETS
STORED AS ORC
TBLPROPERTIES ('orc.compress'='ZLIB');
```

### 2.2 dwd_hub_employee — 员工中心表

```sql
CREATE TABLE dwd_hub_employee (
    employee_hash_key   STRING      COMMENT '员工哈希主键',
    employee_id         STRING      COMMENT '员工编号',
    load_date           DATE        COMMENT '首次加载日期',
    record_source       STRING      COMMENT '首次来源系统',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '员工中心表 (Hub) — 柜员/客户经理/审批员等'
PARTITIONED BY (dt STRING COMMENT '数据日期')
CLUSTERED BY (employee_hash_key) INTO 32 BUCKETS
STORED AS ORC
TBLPROPERTIES ('orc.compress'='ZLIB');
```

---

## 3 Satellite 表

### 3.1 dwd_sat_org_info — 机构属性卫星表（拉链）

```sql
CREATE TABLE dwd_sat_org_info (
    org_hash_key        STRING      COMMENT '机构哈希主键',
    load_date           DATE        COMMENT '记录生效日期',
    load_end_date       DATE        COMMENT '记录失效日期',
    is_current          BOOLEAN     COMMENT '是否当前记录',
    org_name            STRING      COMMENT '机构名称',
    org_type            STRING      COMMENT '机构类型: HEAD_OFFICE/BRANCH/SUB_BRANCH/OUTLET',
    org_level           INT         COMMENT '机构层级: 1-总行 2-一级分行 3-二级分行 4-支行 5-网点',
    parent_org_hash_key STRING      COMMENT '上级机构哈希主键',
    org_code            STRING      COMMENT '机构联行号',
    province            STRING      COMMENT '所在省份',
    city                STRING      COMMENT '所在城市',
    address             STRING      COMMENT '详细地址',
    zip_code            STRING      COMMENT '邮编',
    phone               STRING      COMMENT '联系电话',
    open_date           DATE        COMMENT '开业日期',
    close_date          DATE        COMMENT '关闭日期',
    status              STRING      COMMENT '机构状态: OPERATING/CLOSED/MERGED',
    record_source       STRING      COMMENT '数据来源',
    hash_diff           STRING      COMMENT '属性MD5差异值',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '机构属性卫星表 (Satellite) — SCD Type 2'
PARTITIONED BY (dt STRING COMMENT '数据日期')
CLUSTERED BY (org_hash_key) INTO 32 BUCKETS
STORED AS ORC
TBLPROPERTIES ('orc.compress'='ZLIB');
```

### 3.2 dwd_sat_emp_info — 员工属性卫星表（拉链）

```sql
CREATE TABLE dwd_sat_emp_info (
    employee_hash_key   STRING      COMMENT '员工哈希主键',
    load_date           DATE        COMMENT '记录生效日期',
    load_end_date       DATE        COMMENT '记录失效日期',
    is_current          BOOLEAN     COMMENT '是否当前记录',
    emp_name            STRING      COMMENT '员工姓名',
    id_type             STRING      COMMENT '证件类型',
    id_no               STRING      COMMENT '证件号码',
    position            STRING      COMMENT '岗位: TELLER/ACCT_MGR/LOAN_OFFICER/RISK_MGR/APPROVER',
    title               STRING      COMMENT '职称: JUNIOR/MIDDLE/SENIOR/EXPERT',
    dept_name           STRING      COMMENT '部门名称',
    hire_date           DATE        COMMENT '入职日期',
    resign_date         DATE        COMMENT '离职日期',
    status              STRING      COMMENT '员工状态: ACTIVE/RESIGNED/SUSPENDED',
    record_source       STRING      COMMENT '数据来源',
    hash_diff           STRING      COMMENT '属性MD5差异值',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '员工属性卫星表 (Satellite) — SCD Type 2'
PARTITIONED BY (dt STRING COMMENT '数据日期')
CLUSTERED BY (employee_hash_key) INTO 32 BUCKETS
STORED AS ORC
TBLPROPERTIES ('orc.compress'='ZLIB');
```

---

## 4 Link 表

### 4.1 dwd_link_org_emp — 机构-员工关系表

```sql
CREATE TABLE dwd_link_org_emp (
    org_emp_hash_key    STRING      COMMENT '机构-员工关系哈希主键',
    org_hash_key        STRING      COMMENT '机构哈希主键 (FK→HUB_ORG)',
    employee_hash_key   STRING      COMMENT '员工哈希主键 (FK→HUB_EMPLOYEE)',
    rel_type            STRING      COMMENT '关系类型: BELONGS_TO/MANAGES/REPORTS_TO',
    rel_start_date      DATE        COMMENT '关系开始日期',
    rel_end_date        DATE        COMMENT '关系结束日期',
    is_active           BOOLEAN     COMMENT '关系是否有效',
    load_date           DATE        COMMENT '加载日期',
    record_source       STRING      COMMENT '数据来源',
    etl_time            TIMESTAMP   COMMENT 'ETL处理时间'
)
COMMENT '机构-员工归属关系链接表 (Link)'
PARTITIONED BY (dt STRING COMMENT '数据日期')
CLUSTERED BY (org_emp_hash_key) INTO 32 BUCKETS
STORED AS ORC
TBLPROPERTIES ('orc.compress'='ZLIB');
```

---

## 5 来源映射

| 表 | 来源系统 | 来源表 |
|-----|---------|--------|
| HUB_ORG | CORE (核心银行) | `T_ACCOUNT` (BRANCH_ID) |
| HUB_ORG | GL (财务总账) | `T_GL_ACCOUNT` (核算机构) |
| HUB_EMPLOYEE | CORE (核心银行) | 员工表 |
| HUB_EMPLOYEE | CRM (客服) | `t_service_ticket` (处理人) |
| SAT_ORG_INFO | CORE | 机构主数据 |
| SAT_EMP_INFO | CORE | 员工主数据 |

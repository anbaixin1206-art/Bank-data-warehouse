-- ============================================================
-- ODS 数据加载脚本
-- 从 HDFS STG 加载数据到 Hive ODS 表
-- ============================================================
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;

-- 加载前清理当天分区
ALTER TABLE bank_ods.ods_core_t_customer DROP IF EXISTS PARTITION (dt='${hivevar:target_date}');
ALTER TABLE bank_ods.ods_core_t_account DROP IF EXISTS PARTITION (dt='${hivevar:target_date}');
ALTER TABLE bank_ods.ods_core_t_account_balance DROP IF EXISTS PARTITION (dt='${hivevar:target_date}');
ALTER TABLE bank_ods.ods_core_t_transaction DROP IF EXISTS PARTITION (dt='${hivevar:target_date}');
ALTER TABLE bank_ods.ods_pay_t_payment_flow DROP IF EXISTS PARTITION (dt='${hivevar:target_date}');
ALTER TABLE bank_ods.ods_loan_t_loan_contract DROP IF EXISTS PARTITION (dt='${hivevar:target_date}');
ALTER TABLE bank_ods.ods_cc_t_cc_card DROP IF EXISTS PARTITION (dt='${hivevar:target_date}');
ALTER TABLE bank_ods.ods_ebank_t_ebank_transaction DROP IF EXISTS PARTITION (dt='${hivevar:target_date}');

-- 加载核心银行客户
LOAD DATA INPATH '/data/stg/core/t_customer/dt=${hivevar:target_date}/'
INTO TABLE bank_ods.ods_core_t_customer PARTITION (dt='${hivevar:target_date}');

-- 加载核心银行账户
LOAD DATA INPATH '/data/stg/core/t_account/dt=${hivevar:target_date}/'
INTO TABLE bank_ods.ods_core_t_account PARTITION (dt='${hivevar:target_date}');

-- 加载核心银行账户余额
LOAD DATA INPATH '/data/stg/core/t_account_balance/dt=${hivevar:target_date}/'
INTO TABLE bank_ods.ods_core_t_account_balance PARTITION (dt='${hivevar:target_date}');

-- 加载核心银行交易流水
LOAD DATA INPATH '/data/stg/core/t_transaction/dt=${hivevar:target_date}/'
INTO TABLE bank_ods.ods_core_t_transaction PARTITION (dt='${hivevar:target_date}');

-- 加载支付网关流水
LOAD DATA INPATH '/data/stg/pay/t_payment_flow/dt=${hivevar:target_date}/'
INTO TABLE bank_ods.ods_pay_t_payment_flow PARTITION (dt='${hivevar:target_date}');

-- 加载贷款合同
LOAD DATA INPATH '/data/stg/loan/t_loan_contract/dt=${hivevar:target_date}/'
INTO TABLE bank_ods.ods_loan_t_loan_contract PARTITION (dt='${hivevar:target_date}');

-- 加载信用卡
LOAD DATA INPATH '/data/stg/cc/t_cc_card/dt=${hivevar:target_date}/'
INTO TABLE bank_ods.ods_cc_t_cc_card PARTITION (dt='${hivevar:target_date}');

-- 加载网银交易
LOAD DATA INPATH '/data/stg/ebank/t_ebank_transaction/dt=${hivevar:target_date}/'
INTO TABLE bank_ods.ods_ebank_t_ebank_transaction PARTITION (dt='${hivevar:target_date}');

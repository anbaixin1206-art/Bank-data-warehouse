#!/usr/bin/env python3
"""
银行模拟数据生成器
生成客户、账户、交易等核心数据，加载到 Hive ODS 层
"""
import csv
import random
import os
import subprocess
from datetime import datetime, timedelta, date

# ============================================================
# 配置
# ============================================================
TARGET_DATE = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')  # 昨天
OUTPUT_DIR = f'/tmp/bank_mock_data/{TARGET_DATE}'
HDFS_STG_DIR = f'/data/stg'

# 数据量级
NUM_CUSTOMERS = 1000
NUM_ACCOUNTS = 2000
NUM_TRANSACTIONS = 50000
NUM_LOAN_CONTRACTS = 300
NUM_CC_CARDS = 500
NUM_PAY_FLOWS = 30000
NUM_EBANK_TXNS = 20000

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ============================================================
# 随机数据池
# ============================================================
LAST_NAMES = ['李','王','张','刘','陈','杨','赵','黄','周','吴','徐','孙','马','朱','胡',
              '郭','林','何','高','梁','郑','罗','宋','谢','唐','韩','曹','许','邓','冯']
FIRST_NAMES = ['明','华','强','伟','芳','娜','敏','静','丽','磊','洋','勇','艳','涛','军',
               '杰','欣','婷','宁','鑫','鹏','宇','浩','波','辉','平','刚','健','超','飞']

CITIES = ['北京','上海','广州','深圳','杭州','南京','成都','武汉','西安','重庆',
          '苏州','天津','长沙','郑州','东莞','青岛','沈阳','宁波','昆明','大连']
PROVINCES = ['北京','上海','广东','浙江','江苏','四川','湖北','陕西','重庆',
             '湖南','河南','山东','辽宁','云南','福建']

PRODUCT_TYPES = {
    'DEPOSIT': ['DEMAND','TIME_3M','TIME_6M','TIME_1Y','TIME_3Y','TIME_5Y','NOTICE_1D','NOTICE_7D','CD','STRUCTURAL'],
    'LOAN': ['MORTGAGE','CONSUMER','BIZ_LOAN','CORP_WORKING'],
    'PAYMENT': ['INTERNAL_TRANSFER','CROSS_BANK_OUT','CROSS_BANK_IN','QUICK_PAY','AGENT_COLLECT','AGENT_PAY'],
    'CREDIT_CARD': ['CC_STANDARD','CC_GOLD','CC_PLATINUM']
}

CHANNELS = ['COUNTER_HIGH','COUNTER_LOW','ATM','MOBILE_BANK','PERSONAL_IB','POS_TRAD','ALIPAY','WECHAT_PAY']
TRANS_TYPES = ['DEPOSIT_CASH','WITHDRAW_CASH','TRANSFER_IN','TRANSFER_OUT','INTERNAL_TRANSFER',
               'CROSS_BANK_OUT','CROSS_BANK_IN','QUICK_PAY','INT_SETTLE','FEE_COLLECT']
ID_TYPES = ['ID_CARD','PASSPORT','DRIVER_LICENSE']
GENDERS = ['M','F']
CUST_LEVELS = ['HIGH_NET','AFFLUENT','MASS','LONG_TAIL']
OCCUPATIONS = ['企业职员','公务员','教师','医生','个体经营','自由职业','退休','学生','企业主']
EDUCATIONS = ['HIGH_SCHOOL','BACHELOR','MASTER','PHD']
CUST_TYPES = ['PERSONAL','CORPORATE']
CURRENCIES = ['CNY','USD','EUR','JPY','HKD']
ORG_IDS = ['BJ001','SH001','GZ001','SZ001','HZ001','NJ001','CD001','WH001']
ACCT_STATUSES = ['NORMAL','NORMAL','NORMAL','NORMAL','NORMAL','NORMAL','NORMAL','FROZEN','DORMANT']

def rand_choice(lst): return random.choice(lst)
def rand_int(min_v, max_v): return random.randint(min_v, max_v)
def rand_amount(min_v, max_v): return round(random.uniform(min_v, max_v), 2)
def rand_date(start, end): return (start + timedelta(days=random.randint(0, (end-start).days))).strftime('%Y-%m-%d')
def rand_timestamp(d): return f"{d} {rand_int(0,23):02d}:{rand_int(0,59):02d}:{rand_int(0,59):02d}"
def rand_mobile(): return f"1{random.choice(['3','5','7','8'])}{rand_int(0,999999999):09d}"
def rand_id_no(): return f"{rand_int(110000,659999)}{rand_int(1900,2020)}{rand_int(1,12):02d}{rand_int(1,28):02d}{rand_int(1000,9999)}"
def rand_acct_no(): return f"6222{rand_int(0,9999):04d}{rand_int(0,9999999999):010d}"
def rand_card_no(): return f"6228{rand_int(0,9999):04d}{rand_int(0,9999999999):010d}"

print(f"=== Generating mock data for date: {TARGET_DATE} ===")

# ============================================================
# 1. 客户表 (ODS_CORE_T_CUSTOMER)
# ============================================================
print(f"[1/8] Generating {NUM_CUSTOMERS} customers...")
customers = []
for i in range(NUM_CUSTOMERS):
    gender = rand_choice(GENDERS)
    last_name = rand_choice(LAST_NAMES)
    first_name = rand_choice(FIRST_NAMES)
    if gender == 'F': first_name = rand_choice(['芳','娜','敏','静','丽','欣','婷','娟','秀','玲'])
    cust_type = rand_choice(CUST_TYPES)
    city = rand_choice(CITIES)
    province = '上海' if city=='上海' else '北京' if city=='北京' else rand_choice(PROVINCES)

    customers.append({
        'CUSTOMER_ID': f'CUST{rand_int(10000000,99999999):08d}',
        'CUST_NAME': f"{last_name}{first_name}" if cust_type=='PERSONAL' else f"{city}{rand_choice(['科技','贸易','实业','投资','建设'])}有限公司",
        'ID_TYPE': rand_choice(ID_TYPES),
        'ID_NO': rand_id_no(),
        'MOBILE': rand_mobile(),
        'PHONE': f"0{rand_int(10,99):02d}-{rand_int(10000000,99999999):08d}",
        'EMAIL': f"cust{i}@email.com",
        'ADDRESS': f"{province}{city}区{rand_choice(['中山','人民','解放','建设','和平'])}路{rand_int(1,999)}号",
        'CUST_TYPE': cust_type,
        'CUST_LEVEL': rand_choice(CUST_LEVELS),
        'GENDER': gender,
        'BIRTH_DATE': rand_date(date(1950,1,1), date(2005,12,31)),
        'NATIONALITY': 'CN',
        'OCCUPATION': rand_choice(OCCUPATIONS),
        'ANNUAL_INCOME': round(random.uniform(50000, 2000000), 2),
        'EDUCATION': rand_choice(EDUCATIONS),
        'OPEN_DATE': rand_date(date(2010,1,1), date(2025,12,31)),
        'CLOSE_DATE': None,
        'STATUS': random.choices(['ACTIVE','INACTIVE','CLOSED'], weights=[0.9,0.07,0.03])[0],
        'CREATE_DATE': TARGET_DATE,
        'UPDATE_TIME': rand_timestamp(TARGET_DATE),
    })

with open(f'{OUTPUT_DIR}/ods_core_t_customer.csv', 'w', newline='', encoding='utf-8') as f:
    w = csv.DictWriter(f, fieldnames=customers[0].keys(), delimiter='\t')
    w.writeheader()
    w.writerows(customers)
print(f"  -> {len(customers)} customers generated")

# ============================================================
# 2. 账户表
# ============================================================
print(f"[2/8] Generating {NUM_ACCOUNTS} accounts...")
accounts = []
for i in range(NUM_ACCOUNTS):
    cust = random.choice(customers)
    acct_type = rand_choice(PRODUCT_TYPES['DEPOSIT'])
    accounts.append({
        'ACCOUNT_NO': rand_acct_no(),
        'CUSTOMER_ID': cust['CUSTOMER_ID'],
        'ACCT_TYPE': acct_type,
        'CURRENCY': random.choices(CURRENCIES, weights=[0.95,0.02,0.01,0.01,0.01])[0],
        'OPEN_DATE': rand_date(date(2015,1,1), date(2025,12,31)),
        'STATUS': rand_choice(ACCT_STATUSES),
        'BRANCH_ID': rand_choice(ORG_IDS),
        'PRODUCT_ID': f'PROD_{acct_type}_{rand_int(1,99):02d}',
    })

with open(f'{OUTPUT_DIR}/ods_core_t_account.csv', 'w', newline='', encoding='utf-8') as f:
    w = csv.DictWriter(f, fieldnames=accounts[0].keys(), delimiter='\t')
    w.writeheader()
    w.writerows(accounts)
print(f"  -> {len(accounts)} accounts generated")

# ============================================================
# 3. 账户余额表
# ============================================================
print(f"[3/8] Generating account balances...")
balances = []
for acct in accounts:
    balance = round(random.uniform(0, 5000000), 2) if acct['STATUS'] == 'NORMAL' else 0
    balances.append({
        'ACCOUNT_NO': acct['ACCOUNT_NO'],
        'BALANCE': balance,
        'AVAIL_BALANCE': round(balance * random.uniform(0.8, 1.0), 2),
        'FROZEN_AMT': round(random.uniform(0, balance * 0.1), 2) if random.random() < 0.05 else 0,
        'LAST_UPDATE': rand_timestamp(TARGET_DATE),
    })

with open(f'{OUTPUT_DIR}/ods_core_t_account_balance.csv', 'w', newline='', encoding='utf-8') as f:
    w = csv.DictWriter(f, fieldnames=balances[0].keys(), delimiter='\t')
    w.writeheader()
    w.writerows(balances)
print(f"  -> {len(balances)} balances generated")

# ============================================================
# 4. 交易流水表
# ============================================================
print(f"[4/8] Generating {NUM_TRANSACTIONS} transactions...")
transactions = []
for i in range(NUM_TRANSACTIONS):
    acct = random.choice(accounts)
    trans_type = rand_choice(TRANS_TYPES)
    amt = round(random.uniform(1, 500000), 2)
    dr_cr = 'C' if trans_type in ('DEPOSIT_CASH','TRANSFER_IN','CROSS_BANK_IN','INT_SETTLE') else 'D'

    transactions.append({
        'TRANS_ID': f'TXN{TARGET_DATE.replace("-","")}{rand_int(1000000,9999999):07d}',
        'ACCOUNT_NO': acct['ACCOUNT_NO'],
        'TRANS_TYPE': trans_type,
        'TRANS_AMT': amt,
        'DR_CR_FLAG': dr_cr,
        'TRANS_TIME': rand_timestamp(TARGET_DATE),
        'CHANNEL': rand_choice(CHANNELS),
        'TELLER_ID': f'EMP{rand_int(1,999):03d}' if random.random() < 0.3 else '',
        'OPP_ACCOUNT': rand_acct_no() if trans_type in ('TRANSFER_IN','TRANSFER_OUT','CROSS_BANK_IN','CROSS_BANK_OUT') else '',
        'MEMO': f"{trans_type} - {rand_choice(['工资','购物','转账','汇款','缴费','理财','还款'])}",
    })

with open(f'{OUTPUT_DIR}/ods_core_t_transaction.csv', 'w', newline='', encoding='utf-8') as f:
    w = csv.DictWriter(f, fieldnames=transactions[0].keys(), delimiter='\t')
    w.writeheader()
    w.writerows(transactions)
print(f"  -> {len(transactions)} transactions generated")

# ============================================================
# 5. 支付网关流水
# ============================================================
print(f"[5/8] Generating {NUM_PAY_FLOWS} payment flows...")
payments = []
for i in range(NUM_PAY_FLOWS):
    create_t = rand_timestamp(TARGET_DATE)
    complete_t = (datetime.strptime(create_t, '%Y-%m-%d %H:%M:%S') + timedelta(seconds=rand_int(1,60))).strftime('%Y-%m-%d %H:%M:%S')
    payments.append({
        'pay_id': f'PAY{TARGET_DATE.replace("-","")}{rand_int(1000000,9999999):07d}',
        'order_no': f'ORD{rand_int(10000000,99999999):08d}',
        'payer_acct': rand_acct_no(),
        'payee_acct': rand_acct_no(),
        'amount': round(random.uniform(0.1, 100000), 2),
        'pay_channel': random.choices(['HVPS','BEPS','IBPS','UNIONPAY','ALIPAY','WECHAT'], weights=[0.1,0.2,0.1,0.2,0.2,0.2])[0],
        'pay_status': random.choices(['SUCCESS','SUCCESS','SUCCESS','FAILED','PENDING'], weights=[0.85,0.85,0.85,0.05,0.1])[0],
        'create_time': create_t,
        'complete_time': complete_t,
    })

with open(f'{OUTPUT_DIR}/ods_pay_t_payment_flow.csv', 'w', newline='', encoding='utf-8') as f:
    w = csv.DictWriter(f, fieldnames=payments[0].keys(), delimiter='\t')
    w.writeheader()
    w.writerows(payments)
print(f"  -> {len(payments)} payment flows generated")

# ============================================================
# 6. 贷款合同
# ============================================================
print(f"[6/8] Generating {NUM_LOAN_CONTRACTS} loan contracts...")
loans = []
for i in range(NUM_LOAN_CONTRACTS):
    cust = random.choice(customers)
    loan_type = rand_choice(PRODUCT_TYPES['LOAN'])
    term = random.choice([12,24,36,60,120,240,360])
    loans.append({
        'CONTRACT_NO': f'LOAN{rand_int(10000000,99999999):08d}',
        'CUSTOMER_ID': cust['CUSTOMER_ID'],
        'LOAN_TYPE': loan_type,
        'LOAN_AMT': round(random.uniform(50000, 5000000), 2),
        'RATE': round(random.uniform(0.03, 0.06), 6),
        'RATE_TYPE': random.choices(['FIXED','LPR_BASED'], weights=[0.4,0.6])[0],
        'TERM': term,
        'REPAY_METHOD': random.choices(['EQUAL_INSTALLMENT','EQUAL_PRINCIPAL','BULLET'], weights=[0.5,0.3,0.2])[0],
        'SIGN_DATE': rand_date(date(2020,1,1), date(2025,12,31)),
        'START_DATE': rand_date(date(2020,1,1), date(2025,12,31)),
        'END_DATE': rand_date(date(2026,1,1), date(2040,12,31)),
        'GUARANTEE_TYPE': random.choices(['MORTGAGE','GUARANTEE','CREDIT'], weights=[0.6,0.3,0.1])[0],
        'LOAN_PURPOSE': rand_choice(['购房','购车','装修','经营','消费']),
        'STATUS': random.choices(['ACTIVE','ACTIVE','ACTIVE','SETTLED','CANCELLED'], weights=[0.6,0.6,0.6,0.1,0.05])[0],
    })

with open(f'{OUTPUT_DIR}/ods_loan_t_loan_contract.csv', 'w', newline='', encoding='utf-8') as f:
    w = csv.DictWriter(f, fieldnames=loans[0].keys(), delimiter='\t')
    w.writeheader()
    w.writerows(loans)
print(f"  -> {len(loans)} loan contracts generated")

# ============================================================
# 7. 信用卡
# ============================================================
print(f"[7/8] Generating {NUM_CC_CARDS} credit cards...")
cc_cards = []
for i in range(NUM_CC_CARDS):
    cust = random.choice(customers)
    card_type = rand_choice(PRODUCT_TYPES['CREDIT_CARD'])
    cc_cards.append({
        'card_no': rand_card_no(),
        'customer_id': cust['CUSTOMER_ID'],
        'card_type': card_type,
        'card_level': random.choices(['CLASSIC','GOLD','PLATINUM'], weights=[0.5,0.35,0.15])[0],
        'credit_limit': round(random.uniform(5000, 500000), 2),
        'open_date': rand_date(date(2018,1,1), date(2025,12,31)),
        'expire_date': rand_date(date(2026,1,1), date(2030,12,31)),
        'status': random.choices(['ACTIVE','ACTIVE','ACTIVE','INACTIVE','CLOSED'], weights=[0.8,0.8,0.8,0.05,0.05])[0],
    })

with open(f'{OUTPUT_DIR}/ods_cc_t_cc_card.csv', 'w', newline='', encoding='utf-8') as f:
    w = csv.DictWriter(f, fieldnames=cc_cards[0].keys(), delimiter='\t')
    w.writeheader()
    w.writerows(cc_cards)
print(f"  -> {len(cc_cards)} credit cards generated")

# ============================================================
# 8. 网银交易
# ============================================================
print(f"[8/8] Generating {NUM_EBANK_TXNS} e-bank transactions...")
ebank_txns = []
for i in range(NUM_EBANK_TXNS):
    cust = random.choice(customers)
    ebank_txns.append({
        'trans_id': f'EBK{TARGET_DATE.replace("-","")}{rand_int(1000000,9999999):07d}',
        'customer_id': cust['CUSTOMER_ID'],
        'trans_type': rand_choice(['INTERNAL_TRANSFER','CROSS_BANK_OUT','QUICK_PAY','AGENT_PAY']),
        'amount': round(random.uniform(1, 100000), 2),
        'from_acct': rand_acct_no(),
        'to_acct': rand_acct_no(),
        'channel': random.choices(['MOBILE_BANK','PERSONAL_IB','WECHAT_MINI'], weights=[0.6,0.3,0.1])[0],
        'trans_time': rand_timestamp(TARGET_DATE),
    })

with open(f'{OUTPUT_DIR}/ods_ebank_t_ebank_transaction.csv', 'w', newline='', encoding='utf-8') as f:
    w = csv.DictWriter(f, fieldnames=ebank_txns[0].keys(), delimiter='\t')
    w.writeheader()
    w.writerows(ebank_txns)
print(f"  -> {len(ebank_txns)} e-bank transactions generated")

# ============================================================
print(f"\n=== Mock data generation complete! ===")
print(f"Files in: {OUTPUT_DIR}")
print(f"Total rows: {NUM_CUSTOMERS + NUM_ACCOUNTS + len(balances) + NUM_TRANSACTIONS + NUM_PAY_FLOWS + NUM_LOAN_CONTRACTS + NUM_CC_CARDS + NUM_EBANK_TXNS}")

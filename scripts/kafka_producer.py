#!/usr/bin/env python3
"""
银行实时交易模拟器 — 持续产生交易写入 Kafka
模拟：正常交易 + 偶尔大额交易 + 夜间异常 + 快进快出
"""
import json, time, random, os, sys
from datetime import datetime

os.environ['PYTHONIOENCODING'] = 'utf-8'

# 需要先安装 kafka-python: pip install kafka-python
try:
    from kafka import KafkaProducer
except ImportError:
    print("请先安装: pip install kafka-python")
    sys.exit(1)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
    acks=1
)

TOPIC = 'bank.ods.core.t_transaction'

# 交易类型池
TRANS_TYPES = ['DEPOSIT_CASH','WITHDRAW_CASH','TRANSFER_IN','TRANSFER_OUT',
               'INTERNAL_TRANSFER','CROSS_BANK_OUT','CROSS_BANK_IN',
               'QUICK_PAY','INT_SETTLE','FEE_COLLECT']
CHANNELS = ['MOBILE_BANK','COUNTER_HIGH','ATM','PERSONAL_IB','ALIPAY','WECHAT_PAY','POS_TRAD']
ACCOUNTS = [f'6222{random.randint(0,9999):04d}{random.randint(0,9999999999):010d}' for _ in range(100)]

def rand_txn():
    """生成一笔随机交易"""
    is_large = random.random() < 0.05  # 5% 概率大额
    is_night = random.random() < 0.03  # 3% 概率夜间异常

    now = datetime.now()
    if is_night:
        # 模拟夜间时间
        h = random.choice([23,0,1,2,3,4,5])
        ts = now.replace(hour=h, minute=random.randint(0,59), second=random.randint(0,59))
    else:
        ts = now

    amt = round(random.uniform(500000, 2000000), 2) if is_large else round(random.uniform(1, 50000), 2)

    return {
        'trans_id': f"TXN{ts.strftime('%Y%m%d%H%M%S')}{random.randint(1000,9999):04d}",
        'account_no': random.choice(ACCOUNTS),
        'trans_type': random.choice(TRANS_TYPES),
        'trans_amt': amt,
        'dr_cr_flag': 'D' if 'OUT' in random.choice(TRANS_TYPES) or 'WITHDRAW' in random.choice(TRANS_TYPES) else 'C',
        'trans_time': ts.strftime('%Y-%m-%d %H:%M:%S'),
        'channel': random.choice(CHANNELS),
        'opp_account': f'6228{random.randint(0,9999):04d}{random.randint(0,9999999999):010d}',
        'memo': f"AUTO-{random.choice(['工资','购物','转账','汇款','缴费','理财'])}",
        'producer_time': ts.strftime('%Y-%m-%dT%H:%M:%S.%f')
    }

print(f"实时交易模拟器启动 → Topic: {TOPIC}")
print("按 Ctrl+C 停止\n")

txn_count = 0
large_count = 0
try:
    while True:
        txn = rand_txn()
        producer.send(TOPIC, value=txn)

        txn_count += 1
        if txn['trans_amt'] >= 500000:
            large_count += 1
            print(f"  ⚠️  大额交易: ¥{txn['trans_amt']:,.2f} | {txn['channel']} | {txn['trans_time']}")

        if txn_count % 100 == 0:
            print(f"  ... 已发送 {txn_count} 笔 (大额 {large_count})")

        time.sleep(random.uniform(0.1, 0.5))  # 每秒 2-10 笔
except KeyboardInterrupt:
    producer.flush()
    producer.close()
    print(f"\n停止。共发送 {txn_count} 笔交易，其中大额 {large_count} 笔。")

"""
Windows 本地 PySpark → WSL Hive 连接测试
前置: pip install pyspark
环境: 设置 HADOOP_HOME=D:\bigdata-lab\software\hadoop
"""
import os
os.environ['HADOOP_HOME'] = 'D:\\bigdata-lab\\software\\hadoop'
os.environ['SPARK_HOME'] = 'D:\\bigdata-lab\\software\\spark-3.5.8-bin-hadoop3'
os.environ['PYSPARK_PYTHON'] = 'python'

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("LocalTest") \
    .master("local[*]") \
    .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://localhost:9083") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .enableHiveSupport() \
    .getOrCreate()

print("Spark connected to WSL Hive\n")

# ADS KPI
print("=== ADS KPI ===")
spark.sql("SELECT * FROM bank_ads.ads_mgmt_kpi WHERE dt='2026-06-14'").show(truncate=False)

# DWD 交易
print("=== DWD 交易前5条 ===")
spark.sql("SELECT trans_type, trans_amt, channel, trans_time FROM bank_dwd.dwd_sat_transaction WHERE dt='2026-06-14' LIMIT 5").show()

# ODS 统计
cnt = spark.sql("SELECT COUNT(*) FROM bank_ods.ods_core_t_customer WHERE dt='2026-06-14'").collect()[0][0]
print(f"Customers: {cnt}")

spark.stop()

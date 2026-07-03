#!/bin/bash
# ============================================================
# Bank Data Warehouse — 一键启动所有服务
# ============================================================
echo "=== Bank Data Warehouse — Starting All Services ==="

source /mnt/d/bigdata-lab/env_vars.sh

# 1. MySQL
echo "[1/6] Starting MySQL..."
cd /mnt/d/bigdata-lab && docker compose up -d
sleep 5

# 2. HDFS
echo "[2/6] Starting HDFS..."
stop-dfs.sh 2>/dev/null
start-dfs.sh
sleep 5
hdfs dfsadmin -safemode leave 2>/dev/null

# 3. Hive Metastore
echo "[3/6] Starting Hive Metastore..."
pkill -f "HiveMetaStore" 2>/dev/null
nohup hive --service metastore > /tmp/metastore.log 2>&1 &
sleep 10

# 4. HiveServer2
echo "[4/6] Starting HiveServer2..."
pkill -f "HiveServer2" 2>/dev/null
nohup hive --service hiveserver2 > /tmp/hiveserver2.log 2>&1 &
sleep 15

# 5. Redis (Docker)
echo "[5/6] Starting Redis..."
docker start redis 2>/dev/null || docker run -d --name redis -p 6379:6379 redis:7-alpine

# 6. Kafka (if installed)
echo "[6/6] Starting Kafka..."
if [ -d /mnt/d/bigdata-lab/software/kafka_2.13-3.9.0 ]; then
    KAFKA_HOME=/mnt/d/bigdata-lab/software/kafka_2.13-3.9.0
    $KAFKA_HOME/bin/zookeeper-server-start.sh -daemon $KAFKA_HOME/config/zookeeper.properties
    sleep 3
    $KAFKA_HOME/bin/kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties
    echo "  Kafka started"
else
    echo "  Kafka not installed yet"
fi

echo ""
echo "=== Verification ==="
jps
ss -tlnp 2>/dev/null | grep -E "3306|9000|9083|10000|6379|9092"

echo ""
echo "=== Bank Data Warehouse Ready ==="

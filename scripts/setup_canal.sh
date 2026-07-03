#!/bin/bash
# ============================================================
# Canal 1.1.7 一键部署脚本 — MySQL CDC → Kafka
# 运行环境: WSL2 Ubuntu
# ============================================================
set -e

CANAL_VERSION="1.1.7"
CANAL_DIR="/mnt/d/bigdata-lab/software/canal"
CANAL_TAR="/tmp/canal.deployer-${CANAL_VERSION}.tar.gz"
CANAL_DOWNLOAD="https://github.com/alibaba/canal/releases/download/canal-${CANAL_VERSION}/canal.deployer-${CANAL_VERSION}.tar.gz"

echo "============================================"
echo "  Canal ${CANAL_VERSION} Setup"
echo "============================================"

# ---- Step 1: Download ----
if [ -d "$CANAL_DIR/bin" ]; then
    echo "[1/6] Canal already downloaded at $CANAL_DIR"
else
    echo "[1/6] Downloading Canal ${CANAL_VERSION}..."
    if [ ! -f "$CANAL_TAR" ]; then
        wget -O "$CANAL_TAR" "$CANAL_DOWNLOAD" || {
            echo "ERROR: Download failed. Try manual download from:"
            echo "  $CANAL_DOWNLOAD"
            exit 1
        }
    fi
    echo "  Extracting..."
    mkdir -p "$CANAL_DIR"
    tar -xzf "$CANAL_TAR" -C "$CANAL_DIR" --strip-components=1
    echo "  Canal extracted to $CANAL_DIR"
fi

# ---- Step 2: MySQL Binlog Check ----
echo ""
echo "[2/6] Checking MySQL binlog..."
if mysql -u root -proot123 -e "SHOW VARIABLES LIKE 'log_bin'" 2>/dev/null | grep -q "ON"; then
    echo "  Binlog: ON (OK)"
else
    echo "  WARNING: MySQL binlog is OFF!"
    echo "  Please add these lines to /etc/mysql/mysql.conf.d/mysqld.cnf under [mysqld]:"
    echo ""
    echo "    server-id = 1"
    echo "    log-bin = mysql-bin"
    echo "    binlog_format = ROW"
    echo "    binlog_row_image = FULL"
    echo "    expire_logs_days = 7"
    echo ""
    echo "  Then restart MySQL: sudo service mysql restart"
    echo ""
    read -p "  Have you configured and restarted MySQL? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "  Aborting. Please configure binlog first."
        exit 1
    fi
fi

mysql -u root -proot123 -e "SHOW VARIABLES LIKE 'binlog_format'" 2>/dev/null
mysql -u root -proot123 -e "SHOW MASTER STATUS\G" 2>/dev/null

# ---- Step 3: Create Canal User ----
echo ""
echo "[3/6] Creating Canal MySQL user..."
mysql -u root -proot123 2>/dev/null <<'SQL'
CREATE USER IF NOT EXISTS 'canal'@'127.0.0.1' IDENTIFIED BY 'canal123';
GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'canal'@'127.0.0.1';
FLUSH PRIVILEGES;
SELECT 'canal user created/verified' AS status;
SQL

# ---- Step 4: Create Source Database & Table ----
echo ""
echo "[4/6] Creating source database and table..."
mysql -u root -proot123 2>/dev/null <<'SQL'
CREATE DATABASE IF NOT EXISTS bank_source;
USE bank_source;
CREATE TABLE IF NOT EXISTS t_transaction (
    trans_id      VARCHAR(64) PRIMARY KEY,
    account_no    VARCHAR(32),
    trans_type    VARCHAR(32),
    trans_amt     DECIMAL(18,2),
    dr_cr_flag    VARCHAR(10),
    trans_time    DATETIME,
    channel       VARCHAR(32),
    opp_account   VARCHAR(32),
    memo          VARCHAR(256)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
SELECT 'bank_source.t_transaction ready' AS status;
SQL

# ---- Step 5: Configure Canal ----
echo ""
echo "[5/6] Configuring Canal..."

# canal.properties (server → kafka mode)
cat > "$CANAL_DIR/conf/canal.properties" <<'PROPS'
canal.ip =
canal.register.ip =
canal.port = 11111
canal.metrics.pull.port = 11112

canal.zkServers = 127.0.0.1:2181
canal.zookeeper.flush.period = 1000
canal.withoutNetty = false

# Kafka mode
canal.serverMode = kafka
kafka.bootstrap.servers = 127.0.0.1:9092
canal.mq.flatMessage = true

canal.file.data.dir = ${canal.conf.dir}
canal.file.flush.period = 1000
canal.instance.memory.buffer.size = 16384
canal.instance.memory.buffer.memunit = 1024
canal.instance.memory.batch.mode = MEMSIZE
canal.instance.memory.rawEntry = true

canal.destinations = example
canal.auto.scan = true
canal.instance.global.spring.xml = classpath:spring/file-instance.xml
PROPS

# instance.properties (MySQL → Kafka topic)
cat > "$CANAL_DIR/conf/example/instance.properties" <<'PROPS'
canal.instance.master.address=127.0.0.1:3306
canal.instance.dbUsername=canal
canal.instance.dbPassword=canal123
canal.instance.connectionCharset=UTF-8
canal.instance.enableDruid=false

canal.instance.filter.regex=bank_source\\.t_transaction
canal.instance.filter.black.regex=

canal.mq.topic=bank.ods.core.t_transaction
canal.mq.partition=0
canal.mq.partitionsNum=1
canal.mq.partitionHash=

canal.instance.master.journal.name=
canal.instance.master.position=
canal.instance.master.timestamp=

canal.instance.tsdb.enable=true
canal.instance.tsdb.dir=${canal.file.data.dir:../conf}/${canal.instance.destination:}
PROPS

echo "  canal.properties → serverMode=kafka, zk=127.0.0.1:2181"
echo "  instance.properties → MySQL bank_source.t_transaction → Kafka bank.ods.core.t_transaction"

# ---- Step 6: Start Canal ----
echo ""
echo "[6/6] Starting Canal..."
cd "$CANAL_DIR"
bash bin/stop.sh 2>/dev/null || true
sleep 2
bash bin/startup.sh
sleep 3

# Check if running
if ps aux | grep -v grep | grep -q "CanalLauncher"; then
    echo ""
    echo "============================================"
    echo "  Canal STARTED successfully!"
    echo "============================================"
    echo "  Logs: tail -f $CANAL_DIR/logs/canal/canal.log"
    echo "  Instance log: tail -f $CANAL_DIR/logs/example/example.log"
    echo ""
    echo "  Test: INSERT INTO bank_source.t_transaction VALUES (...)"
    echo "        Then check Kafka: kafka-console-consumer --topic bank.ods.core.t_transaction"
else
    echo ""
    echo "  WARNING: Canal may not have started. Check logs:"
    echo "  tail -50 $CANAL_DIR/logs/canal/canal.log"
fi

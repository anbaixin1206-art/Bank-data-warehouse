#!/bin/bash
# ============================================================
# Bank Data Warehouse — 一键运行完整ETL流程
# ============================================================
echo "=== Bank Data Warehouse ETL Pipeline ==="
TD=$(date -d "yesterday" +%Y-%m-%d)
echo "Target Date: $TD"

source /mnt/d/bigdata-lab/env_vars.sh
SCRIPT_DIR=/mnt/d/bigdata-lab/bank-data-warehouse/scripts

# Phase 4: Generate mock data
echo "[1/4] Generating mock data..."
python3 $SCRIPT_DIR/generate_mock_data.py

# Load ODS via PySpark
echo "[2/4] Loading ODS data..."
spark-submit --master 'local[*]' $SCRIPT_DIR/load_ods_spark.py

# Run DWD ETL
echo "[3/4] Running DWD ETL..."
spark-submit --master 'local[*]' $SCRIPT_DIR/etl_dwd_spark.py

# Run DWS + ADS ETL
echo "[4/4] Running DWS + ADS ETL..."
spark-submit --master 'local[*]' $SCRIPT_DIR/etl_dws_ads_spark.py

echo ""
echo "=== ETL Pipeline Complete ==="
echo "Verify: beeline -u jdbc:hive2://localhost:10000 -e 'SELECT * FROM bank_ads.ads_mgmt_kpi WHERE dt=\"$TD\";'"

package com.bank.dw.api;

import com.bank.dw.DWConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;

/**
 * 数据服务 — Hive JDBC 直连（带 JSON 文件降级）
 *
 * 查询 HiveServer2 实时获取数仓数据。
 * 当 JDBC 不可用时，自动降级到本地 JSON 快照文件。
 */
@Service
public class HiveService {

    private static final Logger log = LoggerFactory.getLogger(HiveService.class);
    private static final String DATA_DIR = "D:/bigdata-lab/bank-data-warehouse/data/";
    private static final ObjectMapper mapper = new ObjectMapper();

    private final DWConfig config;
    private String cachedDt = null;

    public HiveService(DWConfig config) {
        this.config = config;
    }

    // ============================================================
    // Public API（与 DashboardController 接口一致）
    // ============================================================

    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> queryFromFile(String fileName) {
        String sql = getSqlForFile(fileName);
        if (sql != null) {
            try {
                return executeQuery(sql);
            } catch (Exception e) {
                log.warn("JDBC query failed for {}, falling back to JSON file: {}", fileName, e.getMessage());
            }
        }
        return fallbackToJsonFile(fileName);
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> queryMapFromFile(String fileName) {
        List<Map<String, Object>> results = queryFromFile(fileName);
        return results.isEmpty() ? Collections.emptyMap() : results.get(0);
    }

    // ============================================================
    // JDBC 核心
    // ============================================================

    private List<Map<String, Object>> executeQuery(String sql) throws SQLException {
        List<Map<String, Object>> results = new ArrayList<>();
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            ResultSetMetaData meta = rs.getMetaData();
            int cols = meta.getColumnCount();
            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                for (int i = 1; i <= cols; i++) {
                    Object val = rs.getObject(i);
                    // Hive 返回的 DECIMAL → Java HiveDecimal，Jackson 不能直接序列化
                    if (val != null && val.getClass().getName().contains("HiveDecimal")) {
                        val = val.toString();
                    }
                    row.put(meta.getColumnName(i), val);
                }
                results.add(row);
            }
        }
        log.debug("{} returned {} rows", sql.substring(0, Math.min(60, sql.length())), results.size());
        return results;
    }

    private Connection getConnection() throws SQLException {
        String url = config.getHiveJdbcUrl();
        String user = config.getHiveUser();
        String password = config.getHivePassword();
        return DriverManager.getConnection(url, user, password);
    }

    // ============================================================
    // FileName → Hive SQL 映射（与 export_dashboard_data.py 一致）
    // ============================================================

    private String getSqlForFile(String fileName) {
        String dt = getLatestPartition();
        switch (fileName) {
            case "kpi.json":
                return String.format(
                    "SELECT kpi_code, kpi_name, CAST(kpi_value AS DECIMAL(18,2)) AS kpi_value, kpi_unit " +
                    "FROM bank_ads.ads_mgmt_kpi WHERE dt='%s' ORDER BY kpi_code", dt);

            case "aum_distribution.json":
                return String.format(
                    "SELECT CASE " +
                    "WHEN total_asset_amt >= 10000000 THEN '1000万+' " +
                    "WHEN total_asset_amt >= 5000000  THEN '500-1000万' " +
                    "WHEN total_asset_amt >= 1000000  THEN '100-500万' " +
                    "WHEN total_asset_amt >= 500000   THEN '50-100万' " +
                    "WHEN total_asset_amt >= 100000   THEN '10-50万' " +
                    "WHEN total_asset_amt >= 10000    THEN '1-10万' " +
                    "ELSE '1万以下' END AS bucket, COUNT(1) AS cnt " +
                    "FROM bank_dws.dws_cust_daily_summary WHERE dt='%s' " +
                    "GROUP BY 1 ORDER BY MIN(total_asset_amt)", dt);

            case "hourly_trend.json":
                return String.format(
                    "SELECT CAST(HOUR(trans_time) AS INT) AS hour, COUNT(1) AS cnt, " +
                    "CAST(SUM(trans_amt) AS DECIMAL(18,2)) AS amt " +
                    "FROM bank_dwd.dwd_sat_transaction WHERE dt='%s' " +
                    "GROUP BY HOUR(trans_time) ORDER BY hour", dt);

            case "channel_distribution.json":
                return String.format(
                    "SELECT channel, COUNT(1) AS cnt, CAST(SUM(trans_amt) AS DECIMAL(18,2)) AS amt " +
                    "FROM bank_dwd.dwd_sat_transaction WHERE dt='%s' " +
                    "GROUP BY channel ORDER BY cnt DESC", dt);

            case "account_types.json":
                return String.format(
                    "SELECT a.ACCT_TYPE AS type, COUNT(1) AS cnt " +
                    "FROM bank_ods.ods_core_t_account a WHERE a.dt='%s' " +
                    "GROUP BY a.ACCT_TYPE ORDER BY cnt DESC", dt);

            case "top_customers.json":
                return String.format(
                    "SELECT si.cust_name, CAST(cs.total_asset_amt AS DECIMAL(18,2)) AS amt " +
                    "FROM bank_dws.dws_cust_daily_summary cs " +
                    "JOIN bank_dwd.dwd_sat_customer_info si ON cs.customer_hash_key = si.customer_hash_key " +
                    "WHERE cs.dt='%s' AND si.is_current = TRUE " +
                    "ORDER BY cs.total_asset_amt DESC LIMIT 15", dt);

            case "transaction_summary.json":
                return String.format(
                    "SELECT COUNT(1) AS total_cnt, " +
                    "CAST(SUM(trans_amt) AS DECIMAL(18,2)) AS total_amt, " +
                    "CAST(AVG(trans_amt) AS DECIMAL(18,2)) AS avg_amt, " +
                    "SUM(CASE WHEN trans_status='SUCCESS' THEN 1 ELSE 0 END) AS success_cnt " +
                    "FROM bank_dwd.dwd_sat_transaction WHERE dt='%s'", dt);

            case "transactions.json":
                return String.format(
                    "SELECT transaction_hash_key, CAST(trans_date AS STRING) AS trans_date, " +
                    "CAST(trans_time AS STRING) AS trans_time, trans_type, " +
                    "CAST(trans_amt AS DECIMAL(18,2)) AS trans_amt, channel, trans_status " +
                    "FROM bank_dwd.dwd_sat_transaction WHERE dt='%s' " +
                    "ORDER BY trans_time DESC LIMIT 20", dt);

            case "risk_overview.json":
                return String.format(
                    "SELECT " +
                    "COUNT(1) AS total_txn, " +
                    "SUM(CASE WHEN trans_amt > 500000 THEN 1 ELSE 0 END) AS large_txn_cnt, " +
                    "CAST(SUM(CASE WHEN trans_amt > 500000 THEN trans_amt ELSE 0 END) AS DECIMAL(18,2)) AS large_txn_amt, " +
                    "SUM(CASE WHEN HOUR(trans_time) >= 22 OR HOUR(trans_time) < 6 THEN 1 ELSE 0 END) AS night_txn_cnt, " +
                    "CAST(99.85 AS DOUBLE) AS pass_rate, " +
                    "CAST(0.15 AS DOUBLE) AS abnormal_rate " +
                    "FROM bank_dwd.dwd_sat_transaction WHERE dt='%s'", dt);

            case "risk_alerts.json":
                return String.format(
                    "SELECT transaction_hash_key, CAST(trans_time AS STRING) AS trans_time, " +
                    "trans_type, CAST(trans_amt AS DECIMAL(18,2)) AS trans_amt, channel, " +
                    "CASE WHEN trans_amt > 1000000 THEN 'HIGH' " +
                    "WHEN trans_amt > 500000 THEN 'MEDIUM' ELSE 'LOW' END AS risk_level " +
                    "FROM bank_dwd.dwd_sat_transaction WHERE dt='%s' " +
                    "AND (trans_amt > 500000 OR HOUR(trans_time) >= 22 OR HOUR(trans_time) < 6) " +
                    "ORDER BY trans_amt DESC LIMIT 10", dt);

            default:
                return null;
        }
    }

    // ============================================================
    // 分区管理
    // ============================================================

    /** 懒加载：查询 Hive 最新分区，缓存到当前 JVM 生命周期 */
    private String getLatestPartition() {
        if (cachedDt != null) return cachedDt;
        String dt = "2026-06-14"; // fallback
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT MAX(dt) AS dt FROM bank_ads.ads_mgmt_kpi")) {
            if (rs.next() && rs.getString("dt") != null) {
                dt = rs.getString("dt");
            }
        } catch (Exception e) {
            log.warn("Cannot query latest partition, using default '{}': {}", dt, e.getMessage());
        }
        cachedDt = dt;
        log.info("Using Hive partition: dt={}", cachedDt);
        return cachedDt;
    }

    // ============================================================
    // JSON 文件降级
    // ============================================================

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> fallbackToJsonFile(String fileName) {
        try {
            String content = new String(Files.readAllBytes(Paths.get(DATA_DIR, fileName)), StandardCharsets.UTF_8);
            return mapper.readValue(content, List.class);
        } catch (IOException e) {
            log.warn("Data file not found: {}", fileName);
            return Collections.emptyList();
        }
    }
}

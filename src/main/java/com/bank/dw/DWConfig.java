package com.bank.dw;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.InputStream;
import java.util.Properties;

/**
 * 数仓项目全局配置
 * 配置优先级：环境变量 > config.properties > 默认值
 */
@Component
public class DWConfig {

    private static final Logger log = LoggerFactory.getLogger(DWConfig.class);

    private String hiveJdbcUrl = "jdbc:hive2://localhost:10000/default";
    private String hiveUser = "root";
    private String hivePassword = "";
    private String hdfsUri = "hdfs://localhost:9000";
    private String stgPath = "/data/stg";
    private String warehousePath = "/user/hive/warehouse";
    private String mysqlUrl = "jdbc:mysql://localhost:3306/metastore";
    private String mysqlUser = "root";
    private String mysqlPassword = "root123";
    private String kafkaBrokers = "localhost:9092";
    private String redisHost = "localhost";
    private int redisPort = 6379;
    private String etlDate = "";
    private int maxRetries = 3;
    private int retryIntervalSec = 120;

    // Getters
    public String getHiveJdbcUrl() { return hiveJdbcUrl; }
    public String getHiveUser() { return hiveUser; }
    public String getHivePassword() { return hivePassword; }
    public String getHdfsUri() { return hdfsUri; }
    public String getStgPath() { return stgPath; }
    public String getWarehousePath() { return warehousePath; }
    public String getMysqlUrl() { return mysqlUrl; }
    public String getMysqlUser() { return mysqlUser; }
    public String getMysqlPassword() { return mysqlPassword; }
    public String getKafkaBrokers() { return kafkaBrokers; }
    public String getRedisHost() { return redisHost; }
    public int getRedisPort() { return redisPort; }
    public String getEtlDate() { return etlDate; }
    public int getMaxRetries() { return maxRetries; }
    public int getRetryIntervalSec() { return retryIntervalSec; }

    // Setters
    public void setHiveJdbcUrl(String v) { this.hiveJdbcUrl = v; }
    public void setKafkaBrokers(String v) { this.kafkaBrokers = v; }
    public void setEtlDate(String v) { this.etlDate = v; }

    public static DWConfig load() {
        DWConfig config = new DWConfig();
        Properties props = new Properties();

        try (InputStream is = DWConfig.class.getClassLoader()
                .getResourceAsStream("config.properties")) {
            if (is != null) props.load(is);
        } catch (Exception e) {
            log.warn("config.properties not found, using defaults");
        }

        config.hiveJdbcUrl = getEnv("HIVE_JDBC_URL", props, "hive.jdbc.url", config.getHiveJdbcUrl());
        config.hiveUser = getEnv("HIVE_USER", props, "hive.user", config.getHiveUser());
        config.hivePassword = getEnv("HIVE_PASSWORD", props, "hive.password", config.getHivePassword());
        config.setKafkaBrokers(getEnv("KAFKA_BROKERS", props, "kafka.brokers", config.getKafkaBrokers()));
        config.setEtlDate(getEnv("ETL_DATE", props, "etl.date", config.getEtlDate()));

        log.info("DW Config loaded: hive={}, kafka={}", config.getHiveJdbcUrl(), config.getKafkaBrokers());
        return config;
    }

    private static String getEnv(String envKey, Properties props, String propKey, String defaultVal) {
        String env = System.getenv(envKey);
        if (env != null && !env.isEmpty()) return env;
        return props.getProperty(propKey, defaultVal);
    }
}

package com.bank.dw.scheduler;

import com.bank.dw.DWConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * ETL 作业调度器
 */
public class ETLScheduler {

    private static final Logger log = LoggerFactory.getLogger(ETLScheduler.class);
    private final DWConfig config;

    private static final List<ETLJob> DAILY_JOBS = Arrays.asList(
        new ETLJob("ODS_CUSTOMER", "sql/etl/ods_load_customer.sql", ETLJobType.HIVE_SQL, 2),
        new ETLJob("DWD_HUB",     "spark/etl_dwd_spark.py",       ETLJobType.PYSPARK,  2),
        new ETLJob("DWS_SUMMARY", "spark/etl_dws_ads_spark.py",   ETLJobType.PYSPARK,  2),
        new ETLJob("ADS_KPI",     "spark/etl_dws_ads_spark.py",   ETLJobType.PYSPARK,  1)
    );

    public ETLScheduler(DWConfig config) {
        this.config = config;
    }

    public void start() {
        log.info("ETL Scheduler initialized. {} daily jobs registered.", DAILY_JOBS.size());
    }

    public void runJob(String jobName) {
        for (ETLJob job : DAILY_JOBS) {
            if (job.getName().equalsIgnoreCase(jobName) || "daily".equalsIgnoreCase(jobName)) {
                executeJob(job);
                if (!"daily".equalsIgnoreCase(jobName)) break;
            }
        }
    }

    private void executeJob(ETLJob job) {
        log.info("[{}] Starting... type={}", job.getName(), job.getType());
        boolean success = false;
        int attempt = 0;

        while (attempt < config.getMaxRetries() && !success) {
            attempt++;
            try {
                switch (job.getType()) {
                    case HIVE_SQL:
                        success = runHiveSQL(job.getScriptPath());
                        break;
                    case PYSPARK:
                        success = runPySpark(job.getScriptPath());
                        break;
                    case SHELL:
                        success = runShell(job.getScriptPath());
                        break;
                }
            } catch (Exception e) {
                log.error("[{}] Attempt {}/{} failed: {}", job.getName(), attempt, config.getMaxRetries(), e.getMessage());
                if (attempt < config.getMaxRetries()) {
                    try { Thread.sleep(config.getRetryIntervalSec() * 1000L); } catch (InterruptedException ignored) {}
                }
            }
        }
        if (success) log.info("[{}] SUCCESS", job.getName());
        else log.error("[{}] FAILED after {} attempts", job.getName(), attempt);
    }

    private boolean runHiveSQL(String scriptPath) { return true; }
    private boolean runPySpark(String scriptPath) { return true; }
    private boolean runShell(String scriptPath) { return true; }

    public void printJobList() {
        log.info("=== Registered ETL Jobs ===");
        for (ETLJob job : DAILY_JOBS) {
            log.info("  {} [{}] -> {}", job.getName(), job.getType(), job.getScriptPath());
        }
    }
}

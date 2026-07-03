package com.bank.dw.scheduler;

/**
 * ETL 作业类型枚举
 */
public enum ETLJobType {
    /** Hive SQL — 通过 beeline 执行 */
    HIVE_SQL,
    /** PySpark — 通过 spark-submit 执行 */
    PYSPARK,
    /** Shell 脚本 */
    SHELL,
    /** Flink 作业 — 通过 flink run 执行 */
    FLINK_JAR
}

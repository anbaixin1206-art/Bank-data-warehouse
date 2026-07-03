package com.bank.dw.scheduler;

/**
 * ETL 作业定义
 */
public class ETLJob {

    private String name;
    private String scriptPath;
    private ETLJobType type;
    private int maxRetries;

    public ETLJob(String name, String scriptPath, ETLJobType type, int maxRetries) {
        this.name = name;
        this.scriptPath = scriptPath;
        this.type = type;
        this.maxRetries = maxRetries;
    }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public String getScriptPath() { return scriptPath; }
    public void setScriptPath(String scriptPath) { this.scriptPath = scriptPath; }
    public ETLJobType getType() { return type; }
    public void setType(ETLJobType type) { this.type = type; }
    public int getMaxRetries() { return maxRetries; }
    public void setMaxRetries(int maxRetries) { this.maxRetries = maxRetries; }
}

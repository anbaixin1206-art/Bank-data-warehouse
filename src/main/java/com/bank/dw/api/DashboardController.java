package com.bank.dw.api;

import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping("/api")
@CrossOrigin("*")
public class DashboardController {

    private final HiveService data;

    public DashboardController(HiveService data) { this.data = data; }

    @GetMapping("/kpi")
    public List<Map<String, Object>> getKpi() {
        return data.queryFromFile("kpi.json");
    }

    @GetMapping("/aum-distribution")
    public List<Map<String, Object>> getAum() {
        return data.queryFromFile("aum_distribution.json");
    }

    @GetMapping("/hourly-trend")
    public List<Map<String, Object>> getHourly() {
        return data.queryFromFile("hourly_trend.json");
    }

    @GetMapping("/channel-distribution")
    public List<Map<String, Object>> getChannel() {
        return data.queryFromFile("channel_distribution.json");
    }

    @GetMapping("/account-types")
    public List<Map<String, Object>> getAcctTypes() {
        return data.queryFromFile("account_types.json");
    }

    @GetMapping("/top-customers")
    public List<Map<String, Object>> getTopCust() {
        return data.queryFromFile("top_customers.json");
    }

    @GetMapping("/transaction-summary")
    public Map<String, Object> getTxnSummary() {
        return data.queryMapFromFile("transaction_summary.json");
    }

    @GetMapping("/transactions")
    public List<Map<String, Object>> getTxns() {
        return data.queryFromFile("transactions.json");
    }

    @GetMapping("/risk/overview")
    public Map<String, Object> getRiskOverview() {
        return data.queryMapFromFile("risk_overview.json");
    }

    @GetMapping("/risk/alerts")
    public List<Map<String, Object>> getRiskAlerts() {
        return data.queryFromFile("risk_alerts.json");
    }
}

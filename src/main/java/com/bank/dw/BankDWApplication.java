package com.bank.dw;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 银行数据仓库 — Spring Boot 主入口
 * 启动后访问: http://localhost:8899/dashboard.html
 */
@SpringBootApplication
public class BankDWApplication {
    public static void main(String[] args) {
        SpringApplication.run(BankDWApplication.class, args);
    }
}

package com.reactiveplatform.order;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Order orchestrator - WebClient, Resilience4j, 202 async API, Kafka saga.
 * <p>
 * Phase 0 skeleton - implemented in Phase 4.
 */
@SpringBootApplication
public class OrderServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(OrderServiceApplication.class, args);
    }
}

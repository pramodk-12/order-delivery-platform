package com.reactiveplatform.gateway;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Spring Cloud Gateway (reactive edge): routing, JWT, Redis rate limiting.
 * <p>
 * Phase 0 skeleton - implemented in Phase 8.
 */
@SpringBootApplication
public class GatewayApplication {

    public static void main(String[] args) {
        SpringApplication.run(GatewayApplication.class, args);
    }
}

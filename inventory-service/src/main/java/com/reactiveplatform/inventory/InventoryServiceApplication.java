package com.reactiveplatform.inventory;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Inventory - WebFlux + R2DBC (MySQL), reactive transactions, optimistic locking.
 * <p>
 * Phase 0 skeleton - implemented in Phase 3.
 */
@SpringBootApplication
public class InventoryServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(InventoryServiceApplication.class, args);
    }
}

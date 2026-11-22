package com.reactiveplatform.orderquery;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * CQRS read side for orders.
 * <p>
 * The write side (order-service) owns the truth in MySQL. This service consumes the Kafka events
 * (orders.created, orders.status-changed, payments.*) and maintains a denormalised order-history
 * projection in PostgreSQL via R2DBC, optimised for read-heavy queries such as
 * {@code GET /customers/{id}/orders}. Reads are eventually consistent with the write side.
 * <p>
 * Phase 0 skeleton - implemented in Phase 6.
 */
@SpringBootApplication
public class OrderQueryServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(OrderQueryServiceApplication.class, args);
    }
}

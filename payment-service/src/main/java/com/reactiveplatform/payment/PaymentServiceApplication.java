package com.reactiveplatform.payment;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Payment - deliberately blocking Spring MVC + JPA + MySQL, @Async/CompletableFuture.
 * <p>
 * Phase 0 skeleton - implemented in Phase 5.
 */
@SpringBootApplication
public class PaymentServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(PaymentServiceApplication.class, args);
    }
}

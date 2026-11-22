package com.reactiveplatform.product;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Product catalog - WebFlux + Reactive MongoDB, NDJSON streaming, change-stream SSE.
 * <p>
 * Phase 0 skeleton - implemented in Phase 2.
 */
@SpringBootApplication
public class ProductServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(ProductServiceApplication.class, args);
    }
}

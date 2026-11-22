package com.reactiveplatform.tracking;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Live tracking - SSE, hot publishers, reactive Redis.
 * <p>
 * Phase 0 skeleton - implemented in Phase 6.
 */
@SpringBootApplication
public class TrackingServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(TrackingServiceApplication.class, args);
    }
}

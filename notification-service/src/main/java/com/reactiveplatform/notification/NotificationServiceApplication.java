package com.reactiveplatform.notification;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Notifications - reactive Kafka consumer, backpressure, WebSocket push.
 * <p>
 * Phase 0 skeleton - implemented in Phase 6.
 */
@SpringBootApplication
public class NotificationServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(NotificationServiceApplication.class, args);
    }
}

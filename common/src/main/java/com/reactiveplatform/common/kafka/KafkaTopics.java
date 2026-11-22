package com.reactiveplatform.common.kafka;

/**
 * Single source of truth for topic names. Topics are pre-created by infra/docker-compose.yml (kafka-init)
 * with 3 partitions so partition-level ordering and consumer-group scaling can be demonstrated.
 */
public final class KafkaTopics {

    public static final String ORDER_CREATED = "orders.created";
    public static final String ORDER_STATUS_CHANGED = "orders.status-changed";
    public static final String PAYMENT_COMPLETED = "payments.completed";
    public static final String PAYMENT_FAILED = "payments.failed";

    /** Suffix for dead-letter topics, e.g. {@code orders.created.DLT}. */
    public static final String DLT_SUFFIX = ".DLT";

    private KafkaTopics() {
    }

    public static String dlt(String topic) {
        return topic + DLT_SUFFIX;
    }
}

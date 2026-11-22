package com.reactiveplatform.common.web;

/**
 * Correlation id propagation keys.
 * <ul>
 *   <li>{@link #HEADER} – HTTP header and Kafka record header</li>
 *   <li>{@link #CONTEXT_KEY} – key in Reactor {@code Context} and in the logging MDC</li>
 * </ul>
 */
public final class CorrelationId {

    public static final String HEADER = "X-Correlation-Id";
    public static final String CONTEXT_KEY = "correlationId";

    private CorrelationId() {
    }
}

package com.reactiveplatform.common.event;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/** Published by payment-service when the charge succeeds; order-service confirms the order. */
public record PaymentCompletedEvent(
        String eventId,
        Instant occurredAt,
        String orderId,
        String paymentId,
        BigDecimal amount
) implements DomainEvent {

    public static PaymentCompletedEvent of(String orderId, String paymentId, BigDecimal amount) {
        return new PaymentCompletedEvent(UUID.randomUUID().toString(), Instant.now(), orderId, paymentId, amount);
    }
}

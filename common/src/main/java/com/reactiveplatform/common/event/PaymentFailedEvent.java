package com.reactiveplatform.common.event;

import java.time.Instant;
import java.util.UUID;

/** Published by payment-service when the charge fails; order-service runs compensation (releases stock). */
public record PaymentFailedEvent(
        String eventId,
        Instant occurredAt,
        String orderId,
        String reason
) implements DomainEvent {

    public static PaymentFailedEvent of(String orderId, String reason) {
        return new PaymentFailedEvent(UUID.randomUUID().toString(), Instant.now(), orderId, reason);
    }
}

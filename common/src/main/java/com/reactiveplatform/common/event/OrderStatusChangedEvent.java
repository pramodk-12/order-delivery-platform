package com.reactiveplatform.common.event;

import com.reactiveplatform.common.model.OrderStatus;

import java.time.Instant;
import java.util.UUID;

/** Published by order-service on every status transition; consumed by notification and tracking services. */
public record OrderStatusChangedEvent(
        String eventId,
        Instant occurredAt,
        String orderId,
        String customerId,
        OrderStatus previousStatus,
        OrderStatus newStatus
) implements DomainEvent {

    public static OrderStatusChangedEvent of(String orderId, String customerId, OrderStatus previous, OrderStatus next) {
        return new OrderStatusChangedEvent(UUID.randomUUID().toString(), Instant.now(), orderId, customerId, previous, next);
    }
}

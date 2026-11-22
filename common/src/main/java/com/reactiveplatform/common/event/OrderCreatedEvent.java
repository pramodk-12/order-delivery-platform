package com.reactiveplatform.common.event;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** Published by order-service once stock is reserved; starts the payment saga. */
public record OrderCreatedEvent(
        String eventId,
        Instant occurredAt,
        String orderId,
        String customerId,
        List<OrderItem> items,
        BigDecimal totalAmount
) implements DomainEvent {

    public static OrderCreatedEvent of(String orderId, String customerId, List<OrderItem> items, BigDecimal totalAmount) {
        return new OrderCreatedEvent(UUID.randomUUID().toString(), Instant.now(), orderId, customerId, List.copyOf(items), totalAmount);
    }
}

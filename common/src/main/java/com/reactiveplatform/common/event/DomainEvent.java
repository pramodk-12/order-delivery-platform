package com.reactiveplatform.common.event;

import java.time.Instant;

/**
 * Base contract for every event published to Kafka.
 * <p>
 * Sealed so the set of events is closed and can be handled with exhaustive
 * {@code switch} pattern matching in consumers.
 */
public sealed interface DomainEvent
        permits OrderCreatedEvent, PaymentCompletedEvent, PaymentFailedEvent, OrderStatusChangedEvent {

    /** Unique id of this event, used by consumers for idempotency checks. */
    String eventId();

    /** When the event happened at the source. */
    Instant occurredAt();

    /** Every event in this platform relates to an order; used as the Kafka message key for per-order ordering. */
    String orderId();
}

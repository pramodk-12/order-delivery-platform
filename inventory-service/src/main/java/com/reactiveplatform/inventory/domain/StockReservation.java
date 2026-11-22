package com.reactiveplatform.inventory.domain;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.time.Instant;

/** One reserved line of an order. Status moves RESERVED -> CONFIRMED (paid) or RELEASED (cancelled). */
@Table("stock_reservation")
public record StockReservation(
        @Id Long id,
        String orderId,
        String sku,
        int quantity,
        ReservationStatus status,
        Instant createdAt,
        Instant updatedAt
) {

    public static StockReservation reserved(String orderId, String sku, int quantity) {
        Instant now = Instant.now();
        return new StockReservation(null, orderId, sku, quantity, ReservationStatus.RESERVED, now, now);
    }
}

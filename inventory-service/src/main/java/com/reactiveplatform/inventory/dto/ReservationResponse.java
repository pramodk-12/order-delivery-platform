package com.reactiveplatform.inventory.dto;

import com.reactiveplatform.inventory.domain.ReservationStatus;
import com.reactiveplatform.inventory.domain.StockReservation;

import java.util.List;

public record ReservationResponse(
        String orderId,
        ReservationStatus status,
        List<ReservationLine> items
) {

    /** All lines of one order always share the same status (they change together, in one transaction). */
    public static ReservationResponse from(String orderId, List<StockReservation> rows) {
        List<ReservationLine> lines = rows.stream()
                .map(r -> new ReservationLine(r.sku(), r.quantity()))
                .toList();
        return new ReservationResponse(orderId, rows.getFirst().status(), lines);
    }
}

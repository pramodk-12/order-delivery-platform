package com.reactiveplatform.inventory.service;

import com.reactiveplatform.inventory.dto.ReservationResponse;

/** {@code created = false} means the request was a replay of an existing reservation (idempotency). */
public record ReservationOutcome(ReservationResponse reservation, boolean created) {
}

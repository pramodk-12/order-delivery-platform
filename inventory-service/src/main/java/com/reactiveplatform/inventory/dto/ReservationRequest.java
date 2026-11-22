package com.reactiveplatform.inventory.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Size;

import java.util.List;

/**
 * Reserve stock for every line of an order - ALL or NOTHING.
 * The orderId doubles as an idempotency key: sending the same request twice reserves only once.
 */
public record ReservationRequest(
        @NotBlank @Size(max = 64) String orderId,
        @NotEmpty @Size(max = 50) List<@Valid ReservationLine> items
) {
}

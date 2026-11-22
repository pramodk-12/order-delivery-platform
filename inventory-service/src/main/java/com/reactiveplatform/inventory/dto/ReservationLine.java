package com.reactiveplatform.inventory.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

public record ReservationLine(
        @NotBlank String sku,
        @NotNull @Positive @Max(1_000) Integer quantity
) {
}

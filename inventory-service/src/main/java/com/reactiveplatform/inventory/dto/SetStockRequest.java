package com.reactiveplatform.inventory.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;

/** Admin "restock": sets the AVAILABLE quantity for a SKU (creates the SKU if it doesn't exist). */
public record SetStockRequest(
        @NotNull @PositiveOrZero @Max(1_000_000) Integer available
) {
}

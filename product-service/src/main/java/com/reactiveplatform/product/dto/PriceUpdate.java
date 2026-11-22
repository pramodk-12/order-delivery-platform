package com.reactiveplatform.product.dto;

import java.math.BigDecimal;
import java.time.Instant;

/** Payload of each Server-Sent Event on /api/catalog/price-updates. */
public record PriceUpdate(
        String productId,
        String sku,
        String name,
        BigDecimal newPrice,
        Instant changedAt
) {
}

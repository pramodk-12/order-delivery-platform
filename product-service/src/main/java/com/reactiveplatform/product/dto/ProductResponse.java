package com.reactiveplatform.product.dto;

import com.reactiveplatform.product.domain.Product;

import java.math.BigDecimal;
import java.time.Instant;

/** API representation - kept separate from the document so the storage model can change freely. */
public record ProductResponse(
        String id,
        String sku,
        String name,
        String description,
        String category,
        BigDecimal price,
        Instant createdAt,
        Instant updatedAt
) {

    public static ProductResponse from(Product p) {
        return new ProductResponse(p.id(), p.sku(), p.name(), p.description(), p.category(), p.price(),
                p.createdAt(), p.updatedAt());
    }
}

package com.reactiveplatform.product.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

/**
 * Product details only. Price is deliberately NOT here: a price change is its own business operation
 * (PATCH /api/products/{id}/price) because it is streamed live to clients and, in real life, audited.
 */
public record UpdateProductRequest(
        @NotBlank @Size(max = 40) String sku,
        @NotBlank @Size(max = 200) String name,
        @Size(max = 2000) String description,
        @NotBlank @Size(max = 60) String category
) {
}

package com.reactiveplatform.product.dto;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.math.BigDecimal;

public record CreateProductRequest(
        @NotBlank @Size(max = 40) String sku,
        @NotBlank @Size(max = 200) String name,
        @Size(max = 2000) String description,
        @NotBlank @Size(max = 60) String category,
        @NotNull @DecimalMin("0.01") @Digits(integer = 10, fraction = 2) BigDecimal price
) {
}

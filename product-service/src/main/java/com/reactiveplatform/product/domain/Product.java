package com.reactiveplatform.product.domain;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.FieldType;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * A catalog product, stored as ONE MongoDB document.
 * <p>
 * Why a document store for the catalog: the catalog is read-heavy, each product is always read as a whole
 * (no joins), and product attributes vary by category - all things documents are good at.
 * <p>
 * Immutable record: Spring Data MongoDB creates a new instance (via the canonical constructor) when it
 * needs to set the generated id, so no setters are needed.
 */
@Document("products")
public record Product(
        @Id String id,

        // unique index -> duplicate SKUs are rejected by MongoDB itself (race-free), mapped to HTTP 409
        @Indexed(unique = true) String sku,

        String name,
        String description,

        @Indexed String category,

        // GOTCHA: by default BigDecimal may be stored as a STRING, and "5" > "10" as strings,
        // which silently breaks price range queries. DECIMAL128 stores it as a real decimal number.
        @Field(targetType = FieldType.DECIMAL128) BigDecimal price,

        Instant createdAt,
        Instant updatedAt
) {

    public static Product newProduct(String sku, String name, String description, String category, BigDecimal price) {
        Instant now = Instant.now();
        return new Product(null, sku, name, description, category, price, now, now);
    }
}

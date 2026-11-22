package com.reactiveplatform.inventory.dto;

import com.reactiveplatform.inventory.domain.InventoryItem;

import java.time.Instant;

public record StockLevelResponse(
        String sku,
        int available,
        int reserved,
        long version,
        Instant updatedAt
) {

    public static StockLevelResponse from(InventoryItem item) {
        return new StockLevelResponse(item.sku(), item.available(), item.reserved(),
                item.version() == null ? 0 : item.version(), item.updatedAt());
    }
}

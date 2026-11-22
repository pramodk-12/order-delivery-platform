package com.reactiveplatform.inventory.domain;

import org.springframework.data.r2dbc.repository.R2dbcRepository;

public interface InventoryItemRepository extends R2dbcRepository<InventoryItem, String> {
}

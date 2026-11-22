package com.reactiveplatform.inventory.service;

import reactor.core.publisher.Mono;

/**
 * Moves {@code quantity} of a SKU from "available" to "reserved", or fails with
 * InsufficientStockException / UnknownSkuException. Always called INSIDE the caller's transaction.
 */
public interface StockUpdater {

    ReservationStrategy strategy();

    Mono<Void> reserve(String sku, int quantity);
}

package com.reactiveplatform.inventory.service;

import com.reactiveplatform.inventory.domain.InventoryItemRepository;
import com.reactiveplatform.inventory.service.InventoryExceptions.InsufficientStockException;
import com.reactiveplatform.inventory.service.InventoryExceptions.UnknownSkuException;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

/**
 * Strategy OPTIMISTIC: read -> check in Java -> save with a version check.
 * <p>
 * {@code save()} runs {@code UPDATE ... WHERE sku = ? AND version = ?}. If another transaction changed the
 * row since we read it, 0 rows match and Spring throws OptimisticLockingFailureException.
 * InventoryService then retries the WHOLE transaction (see the comment there for why it must be the whole one).
 */
@Component
public class OptimisticStockUpdater implements StockUpdater {

    private final InventoryItemRepository items;

    public OptimisticStockUpdater(InventoryItemRepository items) {
        this.items = items;
    }

    @Override
    public ReservationStrategy strategy() {
        return ReservationStrategy.OPTIMISTIC;
    }

    @Override
    public Mono<Void> reserve(String sku, int quantity) {
        return items.findById(sku)
                .switchIfEmpty(Mono.error(() -> new UnknownSkuException(sku)))
                .flatMap(item -> item.available() < quantity
                        ? Mono.error(new InsufficientStockException(sku, quantity, item.available()))
                        : items.save(item.reserve(quantity)))
                .then();
    }
}

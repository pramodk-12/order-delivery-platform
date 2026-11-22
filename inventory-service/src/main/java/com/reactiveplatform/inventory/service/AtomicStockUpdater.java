package com.reactiveplatform.inventory.service;

import com.reactiveplatform.inventory.domain.InventoryItemRepository;
import com.reactiveplatform.inventory.service.InventoryExceptions.InsufficientStockException;
import com.reactiveplatform.inventory.service.InventoryExceptions.UnknownSkuException;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Instant;

/**
 * Strategy ATOMIC: "check and decrement" in ONE SQL statement.
 * <p>
 * MySQL locks the row while it evaluates {@code available >= :qty} and applies the change, so two buyers
 * can never both take the last unit: the second one waits for the lock, re-checks, and matches 0 rows.
 * <p>
 * Uses {@link DatabaseClient} - Spring's reactive "JdbcTemplate" for hand-written SQL.
 */
@Component
public class AtomicStockUpdater implements StockUpdater {

    private static final String RESERVE_SQL = """
            UPDATE inventory_item
               SET available  = available - :qty,
                   reserved   = reserved + :qty,
                   version    = version + 1,
                   updated_at = :now
             WHERE sku = :sku
               AND available >= :qty
            """;

    private final DatabaseClient db;
    private final InventoryItemRepository items;

    public AtomicStockUpdater(DatabaseClient db, InventoryItemRepository items) {
        this.db = db;
        this.items = items;
    }

    @Override
    public ReservationStrategy strategy() {
        return ReservationStrategy.ATOMIC;
    }

    @Override
    public Mono<Void> reserve(String sku, int quantity) {
        return db.sql(RESERVE_SQL)
                .bind("qty", quantity)
                .bind("sku", sku)
                .bind("now", Instant.now())
                .fetch()
                .rowsUpdated()
                .flatMap(rows -> rows == 1 ? Mono.<Void>empty() : explainFailure(sku, quantity));
    }

    /** 0 rows updated: either the SKU doesn't exist, or there isn't enough stock. Find out which. */
    private Mono<Void> explainFailure(String sku, int quantity) {
        return items.findById(sku)
                .switchIfEmpty(Mono.error(() -> new UnknownSkuException(sku)))
                .flatMap(item -> Mono.error(new InsufficientStockException(sku, quantity, item.available())));
    }
}

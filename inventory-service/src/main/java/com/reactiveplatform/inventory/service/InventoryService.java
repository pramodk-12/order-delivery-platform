package com.reactiveplatform.inventory.service;

import com.reactiveplatform.inventory.domain.InventoryItemRepository;
import com.reactiveplatform.inventory.domain.ReservationStatus;
import com.reactiveplatform.inventory.domain.StockReservation;
import com.reactiveplatform.inventory.domain.StockReservationRepository;
import com.reactiveplatform.inventory.dto.ReservationLine;
import com.reactiveplatform.inventory.dto.ReservationRequest;
import com.reactiveplatform.inventory.dto.ReservationResponse;
import com.reactiveplatform.inventory.dto.StockLevelResponse;
import com.reactiveplatform.inventory.service.InventoryExceptions.InvalidReservationStateException;
import com.reactiveplatform.inventory.service.InventoryExceptions.ReservationNotFoundException;
import com.reactiveplatform.inventory.service.InventoryExceptions.StockContentionException;
import com.reactiveplatform.inventory.service.InventoryExceptions.UnknownSkuException;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Stock levels and reservations.
 * <p>
 * REACTIVE TRANSACTIONS: there are two ways to use them, and this class shows both.
 * <ul>
 *   <li>{@code @Transactional} on a method returning Mono/Flux (release, confirm) - same annotation as in MVC</li>
 *   <li>{@link TransactionalOperator} - programmatic, when you need control around the transaction (reserve)</li>
 * </ul>
 * Either way the transaction is bound to the Reactor CONTEXT of the subscription, not to a ThreadLocal,
 * because a reactive request can hop between threads (exercise 8).
 */
@Service
public class InventoryService {

    private static final String SET_STOCK_SQL = """
            INSERT INTO inventory_item (sku, available, reserved, version, updated_at)
            VALUES (:sku, :available, 0, 0, :now)
            ON DUPLICATE KEY UPDATE available = :available, version = version + 1, updated_at = :now
            """;

    private static final String RELEASE_STOCK_SQL = """
            UPDATE inventory_item
               SET available = available + :qty, reserved = reserved - :qty, version = version + 1, updated_at = :now
             WHERE sku = :sku
            """;

    private static final String CONFIRM_STOCK_SQL = """
            UPDATE inventory_item
               SET reserved = reserved - :qty, version = version + 1, updated_at = :now
             WHERE sku = :sku
            """;

    // Guarded status change: only RESERVED rows move. A duplicate/concurrent release updates 0 rows
    // and therefore never returns the same stock twice.
    private static final String MOVE_STATUS_SQL = """
            UPDATE stock_reservation
               SET status = :target, updated_at = :now
             WHERE id = :id AND status = 'RESERVED'
            """;

    private final InventoryItemRepository items;
    private final StockReservationRepository reservations;
    private final R2dbcEntityTemplate template;
    private final DatabaseClient db;
    private final TransactionalOperator transactionalOperator;
    private final Map<ReservationStrategy, StockUpdater> updaters;
    private final ReservationStrategy defaultStrategy;
    private final int maxRetries;
    private final MeterRegistry meters;

    public InventoryService(InventoryItemRepository items,
                            StockReservationRepository reservations,
                            R2dbcEntityTemplate template,
                            DatabaseClient db,
                            TransactionalOperator transactionalOperator,
                            List<StockUpdater> updaters,
                            MeterRegistry meters,
                            @Value("${inventory.reservation.strategy:ATOMIC}") ReservationStrategy defaultStrategy,
                            @Value("${inventory.reservation.max-retries:10}") int maxRetries) {
        this.items = items;
        this.reservations = reservations;
        this.template = template;
        this.db = db;
        this.transactionalOperator = transactionalOperator;
        this.updaters = updaters.stream().collect(Collectors.toUnmodifiableMap(StockUpdater::strategy, Function.identity()));
        this.meters = meters;
        this.defaultStrategy = defaultStrategy;
        this.maxRetries = maxRetries;
    }

    // ============================================================================================ stock levels

    public Mono<StockLevelResponse> getStock(String sku) {
        return items.findById(sku)
                .map(StockLevelResponse::from)
                .switchIfEmpty(Mono.error(() -> new UnknownSkuException(sku)));
    }

    /** Several SKUs in one query - order-service will use this to check a whole cart at once. */
    public Flux<StockLevelResponse> getStock(Collection<String> skus) {
        return items.findAllById(skus).map(StockLevelResponse::from);
    }

    /** Restock: an UPSERT (MySQL "INSERT ... ON DUPLICATE KEY UPDATE") - one atomic statement. */
    public Mono<StockLevelResponse> setStock(String sku, int available) {
        return db.sql(SET_STOCK_SQL)
                .bind("sku", sku)
                .bind("available", available)
                .bind("now", Instant.now())
                .fetch()
                .rowsUpdated()
                .then(Mono.defer(() -> getStock(sku)));
    }

    // ============================================================================================ reservations

    public Mono<ReservationOutcome> reserve(ReservationRequest request) {
        return reserve(request, defaultStrategy);
    }

    /**
     * Reserves every line of the order, ALL OR NOTHING, using the given strategy.
     * <p>
     * Order of operators matters:
     * <pre>
     *   reserveOnce(...)                 the work
     *     .as(transactional)             ONE attempt = ONE transaction (commit, or roll back everything)
     *     .retryWhen(...)                retry = re-subscribe = a brand-new transaction
     * </pre>
     * Why the retry must be OUTSIDE the transaction: MySQL's default isolation (REPEATABLE READ) gives a
     * transaction a snapshot taken at its first read. Retrying inside the same transaction would re-read the
     * same stale row and lose the version check forever.
     */
    public Mono<ReservationOutcome> reserve(ReservationRequest request, ReservationStrategy strategy) {
        StockUpdater updater = updaters.get(strategy);
        List<ReservationLine> lines = normalize(request.items());

        return Mono.defer(() -> reserveOnce(request.orderId(), lines, updater))
                .as(transactionalOperator::transactional)
                .retryWhen(Retry.backoff(maxRetries, Duration.ofMillis(20))
                        .maxBackoff(Duration.ofMillis(500))
                        .jitter(0.75)                       // spread retries so competitors don't collide again
                        .filter(InventoryService::isRetryable)
                        // metric: a rising retry rate means contention (visible at /actuator/metrics)
                        .doBeforeRetry(signal -> meters.counter("inventory.reservation.retries",
                                "strategy", strategy.name(),
                                "reason", signal.failure().getClass().getSimpleName()).increment())
                        .onRetryExhaustedThrow((spec, signal) ->
                                new StockContentionException(request.orderId(), signal.totalRetries())));
    }

    public Mono<ReservationResponse> getReservation(String orderId) {
        return reservations.findByOrderIdOrderBySku(orderId)
                .collectList()
                .filter(rows -> !rows.isEmpty())
                .map(rows -> ReservationResponse.from(orderId, rows))
                .switchIfEmpty(Mono.error(() -> new ReservationNotFoundException(orderId)));
    }

    /** Saga compensation (Phase 5: payment failed) - puts the stock back. Idempotent. */
    @Transactional
    public Mono<ReservationResponse> release(String orderId) {
        return transition(orderId, ReservationStatus.RELEASED, RELEASE_STOCK_SQL);
    }

    /** Payment succeeded (Phase 5) - reserved stock becomes sold. Idempotent. */
    @Transactional
    public Mono<ReservationResponse> confirm(String orderId) {
        return transition(orderId, ReservationStatus.CONFIRMED, CONFIRM_STOCK_SQL);
    }

    // ============================================================================================ internals

    private Mono<ReservationOutcome> reserveOnce(String orderId, List<ReservationLine> lines, StockUpdater updater) {
        return reservations.findByOrderIdOrderBySku(orderId)
                .collectList()
                .flatMap(existing -> existing.isEmpty()
                        ? createReservation(orderId, lines, updater).map(r -> new ReservationOutcome(r, true))
                        // same orderId again (client retry, Kafka redelivery): return what we already did
                        : Mono.just(new ReservationOutcome(ReservationResponse.from(orderId, existing), false)));
    }

    private Mono<ReservationResponse> createReservation(String orderId, List<ReservationLine> lines, StockUpdater updater) {
        return Flux.fromIterable(lines)
                // concatMap, NOT flatMap: a transaction runs on ONE database connection, and statements on one
                // connection must run one after another. flatMap would fire them concurrently.
                .concatMap(line -> updater.reserve(line.sku(), line.quantity())
                        .then(template.insert(StockReservation.reserved(orderId, line.sku(), line.quantity()))))
                .collectList()
                .map(saved -> ReservationResponse.from(orderId, saved));
    }

    private Mono<ReservationResponse> transition(String orderId, ReservationStatus target, String stockSql) {
        return reservations.findByOrderIdOrderBySku(orderId)
                .collectList()
                .flatMap(rows -> {
                    if (rows.isEmpty()) {
                        return Mono.error(new ReservationNotFoundException(orderId));
                    }
                    ReservationStatus current = rows.getFirst().status();
                    if (current == target) {
                        return Mono.just(ReservationResponse.from(orderId, rows));    // already done: idempotent
                    }
                    if (current != ReservationStatus.RESERVED) {
                        return Mono.error(new InvalidReservationStateException(orderId, current, target));
                    }
                    return Flux.fromIterable(rows)
                            .concatMap(row -> moveStatus(row, target)
                                    .filter(changed -> changed)     // only whoever moved the status adjusts stock
                                    .flatMap(changed -> adjustStock(stockSql, row)))
                            .then(Mono.defer(() -> getReservation(orderId)));
                });
    }

    private Mono<Boolean> moveStatus(StockReservation row, ReservationStatus target) {
        return db.sql(MOVE_STATUS_SQL)
                .bind("target", target.name())
                .bind("now", Instant.now())
                .bind("id", row.id())
                .fetch()
                .rowsUpdated()
                .map(updated -> updated == 1);
    }

    private Mono<Long> adjustStock(String sql, StockReservation row) {
        return db.sql(sql)
                .bind("qty", row.quantity())
                .bind("sku", row.sku())
                .bind("now", Instant.now())
                .fetch()
                .rowsUpdated();
    }

    /**
     * Merge duplicate SKUs and SORT by SKU. Every transaction then locks rows in the same order, so two orders
     * containing {A, B} and {B, A} can't deadlock each other.
     */
    private static List<ReservationLine> normalize(List<ReservationLine> items) {
        Map<String, Integer> merged = new TreeMap<>();
        items.forEach(line -> merged.merge(line.sku(), line.quantity(), Integer::sum));
        return merged.entrySet().stream()
                .map(e -> new ReservationLine(e.getKey(), e.getValue()))
                .toList();
    }

    /**
     * Worth retrying (a fresh transaction may succeed):
     * <ul>
     *   <li>TransientDataAccessException - includes OptimisticLockingFailureException (lost a version race)
     *       and PessimisticLockingFailureException (MySQL deadlock / lock wait timeout)</li>
     *   <li>DuplicateKeyException - the same orderId was submitted twice at the same moment; the next attempt
     *       finds the other one's reservation and replays it</li>
     * </ul>
     * NOT retried: InsufficientStock, UnknownSku - retrying can't fix those.
     */
    private static boolean isRetryable(Throwable error) {
        return error instanceof TransientDataAccessException || error instanceof DuplicateKeyException;
    }
}

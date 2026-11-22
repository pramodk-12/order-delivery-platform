package com.reactiveplatform.inventory;

import com.reactiveplatform.inventory.domain.StockReservationRepository;
import com.reactiveplatform.inventory.dto.ReservationLine;
import com.reactiveplatform.inventory.dto.ReservationRequest;
import com.reactiveplatform.inventory.dto.StockLevelResponse;
import com.reactiveplatform.inventory.service.InventoryExceptions.InsufficientStockException;
import com.reactiveplatform.inventory.service.InventoryExceptions.StockContentionException;
import com.reactiveplatform.inventory.service.InventoryService;
import com.reactiveplatform.inventory.service.ReservationStrategy;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * THE interview question: "100 people try to buy the last 50 units at the same moment. What happens?"
 * <p>
 * Fires 100 reservations concurrently (all in flight at once) against a real MySQL, with each strategy,
 * and checks the invariants that must ALWAYS hold. Results are logged so you can quote real numbers.
 */
class OversellingConcurrencyTest extends AbstractInventoryIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(OversellingConcurrencyTest.class);

    @Autowired
    InventoryService inventory;

    @Autowired
    StockReservationRepository reservations;

    @Autowired
    MeterRegistry meters;

    @ParameterizedTest(name = "{0}")
    @EnumSource(ReservationStrategy.class)
    void hundred_concurrent_buyers_for_fifty_units_never_oversell(ReservationStrategy strategy) {
        givenStock("HOT-1", 50);

        long start = System.nanoTime();
        Map<String, Long> outcomes = Flux.range(1, 100)
                .flatMap(i -> inventory.reserve(
                                        new ReservationRequest(strategy + "-order-" + i, List.of(new ReservationLine("HOT-1", 1))),
                                        strategy)
                                .map(ok -> "RESERVED")
                                .onErrorResume(InsufficientStockException.class, e -> Mono.just("SOLD_OUT"))
                                .onErrorResume(StockContentionException.class, e -> Mono.just("GAVE_UP")),
                        100)                                      // all 100 requests in flight at once
                .collect(Collectors.groupingBy(Function.identity(), TreeMap::new, Collectors.counting()))
                .block(Duration.ofMinutes(2));
        long tookMs = Duration.ofNanos(System.nanoTime() - start).toMillis();

        StockLevelResponse hot = inventory.getStock("HOT-1").block();
        Long reservationRows = reservations.findBySku("HOT-1").count().block();
        long reserved = outcomes.getOrDefault("RESERVED", 0L);
        double retries = meters.find("inventory.reservation.retries").tag("strategy", strategy.name())
                .counters().stream().mapToDouble(c -> c.count()).sum();

        log.info("RESULT {} -> {} | available={} reserved={} rows={} | retries={} | {} ms",
                strategy, outcomes, hot.available(), hot.reserved(), reservationRows, (long) retries, tookMs);

        // Invariants that must hold for EVERY strategy:
        assertThat(outcomes.values().stream().mapToLong(Long::longValue).sum()).isEqualTo(100); // every buyer got an answer
        assertThat(hot.available()).isGreaterThanOrEqualTo(0);                                 // never negative
        assertThat(hot.available() + hot.reserved()).isEqualTo(50);                            // no unit created or lost
        assertThat(hot.reserved()).isEqualTo(reserved);                                        // stock matches successes
        assertThat(reservationRows).isEqualTo(reserved);                                       // and matches the rows

        if (strategy == ReservationStrategy.ATOMIC) {
            // the database decides atomically: exactly 50 winners, exactly 50 clean "sold out" answers
            assertThat(outcomes).containsEntry("RESERVED", 50L)
                    .containsEntry("SOLD_OUT", 50L)
                    .doesNotContainKey("GAVE_UP");
        }
    }

    @Test
    void orders_listing_the_same_skus_in_opposite_order_all_succeed() {
        givenStock("SKU-A", 100);
        givenStock("SKU-B", 100);

        Map<String, Long> outcomes = Flux.range(1, 100)
                .flatMap(i -> {
                    List<ReservationLine> lines = i % 2 == 0
                            ? List.of(new ReservationLine("SKU-A", 1), new ReservationLine("SKU-B", 1))
                            : List.of(new ReservationLine("SKU-B", 1), new ReservationLine("SKU-A", 1));
                    return inventory.reserve(new ReservationRequest("pair-" + i, lines), ReservationStrategy.ATOMIC)
                            .map(ok -> "RESERVED")
                            .onErrorResume(e -> Mono.just(e.getClass().getSimpleName()));
                }, 100)
                .collect(Collectors.groupingBy(Function.identity(), TreeMap::new, Collectors.counting()))
                .block(Duration.ofMinutes(2));

        log.info("RESULT opposite-order pairs -> {}", outcomes);

        // lines are sorted before locking, so every transaction locks SKU-A then SKU-B: no deadlock cycle
        assertThat(outcomes).containsOnlyKeys("RESERVED").containsEntry("RESERVED", 100L);
        assertThat(inventory.getStock("SKU-A").block().available()).isZero();
        assertThat(inventory.getStock("SKU-B").block().available()).isZero();
    }
}

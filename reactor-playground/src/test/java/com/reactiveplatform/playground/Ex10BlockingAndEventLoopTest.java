package com.reactiveplatform.playground;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * EXERCISE 10 - Why non-blocking scales, and how to live with blocking code.
 * <p>
 * Ties Reactor to Netty (WebFlux): a few event-loop threads serve thousands of connections ONLY IF
 * nobody blocks them. Blocking work must be moved to boundedElastic (Phase 7: IBM DB2 via JDBC).
 */
class Ex10BlockingAndEventLoopTest {

    @Test
    void a_thousand_concurrent_waits_need_only_a_handful_of_threads() {
        Set<String> threads = ConcurrentHashMap.newKeySet();
        long start = System.nanoTime();

        // 1000 "requests", each waiting 200ms on a non-blocking timer (think: awaiting a DB/HTTP response)
        Long count = Flux.range(1, 1000)
                .flatMap(i -> Mono.delay(Duration.ofMillis(200)).doOnNext(x -> threads.add(Thread.currentThread().getName())), 1000)
                .count()
                .block();

        Duration took = Duration.ofNanos(System.nanoTime() - start);

        assertThat(count).isEqualTo(1000L);
        assertThat(took).isLessThan(Duration.ofSeconds(2));       // thread-per-request with 8 threads: ~25s
        assertThat(threads.size()).isLessThanOrEqualTo(Runtime.getRuntime().availableProcessors());
    }

    @Test
    void wrap_blocking_calls_with_fromCallable_and_subscribeOn_boundedElastic() {
        AtomicReference<String> jdbcThread = new AtomicReference<>();

        Mono<String> customer = Mono.fromCallable(() -> {
                    jdbcThread.set(Thread.currentThread().getName());
                    return legacyJdbcFindCustomer("42");
                })
                .subscribeOn(Schedulers.boundedElastic());

        StepVerifier.create(customer).expectNext("customer-42").verifyComplete();
        assertThat(jdbcThread.get()).startsWith("boundedElastic-");
    }

    @Test
    void calling_block_on_a_non_blocking_thread_is_rejected_by_reactor() {
        // parallel() threads (like Netty event loops) are marked NonBlocking; Reactor refuses block() there.
        // NOTE: Mono.just(x).block() would NOT trip this check - MonoJust short-circuits and returns x directly.
        // A genuinely asynchronous source (a timer, an HTTP call, a DB query) does.
        Mono<Long> misuse = Mono.just(1)
                .publishOn(Schedulers.parallel())
                .map(i -> Mono.delay(Duration.ofMillis(10)).block());

        StepVerifier.create(misuse)
                .expectErrorSatisfies(e -> assertThat(e)
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("block()/blockFirst()/blockLast() are blocking"))
                .verify();
    }

    @Test
    void the_fix_is_to_compose_with_flatMap_instead_of_blocking() {
        Mono<Integer> correct = Mono.just(1)
                .publishOn(Schedulers.parallel())
                .flatMap(i -> Mono.just(i * 2));

        StepVerifier.create(correct).expectNext(2).verifyComplete();
    }

    private static String legacyJdbcFindCustomer(String id) throws InterruptedException {
        Thread.sleep(50);   // pretend this is a DB2 round trip
        return "customer-" + id;
    }
}

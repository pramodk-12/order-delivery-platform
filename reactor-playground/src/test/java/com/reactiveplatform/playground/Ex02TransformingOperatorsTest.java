package com.reactiveplatform.playground;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * EXERCISE 2 - Transforming operators.
 * <p>
 * THE interview question: map vs flatMap vs concatMap vs flatMapSequential.
 * <pre>
 *   map               sync 1:1 transform (T -> R). Never put I/O in map.
 *   flatMap           T -> Publisher<R>; subscribes to inner publishers EAGERLY and CONCURRENTLY,
 *                     results INTERLEAVE (arrival order). Default concurrency 256.
 *   concatMap         one inner at a time, waits for it to complete -> SOURCE ORDER, no concurrency.
 *   flatMapSequential concurrent like flatMap, but re-orders output into SOURCE ORDER (buffers early results).
 * </pre>
 * Virtual time (StepVerifier.withVirtualTime) lets us prove the timing without real sleeps.
 */
class Ex02TransformingOperatorsTest {

    /** Simulates a remote call whose latency depends on the id: id 1 -> 300ms, 2 -> 200ms, 3 -> 100ms. */
    private static Mono<String> remoteCall(int id) {
        return Mono.just("item-" + id).delayElement(Duration.ofMillis(400 - id * 100L));
    }

    @Test
    void map_and_filter_are_synchronous() {
        StepVerifier.create(Flux.range(1, 6).filter(i -> i % 2 == 0).map(i -> i * 10))
                .expectNext(20, 40, 60)
                .verifyComplete();
    }

    @Test
    void flatMap_runs_inner_calls_concurrently_and_emits_in_arrival_order() {
        StepVerifier.withVirtualTime(() -> Flux.just(1, 2, 3).flatMap(Ex02TransformingOperatorsTest::remoteCall))
                .expectSubscription()
                .thenAwait(Duration.ofMillis(100)).expectNext("item-3")   // fastest first
                .thenAwait(Duration.ofMillis(100)).expectNext("item-2")
                .thenAwait(Duration.ofMillis(100)).expectNext("item-1")
                .verifyComplete();                                        // total: 300ms (the slowest call)
    }

    @Test
    void concatMap_runs_one_at_a_time_and_keeps_source_order() {
        StepVerifier.withVirtualTime(() -> Flux.just(1, 2, 3).concatMap(Ex02TransformingOperatorsTest::remoteCall))
                .expectSubscription()
                .thenAwait(Duration.ofMillis(300)).expectNext("item-1")
                .thenAwait(Duration.ofMillis(200)).expectNext("item-2")
                .thenAwait(Duration.ofMillis(100)).expectNext("item-3")
                .verifyComplete();                                        // total: 600ms (sum of all calls)
    }

    @Test
    void flatMapSequential_is_concurrent_but_preserves_source_order() {
        StepVerifier.withVirtualTime(() -> Flux.just(1, 2, 3).flatMapSequential(Ex02TransformingOperatorsTest::remoteCall))
                .expectSubscription()
                .expectNoEvent(Duration.ofMillis(299))                    // item-2/3 are done but held back
                .thenAwait(Duration.ofMillis(1))
                .expectNext("item-1", "item-2", "item-3")                 // all released at 300ms, in order
                .verifyComplete();
    }

    @Test
    void flatMap_concurrency_argument_limits_parallel_inner_subscriptions() {
        // concurrency = 1 behaves like concatMap: useful to protect a downstream system.
        StepVerifier.withVirtualTime(() -> Flux.just(1, 2, 3).flatMap(Ex02TransformingOperatorsTest::remoteCall, 1))
                .expectSubscription()
                .thenAwait(Duration.ofMillis(600))
                .expectNext("item-1", "item-2", "item-3")
                .verifyComplete();
    }

    @Test
    void handle_is_map_and_filter_in_one_step() {
        Flux<Integer> parsed = Flux.just("1", "x", "3")
                .handle((s, sink) -> {
                    if (s.chars().allMatch(Character::isDigit)) {
                        sink.next(Integer.parseInt(s));   // emit 0 or 1 element per input
                    }
                });

        StepVerifier.create(parsed).expectNext(1, 3).verifyComplete();
    }

    @Test
    void switchIfEmpty_argument_is_evaluated_eagerly_unless_deferred() {
        AtomicInteger dbCalls = new AtomicInteger();
        Mono<String> cacheHit = Mono.just("from-cache");

        // GOTCHA: loadFromDb(...) is a normal Java method call -> runs at assembly, even on a cache hit.
        Mono<String> wasteful = cacheHit.switchIfEmpty(loadFromDb(dbCalls));
        StepVerifier.create(wasteful).expectNext("from-cache").verifyComplete();
        assertThat(dbCalls).hasValue(1);

        // FIX: wrap in Mono.defer so the fallback is only built when actually needed.
        Mono<String> correct = cacheHit.switchIfEmpty(Mono.defer(() -> loadFromDb(dbCalls)));
        StepVerifier.create(correct).expectNext("from-cache").verifyComplete();
        assertThat(dbCalls).hasValue(1);

        StepVerifier.create(Mono.<String>empty().switchIfEmpty(Mono.defer(() -> loadFromDb(dbCalls))))
                .expectNext("from-db")
                .verifyComplete();
        assertThat(dbCalls).hasValue(2);
    }

    @Test
    void defaultIfEmpty_supplies_a_plain_value() {
        StepVerifier.create(Mono.<String>empty().defaultIfEmpty("guest")).expectNext("guest").verifyComplete();
    }

    @Test
    void aggregation_operators() {
        StepVerifier.create(Flux.range(1, 4).reduce(Integer::sum)).expectNext(10).verifyComplete();
        StepVerifier.create(Flux.range(1, 5).buffer(2))
                .expectNext(List.of(1, 2), List.of(3, 4), List.of(5))
                .verifyComplete();
        StepVerifier.create(Flux.just("a", "b", "a", "c", "b").distinct()).expectNext("a", "b", "c").verifyComplete();
        StepVerifier.create(Flux.range(1, 10).skip(2).take(3)).expectNext(3, 4, 5).verifyComplete();
    }

    @Test
    void groupBy_splits_a_stream_into_keyed_substreams() {
        // Each group MUST be consumed (here via flatMap), otherwise groupBy can stall.
        // Phase 6 uses this to keep per-partition / per-order ordering while processing groups concurrently.
        Flux<String> grouped = Flux.range(1, 5)
                .groupBy(i -> i % 2 == 0 ? "even" : "odd")
                .flatMap(group -> group.collectList().map(list -> describe(group, list)));

        StepVerifier.create(grouped.collectList())
                .assertNext(list -> assertThat(list).containsExactlyInAnyOrder("odd=[1, 3, 5]", "even=[2, 4]"))
                .verifyComplete();
    }

    // ----------------------------------------------------------------------------------- helpers

    private static Mono<String> loadFromDb(AtomicInteger counter) {
        counter.incrementAndGet();
        return Mono.just("from-db");
    }

    private static String describe(GroupedFlux<String, Integer> group, List<Integer> values) {
        return group.key() + "=" + values;
    }
}

package com.reactiveplatform.playground;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * EXERCISE 3 - Combining publishers.
 * <pre>
 *   Mono.zip      wait for ALL, run in PARALLEL, combine results  -> Order Service: product + stock + customer
 *   merge         interleave as items arrive (concurrent subscription)
 *   concat        A fully, then B (sequential subscription)
 *   then / when   "do this after that", ignore values
 *   firstWithValue race: first publisher to emit a value wins (e.g. primary vs replica)
 * </pre>
 */
class Ex03CombiningPublishersTest {

    private static Mono<String> call(String value, long latencyMs) {
        return Mono.just(value).delayElement(Duration.ofMillis(latencyMs));
    }

    @Test
    void zip_runs_calls_in_parallel_so_total_time_is_the_slowest_not_the_sum() {
        StepVerifier.withVirtualTime(() ->
                        Mono.zip(call("product", 300), call("stock", 200), call("customer", 100))
                                .map(t -> t.getT1() + "|" + t.getT2() + "|" + t.getT3()))
                .expectSubscription()
                .expectNoEvent(Duration.ofMillis(299))
                .thenAwait(Duration.ofMillis(1))
                .expectNext("product|stock|customer")   // at 300ms, not 600ms
                .verifyComplete();
    }

    @Test
    void zip_with_an_empty_source_completes_empty_without_calling_the_combinator() {
        // GOTCHA: if any source is empty, zip produces NOTHING. Guard with defaultIfEmpty / switchIfEmpty.
        StepVerifier.create(Mono.zip(Mono.just("a"), Mono.<String>empty()))
                .verifyComplete();

        StepVerifier.create(Mono.zip(Mono.just("a"), Mono.<String>empty().defaultIfEmpty("none"))
                        .map(t -> t.getT1() + "+" + t.getT2()))
                .expectNext("a+none")
                .verifyComplete();
    }

    @Test
    void zipWith_and_flux_zip_pair_elements_by_index() {
        StepVerifier.create(Flux.just("a", "b", "c").zipWith(Flux.range(1, 2), (s, i) -> s + i))
                .expectNext("a1", "b2")       // stops at the shorter source
                .verifyComplete();
    }

    @Test
    void merge_interleaves_while_concat_is_sequential() {
        StepVerifier.withVirtualTime(() -> Flux.merge(
                        Flux.just("a1", "a2").delayElements(Duration.ofMillis(100)),
                        Flux.just("b1", "b2").delayElements(Duration.ofMillis(150))))
                .thenAwait(Duration.ofSeconds(1))
                .expectNext("a1", "b1", "a2", "b2")   // a1@100 b1@150 a2@200 b2@300
                .verifyComplete();

        StepVerifier.withVirtualTime(() -> Flux.concat(
                        Flux.just("a1", "a2").delayElements(Duration.ofMillis(100)),
                        Flux.just("b1", "b2").delayElements(Duration.ofMillis(150))))
                .thenAwait(Duration.ofSeconds(1))
                .expectNext("a1", "a2", "b1", "b2")   // B is only subscribed after A completes
                .verifyComplete();
    }

    @Test
    void then_ignores_the_value_and_continues_after_completion() {
        List<String> log = new ArrayList<>();
        Mono<Void> saveOrder = Mono.fromRunnable(() -> log.add("saved"));
        Mono<Void> publishEvent = Mono.fromRunnable(() -> log.add("published"));

        // typical "save, then publish, then return 202" flow
        Mono<String> flow = saveOrder.then(publishEvent).then(Mono.just("ACCEPTED"));

        StepVerifier.create(flow).expectNext("ACCEPTED").verifyComplete();
        assertThat(log).containsExactly("saved", "published");
    }

    @Test
    void when_completes_once_all_sources_complete() {
        StepVerifier.withVirtualTime(() -> Mono.when(call("a", 100), call("b", 200)))
                .expectSubscription()
                .expectNoEvent(Duration.ofMillis(199))
                .thenAwait(Duration.ofMillis(1))
                .verifyComplete();
    }

    @Test
    void firstWithValue_races_publishers_and_cancels_the_losers() {
        StepVerifier.withVirtualTime(() -> Mono.firstWithValue(call("primary", 500), call("replica", 100)))
                .expectSubscription()
                .thenAwait(Duration.ofMillis(100))
                .expectNext("replica")
                .verifyComplete();
    }
}

package com.reactiveplatform.playground;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;
import reactor.test.scheduler.VirtualTimeScheduler;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * EXERCISE 7 - Hot vs cold publishers, and Sinks.
 * <pre>
 *   COLD  (default) - each subscriber triggers its own execution and gets the full sequence.
 *                     e.g. WebClient call, R2DBC query: subscribing twice = 2 HTTP calls / 2 queries!
 *   HOT             - one shared execution; subscribers see only what is emitted AFTER they join.
 *                     e.g. stock ticker, Kafka live feed, SSE broadcast.
 *
 *   share()               cold -> hot, starts with the first subscriber, stops when all leave
 *   publish().autoConnect(n)  starts when n subscribers have joined
 *   replay(n) / cache()   hot + late subscribers get the last n (or all) items
 *   Sinks.*               programmatically emit into a Flux/Mono (the reactive "Subject")
 * </pre>
 */
class Ex07HotVsColdTest {

    @Test
    void cold_publishers_re_execute_for_every_subscriber() {
        AtomicInteger executions = new AtomicInteger();
        Mono<String> httpCall = Mono.fromCallable(() -> "response-" + executions.incrementAndGet());

        StepVerifier.create(httpCall).expectNext("response-1").verifyComplete();
        StepVerifier.create(httpCall).expectNext("response-2").verifyComplete();
        assertThat(executions).hasValue(2);    // two subscriptions = two "HTTP calls"
    }

    @Test
    void cache_executes_once_and_replays_the_result() {
        AtomicInteger executions = new AtomicInteger();
        Mono<String> cached = Mono.fromCallable(() -> "config-" + executions.incrementAndGet()).cache();

        StepVerifier.create(cached).expectNext("config-1").verifyComplete();
        StepVerifier.create(cached).expectNext("config-1").verifyComplete();
        assertThat(executions).hasValue(1);
    }

    @Test
    void share_turns_a_cold_ticker_hot_so_late_subscribers_miss_earlier_ticks() {
        VirtualTimeScheduler clock = VirtualTimeScheduler.create();
        Flux<Long> ticker = Flux.interval(Duration.ofSeconds(1), clock).take(5).share();

        List<Long> early = new CopyOnWriteArrayList<>();
        List<Long> late = new CopyOnWriteArrayList<>();

        ticker.subscribe(early::add);
        clock.advanceTimeBy(Duration.ofSeconds(2));       // ticks 0 and 1 happen
        ticker.subscribe(late::add);
        clock.advanceTimeBy(Duration.ofSeconds(10));

        assertThat(early).containsExactly(0L, 1L, 2L, 3L, 4L);
        assertThat(late).containsExactly(2L, 3L, 4L);
    }

    @Test
    void publish_autoConnect_waits_for_n_subscribers_then_shares_one_upstream_subscription() {
        AtomicInteger upstreamSubscriptions = new AtomicInteger();
        Flux<Integer> shared = Flux.range(1, 3)
                .doOnSubscribe(s -> upstreamSubscriptions.incrementAndGet())
                .publish()
                .autoConnect(2);

        List<Integer> first = new CopyOnWriteArrayList<>();
        List<Integer> second = new CopyOnWriteArrayList<>();

        shared.subscribe(first::add);
        assertThat(first).isEmpty();                     // not connected yet
        shared.subscribe(second::add);                   // 2nd subscriber -> connect

        assertThat(first).containsExactly(1, 2, 3);
        assertThat(second).containsExactly(1, 2, 3);
        assertThat(upstreamSubscriptions).hasValue(1);
    }

    @Test
    void multicast_sink_broadcasts_to_current_subscribers_only() {
        Sinks.Many<String> sink = Sinks.many().multicast().onBackpressureBuffer();
        Flux<String> notifications = sink.asFlux();

        List<String> userA = new CopyOnWriteArrayList<>();
        List<String> userB = new CopyOnWriteArrayList<>();

        notifications.subscribe(userA::add);
        sink.tryEmitNext("order-1 CONFIRMED");
        notifications.subscribe(userB::add);
        sink.tryEmitNext("order-2 CONFIRMED");
        sink.tryEmitComplete();

        assertThat(userA).containsExactly("order-1 CONFIRMED", "order-2 CONFIRMED");
        assertThat(userB).containsExactly("order-2 CONFIRMED");
    }

    @Test
    void replay_sink_gives_late_subscribers_the_last_n_items() {
        Sinks.Many<String> sink = Sinks.many().replay().limit(2);
        sink.tryEmitNext("loc-1");
        sink.tryEmitNext("loc-2");
        sink.tryEmitNext("loc-3");

        StepVerifier.create(sink.asFlux().take(2))
                .expectNext("loc-2", "loc-3")            // a late joiner immediately sees recent positions
                .verifyComplete();
    }

    @Test
    void unicast_sink_allows_exactly_one_subscriber() {
        Sinks.Many<String> sink = Sinks.many().unicast().onBackpressureBuffer();
        sink.asFlux().subscribe();

        StepVerifier.create(sink.asFlux())
                .expectError(IllegalStateException.class)
                .verify();
    }

    @Test
    void sinks_one_completes_a_mono_from_outside_like_a_completable_future() {
        Sinks.One<String> reply = Sinks.one();

        StepVerifier.create(reply.asMono())
                .then(() -> reply.tryEmitValue("payment-approved"))     // e.g. correlated Kafka reply
                .expectNext("payment-approved")
                .verifyComplete();

        // emitting into a terminated sink is reported, not thrown
        assertThat(reply.tryEmitValue("again")).isEqualTo(Sinks.EmitResult.FAIL_TERMINATED);
    }
}

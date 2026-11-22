package com.reactiveplatform.playground;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * EXERCISE 6 - Backpressure.
 * <p>
 * Reactive Streams protocol: subscribe -> onSubscribe(Subscription) -> request(n) -> onNext (at most n) -> ...
 * The SUBSCRIBER controls the rate by signalling demand. That's backpressure.
 * <ul>
 *   <li>Pull-capable sources (range, fromIterable, R2DBC, Mongo cursor) simply produce on demand.</li>
 *   <li>Push sources that can't slow down (UI events, Flux.interval, sensors, a Kafka firehose)
 *       need an OVERFLOW STRATEGY: buffer (bounded!), drop, latest, or error.</li>
 * </ul>
 * subscribe() / block*() request Long.MAX_VALUE = "unbounded", i.e. no backpressure at all.
 */
class Ex06BackpressureTest {

    @Test
    void default_subscribers_request_an_unbounded_amount() {
        List<Long> requests = new CopyOnWriteArrayList<>();
        Flux.range(1, 3).doOnRequest(requests::add).blockLast();
        assertThat(requests).containsExactly(Long.MAX_VALUE);
    }

    @Test
    void a_custom_subscriber_controls_demand_one_item_at_a_time() {
        List<Long> requests = new CopyOnWriteArrayList<>();
        List<Integer> received = new CopyOnWriteArrayList<>();

        Flux.range(1, 5)
                .doOnRequest(requests::add)
                .subscribe(new BaseSubscriber<>() {
                    @Override
                    protected void hookOnSubscribe(Subscription subscription) {
                        request(1);                 // initial demand
                    }

                    @Override
                    protected void hookOnNext(Integer value) {
                        received.add(value);        // "process" it...
                        request(1);                 // ...then ask for the next one
                    }
                });

        assertThat(received).containsExactly(1, 2, 3, 4, 5);
        assertThat(requests).isNotEmpty().allMatch(n -> n == 1L);
    }

    @Test
    void stepVerifier_can_drive_demand_explicitly() {
        StepVerifier.create(Flux.range(1, 10), 0)       // initial request = 0
                .expectSubscription()
                .expectNoEvent(Duration.ofMillis(50))    // no demand -> nothing emitted
                .thenRequest(3)
                .expectNext(1, 2, 3)
                .thenRequest(2)
                .expectNext(4, 5)
                .thenCancel()
                .verify();
    }

    @Test
    void limitRate_turns_unbounded_demand_into_small_batches() {
        List<Long> requests = new CopyOnWriteArrayList<>();

        Flux.range(1, 100)
                .doOnRequest(requests::add)
                .limitRate(10)          // requests 10, then replenishes 75% (8) as items are consumed
                .blockLast();           // downstream asks for Long.MAX_VALUE...

        assertThat(requests.getFirst()).isEqualTo(10L);           // ...but upstream only ever sees small requests
        assertThat(requests).allMatch(n -> n <= 10L);
    }

    // ------------------------------------------------------------------- overflow strategies for push sources

    /** A source that pushes 100 items immediately and ignores demand (a "firehose"). */
    private static Flux<Integer> firehose() {
        return Flux.create(sink -> {
            for (int i = 1; i <= 100; i++) {
                sink.next(i);
            }
            sink.complete();
        }, FluxSink.OverflowStrategy.IGNORE);
    }

    /*
     * NOTE: the overflow tests use a hand-written SlowSubscriber instead of StepVerifier.
     * StepVerifier runs the next script step (e.g. thenRequest) synchronously, as soon as the previous
     * expectation is met - i.e. while the firehose is still inside its for-loop. That would add demand
     * mid-push and hide the overflow behaviour we want to observe.
     */

    @Test
    void onBackpressureBuffer_with_a_bound_signals_an_overflow_error() {
        SlowSubscriber<Integer> subscriber = new SlowSubscriber<>(0);
        firehose().onBackpressureBuffer(10).subscribe(subscriber);   // 100 pushed while demand = 0

        subscriber.request(Long.MAX_VALUE);                           // now drain whatever survived

        assertThat(subscriber.received).hasSize(10);                  // only what fitted in the buffer
        assertThat(Exceptions.isOverflow(subscriber.error)).isTrue(); // then the overflow error
    }

    @Test
    void onBackpressureDrop_discards_items_the_subscriber_has_no_demand_for() {
        List<Integer> dropped = new CopyOnWriteArrayList<>();
        SlowSubscriber<Integer> subscriber = new SlowSubscriber<>(3);

        firehose().onBackpressureDrop(dropped::add).subscribe(subscriber);

        assertThat(subscriber.received).containsExactly(1, 2, 3);
        assertThat(dropped).hasSize(97);
        assertThat(subscriber.completed).isTrue();                    // completion needs no demand
    }

    @Test
    void onBackpressureLatest_keeps_only_the_most_recent_item() {
        // Good for "current state" streams: stock price, driver location (Phase 6 tracking).
        SlowSubscriber<Integer> subscriber = new SlowSubscriber<>(3);
        firehose().onBackpressureLatest().subscribe(subscriber);

        assertThat(subscriber.received).containsExactly(1, 2, 3);
        assertThat(subscriber.completed).isFalse();                   // 100 is still pending

        subscriber.request(1);

        assertThat(subscriber.received).containsExactly(1, 2, 3, 100);   // 4..99 were overwritten
        assertThat(subscriber.completed).isTrue();
    }

    /** A subscriber that requests a fixed amount up front and then only on explicit request(n) calls. */
    static final class SlowSubscriber<T> extends BaseSubscriber<T> {
        final List<T> received = new CopyOnWriteArrayList<>();
        volatile Throwable error;
        volatile boolean completed;
        private final long initialDemand;

        SlowSubscriber(long initialDemand) {
            this.initialDemand = initialDemand;
        }

        @Override
        protected void hookOnSubscribe(Subscription subscription) {
            if (initialDemand > 0) {
                request(initialDemand);
            }
        }

        @Override
        protected void hookOnNext(T value) {
            received.add(value);
        }

        @Override
        protected void hookOnError(Throwable throwable) {
            error = throwable;
        }

        @Override
        protected void hookOnComplete() {
            completed = true;
        }
    }
}

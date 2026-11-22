package com.reactiveplatform.playground;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * EXERCISE 1 - Creating publishers.
 * <p>
 * Mono = 0..1 element, Flux = 0..N elements. Both are Publishers (Reactive Streams spec) and emit
 * onNext* followed by exactly one terminal signal: onComplete OR onError.
 * <p>
 * Interview favourites covered here:
 * <ul>
 *   <li>"Nothing happens until you subscribe" - assembly vs subscription time</li>
 *   <li>Mono.just (eager) vs Mono.defer / fromCallable / fromSupplier (lazy)</li>
 *   <li>Flux.generate (synchronous, pull, one item per round) vs Flux.create (push, bridges callback APIs)</li>
 * </ul>
 */
class Ex01CreatingPublishersTest {

    @Test
    void just_emits_the_given_values_then_completes() {
        StepVerifier.create(Flux.just("A", "B", "C"))
                .expectNext("A", "B", "C")
                .verifyComplete();
    }

    @Test
    void empty_and_error_are_also_valid_sequences() {
        StepVerifier.create(Mono.empty()).verifyComplete();          // no onNext, just onComplete

        StepVerifier.create(Mono.error(new IllegalStateException("boom")))
                .expectErrorMessage("boom")                            // no onNext, just onError
                .verify();
    }

    @Test
    void fromIterable_and_range() {
        StepVerifier.create(Flux.fromIterable(List.of(10, 20, 30))).expectNext(10, 20, 30).verifyComplete();
        StepVerifier.create(Flux.range(5, 3)).expectNext(5, 6, 7).verifyComplete();   // start, count
    }

    @Test
    void nothing_happens_until_subscribe() {
        AtomicInteger processed = new AtomicInteger();

        // ASSEMBLY time: we only describe the pipeline. No data flows.
        Flux<Integer> pipeline = Flux.range(1, 3).doOnNext(i -> processed.incrementAndGet());
        assertThat(processed).hasValue(0);

        // SUBSCRIPTION time: subscribe() triggers the flow from source to subscriber.
        pipeline.subscribe();
        assertThat(processed).hasValue(3);

        // Subscribing again replays the work: a plain Flux is COLD (see Ex07).
        pipeline.subscribe();
        assertThat(processed).hasValue(6);
    }

    @Test
    void just_is_eager_but_defer_is_lazy() {
        AtomicInteger dbCalls = new AtomicInteger();

        // Mono.just(x): x is computed NOW, while building the pipeline - even if nobody ever subscribes.
        Mono<String> eager = Mono.just(expensiveLookup(dbCalls));
        assertThat(dbCalls).hasValue(1);

        // Mono.defer: the lambda runs on EVERY subscription, never at assembly.
        Mono<String> lazy = Mono.defer(() -> Mono.just(expensiveLookup(dbCalls)));
        assertThat(dbCalls).hasValue(1);

        lazy.block();
        lazy.block();
        assertThat(dbCalls).hasValue(3);

        // fromSupplier / fromCallable are lazy too.
        Mono<String> supplied = Mono.fromSupplier(() -> expensiveLookup(dbCalls));
        assertThat(dbCalls).hasValue(3);
        StepVerifier.create(supplied).expectNext("value-4").verifyComplete();
        assertThat(eager.block()).isEqualTo("value-1");
    }

    @Test
    void fromCallable_turns_a_thrown_checked_exception_into_onError() {
        // Callable may throw checked exceptions - ideal for wrapping legacy/JDBC code (Phase 7).
        Mono<String> readFile = Mono.fromCallable(() -> {
            throw new IOException("disk not found");
        });

        StepVerifier.create(readFile)
                .expectError(IOException.class)
                .verify();
    }

    @Test
    void generate_is_synchronous_stateful_and_pull_based() {
        // One sink.next() per invocation; Reactor calls the generator again only when there is demand.
        Flux<Integer> fibonacci = Flux.generate(
                () -> new int[]{0, 1},
                (state, sink) -> {
                    sink.next(state[0]);
                    return new int[]{state[1], state[0] + state[1]};
                });

        StepVerifier.create(fibonacci.take(7))       // infinite source, take() bounds it
                .expectNext(0, 1, 1, 2, 3, 5, 8)
                .verifyComplete();
    }

    @Test
    void create_bridges_a_callback_based_listener_api() {
        PriceFeed feed = new PriceFeed();
        AtomicBoolean unregistered = new AtomicBoolean();

        // Flux.create: push-based, can be called from any thread, many next() per callback.
        Flux<String> prices = Flux.create(sink -> {
            Consumer<String> listener = sink::next;
            feed.register(listener, sink::complete);
            sink.onDispose(() -> {                    // cleanup on cancel / complete / error
                feed.unregister(listener);
                unregistered.set(true);
            });
        });

        StepVerifier.create(prices)
                .then(() -> feed.publish("AAPL=190", "MSFT=420"))
                .expectNext("AAPL=190", "MSFT=420")
                .then(feed::close)
                .verifyComplete();

        assertThat(unregistered).isTrue();
    }

    @Test
    void converting_between_mono_and_flux() {
        StepVerifier.create(Flux.just(1, 2, 3).next()).expectNext(1).verifyComplete();                 // first element
        StepVerifier.create(Flux.just(1, 2, 3).collectList()).expectNext(List.of(1, 2, 3)).verifyComplete();
        StepVerifier.create(Mono.just(9).flux()).expectNext(9).verifyComplete();
        StepVerifier.create(Flux.just(1, 2, 3).count()).expectNext(3L).verifyComplete();
    }

    // ----------------------------------------------------------------------------------- helpers

    private static String expensiveLookup(AtomicInteger counter) {
        return "value-" + counter.incrementAndGet();
    }

    /** A typical non-reactive, listener-style API (think JMS listener, WebSocket client, sensor SDK). */
    static final class PriceFeed {
        private final List<Consumer<String>> listeners = new ArrayList<>();
        private final List<Runnable> closeHandlers = new ArrayList<>();

        void register(Consumer<String> listener, Runnable onClose) {
            listeners.add(listener);
            closeHandlers.add(onClose);
        }

        void unregister(Consumer<String> listener) {
            listeners.remove(listener);
        }

        void publish(String... prices) {
            for (String p : prices) {
                List.copyOf(listeners).forEach(l -> l.accept(p));
            }
        }

        void close() {
            List.copyOf(closeHandlers).forEach(Runnable::run);
        }
    }
}

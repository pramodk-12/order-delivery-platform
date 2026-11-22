package com.reactiveplatform.playground;

import org.junit.jupiter.api.Test;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.test.StepVerifier;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * EXERCISE 4 - Error handling, retry and timeout.
 * <p>
 * Key rule: an error is a TERMINAL signal. After onError the sequence is over; operators like
 * onErrorResume replace the rest of the sequence, they don't "continue" the original one.
 * <pre>
 *   onErrorReturn   static fallback value          (like catch + return)
 *   onErrorResume   fallback publisher, by type    (like catch + call another service)
 *   onErrorMap      translate exception type       (like catch + throw new DomainException(e))
 *   doOnError       side effect (log), error still propagates
 *   retry/retryWhen RE-SUBSCRIBE to the source     (only meaningful for cold, lazy sources)
 *   timeout         error (or fallback) if no signal in time
 * </pre>
 */
class Ex04ErrorHandlingTest {

    private static Flux<Integer> failsOnThree() {
        return Flux.range(1, 5).map(i -> {
            if (i == 3) {
                throw new IllegalArgumentException("bad item " + i);
            }
            return i;
        });
    }

    @Test
    void an_error_terminates_the_sequence() {
        StepVerifier.create(failsOnThree())
                .expectNext(1, 2)
                .expectErrorMessage("bad item 3")     // 4 and 5 are never emitted
                .verify();
    }

    @Test
    void onErrorReturn_completes_with_a_fallback_value() {
        StepVerifier.create(failsOnThree().onErrorReturn(-1))
                .expectNext(1, 2, -1)
                .verifyComplete();
    }

    @Test
    void onErrorResume_switches_to_a_fallback_publisher_by_exception_type() {
        Mono<String> price = Mono.<String>error(new TimeoutException("pricing service slow"))
                .onErrorResume(TimeoutException.class, e -> Mono.just("cached-price"))
                .onErrorResume(IllegalStateException.class, e -> Mono.just("never-used"));

        StepVerifier.create(price).expectNext("cached-price").verifyComplete();
    }

    @Test
    void onErrorMap_translates_technical_errors_into_domain_errors() {
        Mono<String> customer = Mono.<String>error(new IllegalStateException("connection reset"))
                .onErrorMap(e -> new CustomerLookupException("customer 42 unavailable", e));

        StepVerifier.create(customer)
                .expectErrorSatisfies(e -> assertThat(e)
                        .isInstanceOf(CustomerLookupException.class)
                        .hasMessage("customer 42 unavailable")
                        .hasCauseInstanceOf(IllegalStateException.class))
                .verify();
    }

    @Test
    void doOnError_is_only_a_side_effect() {
        List<String> logged = new ArrayList<>();
        StepVerifier.create(failsOnThree().doOnError(e -> logged.add(e.getMessage())))
                .expectNext(1, 2)
                .expectError(IllegalArgumentException.class)
                .verify();
        assertThat(logged).containsExactly("bad item 3");
    }

    @Test
    void onErrorContinue_drops_the_bad_element_and_keeps_going() {
        // Works only with operators that support it (map, filter, flatMap...). Use sparingly:
        // prefer handling the error inside flatMap with onErrorResume for each element.
        List<Object> skipped = new ArrayList<>();
        StepVerifier.create(failsOnThree().onErrorContinue((e, item) -> skipped.add(item)))
                .expectNext(1, 2, 4, 5)
                .verifyComplete();
        assertThat(skipped).containsExactly(3);
    }

    @Test
    void per_element_error_handling_inside_flatMap_is_the_idiomatic_alternative() {
        Flux<String> results = Flux.just(1, 2, 3)
                .flatMap(id -> callInventory(id)
                        .onErrorResume(e -> Mono.just("item-" + id + ":UNKNOWN")));

        StepVerifier.create(results)
                .expectNext("item-1:OK", "item-2:UNKNOWN", "item-3:OK")
                .verifyComplete();
    }

    @Test
    void retry_resubscribes_to_a_cold_source() {
        AtomicInteger attempts = new AtomicInteger();
        Mono<String> flaky = Mono.fromCallable(() -> {
            if (attempts.incrementAndGet() < 3) {
                throw new IllegalStateException("try again");
            }
            return "ok on attempt " + attempts.get();
        });

        StepVerifier.create(flaky.retry(3)).expectNext("ok on attempt 3").verifyComplete();

        // GOTCHA: Mono.just(compute()) would NOT be re-computed by retry - the value was captured at assembly.
    }

    @Test
    void retryWhen_backoff_waits_exponentially_between_attempts() {
        StepVerifier.withVirtualTime(() -> failNTimesThenSucceed(2)
                        .retryWhen(Retry.backoff(3, Duration.ofSeconds(1)).jitter(0)))   // delays: 1s, 2s, 4s...
                .expectSubscription()
                .expectNoEvent(Duration.ofMillis(2999))      // attempt1 fails @0, retry @1s fails, retry @3s succeeds
                .thenAwait(Duration.ofMillis(1))
                .expectNext("ok")
                .verifyComplete();
    }

    @Test
    void exhausted_retries_surface_a_RetryExhaustedException_wrapping_the_last_error() {
        StepVerifier.withVirtualTime(() -> Mono.error(new IllegalStateException("inventory down"))
                        .retryWhen(Retry.backoff(2, Duration.ofMillis(100)).jitter(0)))
                .thenAwait(Duration.ofSeconds(1))
                .expectErrorSatisfies(e -> {
                    assertThat(Exceptions.isRetryExhausted(e)).isTrue();
                    assertThat(e.getCause()).hasMessage("inventory down");
                })
                .verify();
    }

    @Test
    void timeout_errors_or_falls_back_when_the_source_is_too_slow() {
        StepVerifier.withVirtualTime(() -> Mono.just("slow").delayElement(Duration.ofSeconds(5))
                        .timeout(Duration.ofSeconds(1)))
                .expectSubscription()
                .thenAwait(Duration.ofSeconds(1))
                .expectError(TimeoutException.class)
                .verify();

        StepVerifier.withVirtualTime(() -> Mono.just("slow").delayElement(Duration.ofSeconds(5))
                        .timeout(Duration.ofSeconds(1), Mono.just("fallback")))
                .expectSubscription()
                .thenAwait(Duration.ofSeconds(1))
                .expectNext("fallback")
                .verifyComplete();
    }

    @Test
    void doFinally_runs_after_any_terminal_signal_or_cancel() {
        AtomicReference<SignalType> signal = new AtomicReference<>();

        StepVerifier.create(failsOnThree().doFinally(signal::set)).expectNext(1, 2).expectError().verify();
        assertThat(signal).hasValue(SignalType.ON_ERROR);

        StepVerifier.create(Flux.range(1, 100).doFinally(signal::set).take(2)).expectNext(1, 2).verifyComplete();
        assertThat(signal).hasValue(SignalType.CANCEL);   // take(2) cancels upstream
    }

    // ----------------------------------------------------------------------------------- helpers

    private static Mono<String> callInventory(int id) {
        return id == 2 ? Mono.error(new IllegalStateException("timeout")) : Mono.just("item-" + id + ":OK");
    }

    private static Mono<String> failNTimesThenSucceed(int failures) {
        AtomicInteger attempts = new AtomicInteger();
        return Mono.defer(() -> attempts.incrementAndGet() <= failures
                ? Mono.error(new IllegalStateException("attempt " + attempts.get() + " failed"))
                : Mono.just("ok"));
    }

    static final class CustomerLookupException extends RuntimeException {
        CustomerLookupException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}

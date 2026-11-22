package com.reactiveplatform.playground;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.test.publisher.PublisherProbe;
import reactor.test.publisher.TestPublisher;

import java.time.Duration;
import java.util.ArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * EXERCISE 9 - Testing reactive code (reactor-test).
 * <ul>
 *   <li>{@link StepVerifier} - subscribe and assert signals step by step. Always end with verify*().</li>
 *   <li>Virtual time - test hours of delays/intervals in milliseconds.</li>
 *   <li>{@link TestPublisher} - a source you drive manually, to test YOUR operator/pipeline in isolation.</li>
 *   <li>{@link PublisherProbe} - prove which branch (e.g. switchIfEmpty fallback) was actually subscribed.</li>
 * </ul>
 * Never use Thread.sleep or block() in reactive tests when StepVerifier can express it.
 */
class Ex09TestingToolsTest {

    @Test
    void virtual_time_runs_a_three_hour_schedule_instantly() {
        StepVerifier.withVirtualTime(() -> Flux.interval(Duration.ofHours(1)).take(3))
                .expectSubscription()
                .expectNoEvent(Duration.ofHours(1))
                .expectNext(0L)
                .thenAwait(Duration.ofHours(2))
                .expectNext(1L, 2L)
                .verifyComplete();
    }

    @Test
    void assertNext_and_recordWith_for_rich_assertions() {
        record Order(String id, int amount) {
        }

        StepVerifier.create(Flux.just(new Order("o-1", 50), new Order("o-2", 150)))
                .assertNext(o -> assertThat(o.id()).isEqualTo("o-1"))
                .consumeNextWith(o -> assertThat(o.amount()).isGreaterThan(100))
                .verifyComplete();

        StepVerifier.create(Flux.range(1, 50))
                .recordWith(ArrayList::new)
                .expectNextCount(50)
                .consumeRecordedWith(all -> assertThat(all).doesNotContainNull().hasSize(50))
                .verifyComplete();
    }

    @Test
    void testPublisher_lets_you_drive_the_source_by_hand() {
        TestPublisher<String> source = TestPublisher.create();
        Flux<String> pipelineUnderTest = source.flux().map(String::toUpperCase).filter(s -> !s.isBlank());

        StepVerifier.create(pipelineUnderTest)
                .then(() -> source.next("a", " ", "b"))
                .expectNext("A", "B")
                .then(() -> source.error(new IllegalStateException("upstream died")))
                .expectErrorMessage("upstream died")
                .verify();

        source.assertWasSubscribed();
        source.assertNoSubscribers();   // subscription is gone after the error
    }

    @Test
    void publisherProbe_proves_the_fallback_branch_was_used() {
        PublisherProbe<String> dbFallback = PublisherProbe.of(Mono.just("from-db"));

        Mono<String> cacheMiss = Mono.<String>empty().switchIfEmpty(dbFallback.mono());
        StepVerifier.create(cacheMiss).expectNext("from-db").verifyComplete();
        dbFallback.assertWasSubscribed();

        PublisherProbe<String> unusedFallback = PublisherProbe.of(Mono.just("from-db"));
        Mono<String> cacheHit = Mono.just("from-cache").switchIfEmpty(unusedFallback.mono());
        StepVerifier.create(cacheHit).expectNext("from-cache").verifyComplete();
        unusedFallback.assertWasNotSubscribed();
    }

    @Test
    void verify_with_a_timeout_fails_fast_instead_of_hanging() {
        Duration took = StepVerifier.create(Mono.just("fast").delayElement(Duration.ofMillis(20)))
                .expectNext("fast")
                .expectComplete()
                .verify(Duration.ofSeconds(2));     // fails the test if not done in time

        assertThat(took).isLessThan(Duration.ofSeconds(2));
    }
}

package com.reactiveplatform.playground;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.util.context.Context;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * EXERCISE 8 - Reactor Context: the reactive replacement for ThreadLocal.
 * <p>
 * Problem: in reactive code one request hops between threads, so ThreadLocal (and therefore SLF4J MDC,
 * Spring Security's SecurityContextHolder, @Transactional's TransactionSynchronizationManager) breaks.
 * Solution: Context - an immutable map attached to the SUBSCRIPTION, not to a thread.
 * <ul>
 *   <li>Written with {@code contextWrite} (usually at the bottom, e.g. by a WebFilter)</li>
 *   <li>Read with {@code Mono.deferContextual} / {@code transformDeferredContextual}</li>
 *   <li>Propagates from the subscriber UPWARDS: only operators ABOVE a contextWrite can see it</li>
 * </ul>
 * Phase 4 uses this for the correlation id; Spring Security WebFlux uses it for the authenticated user.
 */
class Ex08ContextTest {

    private static final String CORRELATION_ID = "correlationId";

    private static Mono<String> readCorrelationId() {
        return Mono.deferContextual(ctx -> Mono.just(ctx.getOrDefault(CORRELATION_ID, "missing")));
    }

    @Test
    void context_written_at_the_bottom_is_readable_above() {
        StepVerifier.create(readCorrelationId().contextWrite(Context.of(CORRELATION_ID, "req-123")))
                .expectNext("req-123")
                .verifyComplete();
    }

    @Test
    void operators_below_a_contextWrite_cannot_see_it() {
        Mono<String> visible = Mono.just("x")
                .flatMap(x -> readCorrelationId())
                .contextWrite(Context.of(CORRELATION_ID, "req-1"));     // below the reader -> visible

        Mono<String> invisible = Mono.just("x")
                .contextWrite(Context.of(CORRELATION_ID, "req-1"))      // above the reader -> not visible
                .flatMap(x -> readCorrelationId());

        StepVerifier.create(visible).expectNext("req-1").verifyComplete();
        StepVerifier.create(invisible).expectNext("missing").verifyComplete();
    }

    @Test
    void the_contextWrite_closest_to_the_reader_wins() {
        Mono<String> pipeline = readCorrelationId()
                .contextWrite(Context.of(CORRELATION_ID, "inner"))
                .contextWrite(Context.of(CORRELATION_ID, "outer"));

        StepVerifier.create(pipeline).expectNext("inner").verifyComplete();
    }

    @Test
    void context_survives_thread_hops_but_threadlocal_does_not() {
        ThreadLocal<String> threadLocal = ThreadLocal.withInitial(() -> "missing");
        threadLocal.set("req-777");
        try {
            String fromThreadLocal = Mono.just(1)
                    .publishOn(Schedulers.parallel())               // hop to another thread
                    .map(i -> threadLocal.get())
                    .block();
            assertThat(fromThreadLocal).isEqualTo("missing");   // lost -> this is why MDC logging breaks

            Mono<String> fromContext = Mono.just(1)
                    .delayElement(Duration.ofMillis(10))            // hops to parallel scheduler
                    .publishOn(Schedulers.boundedElastic())         // and hops again
                    .flatMap(i -> readCorrelationId())
                    .contextWrite(Context.of(CORRELATION_ID, "req-777"));
            StepVerifier.create(fromContext).expectNext("req-777").verifyComplete();
        } finally {
            threadLocal.remove();
        }
    }
}

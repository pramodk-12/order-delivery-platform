package com.reactiveplatform.playground;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * EXERCISE 5 - Schedulers (threading).
 * <p>
 * Reactor is concurrency-AGNOSTIC: by default everything runs on the thread that calls subscribe().
 * You opt into other threads with:
 * <pre>
 *   subscribeOn(S)  - changes where the SOURCE is subscribed (and everything until the next publishOn).
 *                     Position in the chain doesn't matter; if there are several, the one closest to the source wins.
 *   publishOn(S)    - changes the thread for operators BELOW it (downstream). Position matters.
 *
 *   Schedulers.parallel()       fixed pool, size = CPU cores, for CPU-bound work. NEVER block here.
 *   Schedulers.boundedElastic() growing pool (10 x cores, capped), for BLOCKING calls (JDBC, file IO, legacy SDKs).
 *   Schedulers.single()         one reusable thread.
 *   Schedulers.immediate()      current thread (no-op).
 * </pre>
 * In WebFlux the request already runs on a Netty event-loop thread (reactor-http-nio-N): treat those like parallel().
 */
class Ex05SchedulersTest {

    private static String thread() {
        return Thread.currentThread().getName();
    }

    @Test
    void without_a_scheduler_everything_runs_on_the_subscribing_thread() {
        AtomicReference<String> sourceThread = new AtomicReference<>();
        AtomicReference<String> mapThread = new AtomicReference<>();

        Flux.range(1, 3)
                .doOnNext(i -> sourceThread.set(thread()))
                .map(i -> {
                    mapThread.set(thread());
                    return i;
                })
                .blockLast();

        assertThat(sourceThread).hasValue(thread());   // the JUnit "main" thread
        assertThat(mapThread).hasValue(thread());
    }

    @Test
    void subscribeOn_moves_the_whole_chain_starting_from_the_source() {
        AtomicReference<String> sourceThread = new AtomicReference<>();
        AtomicReference<String> downstreamThread = new AtomicReference<>();

        Flux.range(1, 1)
                .doOnNext(i -> sourceThread.set(thread()))
                .map(i -> i * 2)
                .doOnNext(i -> downstreamThread.set(thread()))
                .subscribeOn(Schedulers.boundedElastic())    // written at the bottom, affects the top
                .blockLast();

        assertThat(sourceThread.get()).startsWith("boundedElastic-");
        assertThat(downstreamThread.get()).startsWith("boundedElastic-");
    }

    @Test
    void publishOn_switches_only_the_operators_below_it() {
        AtomicReference<String> beforeThread = new AtomicReference<>();
        AtomicReference<String> afterThread = new AtomicReference<>();

        Flux.range(1, 1)
                .doOnNext(i -> beforeThread.set(thread()))
                .publishOn(Schedulers.parallel())
                .doOnNext(i -> afterThread.set(thread()))
                .blockLast();

        assertThat(beforeThread).hasValue(thread());
        assertThat(afterThread.get()).startsWith("parallel-");
    }

    @Test
    void with_multiple_subscribeOn_the_one_closest_to_the_source_wins() {
        AtomicReference<String> sourceThread = new AtomicReference<>();

        Flux.range(1, 1)
                .doOnNext(i -> sourceThread.set(thread()))
                .subscribeOn(Schedulers.boundedElastic())   // closest to source -> wins
                .subscribeOn(Schedulers.parallel())
                .blockLast();

        assertThat(sourceThread.get()).startsWith("boundedElastic-");
    }

    @Test
    void typical_pattern_blocking_source_on_boundedElastic_then_cpu_work_on_parallel() {
        AtomicReference<String> ioThread = new AtomicReference<>();
        AtomicReference<String> cpuThread = new AtomicReference<>();

        Mono.fromCallable(() -> {
                    ioThread.set(thread());           // e.g. JDBC call
                    return "row";
                })
                .subscribeOn(Schedulers.boundedElastic())
                .publishOn(Schedulers.parallel())
                .map(row -> {
                    cpuThread.set(thread());          // e.g. mapping / hashing
                    return row.toUpperCase();
                })
                .block();

        assertThat(ioThread.get()).startsWith("boundedElastic-");
        assertThat(cpuThread.get()).startsWith("parallel-");
    }

    @Test
    void parallel_runOn_splits_work_into_rails_on_the_parallel_scheduler() {
        Set<String> threads = ConcurrentHashMap.newKeySet();

        List<Integer> squares = Flux.range(1, 100)
                .parallel(4)
                .runOn(Schedulers.parallel())
                .map(i -> {
                    threads.add(thread());
                    return i * i;
                })
                .sequential()
                .collectList()
                .block();

        assertThat(squares).hasSize(100);
        assertThat(threads).isNotEmpty().hasSizeLessThanOrEqualTo(4).allMatch(t -> t.startsWith("parallel-"));
    }

    @Test
    void flatMap_plus_subscribeOn_gives_real_concurrency_for_blocking_calls() {
        long start = System.nanoTime();
        Flux.range(1, 5)
                .flatMap(i -> Mono.fromCallable(() -> blockingCall(i)).subscribeOn(Schedulers.boundedElastic()))
                .blockLast();
        Duration concurrent = Duration.ofNanos(System.nanoTime() - start);

        start = System.nanoTime();
        Flux.range(1, 5)
                .concatMap(i -> Mono.fromCallable(() -> blockingCall(i)).subscribeOn(Schedulers.boundedElastic()))
                .blockLast();
        Duration sequential = Duration.ofNanos(System.nanoTime() - start);

        // 5 x 200ms: flatMap ~200ms (5 threads in parallel), concatMap ~1000ms (one after another)
        assertThat(concurrent).isLessThan(Duration.ofMillis(700));
        assertThat(sequential).isGreaterThanOrEqualTo(Duration.ofMillis(1000));
    }

    private static int blockingCall(int i) throws InterruptedException {
        Thread.sleep(200);   // stands in for a JDBC / legacy SOAP call
        return i;
    }
}

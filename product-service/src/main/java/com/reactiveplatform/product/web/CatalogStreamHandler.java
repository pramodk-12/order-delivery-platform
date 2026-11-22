package com.reactiveplatform.product.web;

import com.reactiveplatform.common.web.CorrelationId;
import com.reactiveplatform.common.web.ErrorResponse;
import com.reactiveplatform.product.dto.PriceUpdate;
import com.reactiveplatform.product.dto.ProductResponse;
import com.reactiveplatform.product.service.ProductService;
import com.reactiveplatform.product.streaming.PriceChangeStream;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

/**
 * STYLE 2 - functional endpoint handlers (wired to URLs in {@link CatalogRouter}).
 * <p>
 * A handler is just a function {@code ServerRequest -> Mono<ServerResponse>}: no annotations, no reflection,
 * easy to unit test, and routing is plain code you can read top to bottom.
 */
@Component
public class CatalogStreamHandler {

    private static final long MAX_DELAY_MS = 1_000;

    private final ProductService productService;
    private final PriceChangeStream priceChangeStream;
    private final Duration heartbeatInterval;

    public CatalogStreamHandler(ProductService productService, PriceChangeStream priceChangeStream,
                                @Value("${catalog.sse.heartbeat:15s}") Duration heartbeatInterval) {
        this.productService = productService;
        this.priceChangeStream = priceChangeStream;
        this.heartbeatInterval = heartbeatInterval;
    }

    /**
     * GET /api/catalog/stream - the whole catalog as NDJSON (one JSON object per line).
     * <p>
     * vs. returning a JSON array: the client starts receiving products immediately, and the server never
     * holds the full list in memory. If the client reads slowly, TCP backpressure travels all the way
     * back to the MongoDB cursor. {@code ?delayMs=50} slows it down so you can watch it arrive.
     */
    public Mono<ServerResponse> streamProducts(ServerRequest request) {
        // Parse inside the pipeline: a bad value becomes an error SIGNAL (handled by the router),
        // instead of an exception thrown before any Mono exists.
        return Mono.fromCallable(() -> parseDelay(request))
                .flatMap(delay -> {
                    Flux<ProductResponse> products = productService.streamAll();
                    if (!delay.isZero()) {
                        products = products.delayElements(delay);
                    }
                    return ServerResponse.ok()
                            .contentType(MediaType.APPLICATION_NDJSON)
                            .body(products, ProductResponse.class);
                });
    }

    /**
     * GET /api/catalog/price-updates - Server-Sent Events, one event per price change.
     * <p>
     * Heartbeat comments every {@code catalog.sse.heartbeat} keep proxies/load balancers from closing an
     * idle connection; the first one is sent immediately so the client sees the stream open right away.
     */
    public Mono<ServerResponse> priceUpdates(ServerRequest request) {
        Flux<ServerSentEvent<PriceUpdate>> priceEvents = priceChangeStream.updates()
                .map(update -> ServerSentEvent.builder(update)
                        .id(update.productId() + "-" + update.changedAt().toEpochMilli())
                        .event("price-update")
                        .build());

        Flux<ServerSentEvent<PriceUpdate>> heartbeats = Flux.interval(Duration.ZERO, heartbeatInterval)
                .map(tick -> ServerSentEvent.<PriceUpdate>builder().comment("keep-alive").build());

        return ServerResponse.ok()
                .contentType(MediaType.TEXT_EVENT_STREAM)
                .body(BodyInserters.fromServerSentEvents(Flux.merge(priceEvents, heartbeats)));
    }

    /** Used by the router's onError for IllegalArgumentException (incl. NumberFormatException). */
    public Mono<ServerResponse> badRequest(Throwable error, ServerRequest request) {
        ErrorResponse body = ErrorResponse.of(400, "Bad Request", error.getMessage(), request.path(),
                request.headers().firstHeader(CorrelationId.HEADER));
        return ServerResponse.badRequest().contentType(MediaType.APPLICATION_JSON).bodyValue(body);
    }

    private static Duration parseDelay(ServerRequest request) {
        long delayMs = request.queryParam("delayMs").map(Long::parseLong).orElse(0L);
        if (delayMs < 0 || delayMs > MAX_DELAY_MS) {
            throw new IllegalArgumentException("delayMs must be between 0 and " + MAX_DELAY_MS);
        }
        return Duration.ofMillis(delayMs);
    }
}

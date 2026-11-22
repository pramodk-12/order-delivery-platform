package com.reactiveplatform.product.web;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerResponse;

/**
 * STYLE 2 - functional routing: URL -> handler function, declared as plain code.
 * <p>
 * Split of responsibilities in this service:
 * <ul>
 *   <li>/api/products/**  - annotated {@link ProductController} (CRUD, request/response)</li>
 *   <li>/api/catalog/**   - this router (long-lived STREAMING responses)</li>
 * </ul>
 * Error handling lives here too ({@code onError}), because @RestControllerAdvice only covers
 * annotated controllers.
 */
@Configuration
public class CatalogRouter {

    @Bean
    RouterFunction<ServerResponse> catalogRoutes(CatalogStreamHandler handler) {
        return RouterFunctions.route()
                .path("/api/catalog", builder -> builder
                        .GET("/stream", handler::streamProducts)
                        .GET("/price-updates", handler::priceUpdates))
                .onError(IllegalArgumentException.class, handler::badRequest)
                .build();
    }
}

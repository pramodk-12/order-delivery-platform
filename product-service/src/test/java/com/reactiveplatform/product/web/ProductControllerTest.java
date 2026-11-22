package com.reactiveplatform.product.web;

import com.reactiveplatform.product.dto.ProductResponse;
import com.reactiveplatform.product.service.DuplicateSkuException;
import com.reactiveplatform.product.service.ProductNotFoundException;
import com.reactiveplatform.product.service.ProductService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webflux.test.autoconfigure.WebFluxTest;
import org.springframework.boot.webtestclient.autoconfigure.AutoConfigureWebTestClient;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * SLICE test: only the web layer (controller + @RestControllerAdvice + validation + JSON) is started.
 * The service is a Mockito mock, so no MongoDB is needed and tests run in milliseconds.
 * Use it to test HTTP concerns: status codes, headers, validation, error format.
 */
@WebFluxTest(controllers = ProductController.class)
@AutoConfigureWebTestClient
class ProductControllerTest {

    @Autowired
    WebTestClient client;

    @MockitoBean
    ProductService service;

    private static ProductResponse product(String id) {
        Instant at = Instant.parse("2026-01-01T10:00:00Z");
        return new ProductResponse(id, "SKU-1", "Clean Code", "A book", "books", new BigDecimal("12.50"), at, at);
    }

    private static Map<String, Object> validBody() {
        return Map.of("sku", "SKU-1", "name", "Clean Code", "category", "books", "price", 12.50);
    }

    @Test
    void create_returns_201_with_a_location_header() {
        when(service.create(any())).thenReturn(Mono.just(product("p-1")));

        client.post().uri("/api/products")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(validBody())
                .exchange()
                .expectStatus().isCreated()
                .expectHeader().location("/api/products/p-1")
                .expectBody()
                .jsonPath("$.id").isEqualTo("p-1")
                .jsonPath("$.price").isEqualTo(12.50);
    }

    @Test
    void invalid_body_returns_400_with_one_violation_per_field_and_never_reaches_the_service() {
        client.post().uri("/api/products")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(Map.of("sku", "", "name", "Clean Code", "category", "books", "price", -5))
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody()
                .jsonPath("$.message").isEqualTo("Validation failed")
                .jsonPath("$.violations.length()").isEqualTo(2)
                .jsonPath("$.violations[?(@.field == 'sku')]").exists()
                .jsonPath("$.violations[?(@.field == 'price')]").exists();

        verifyNoInteractions(service);
    }

    @Test
    void unknown_product_returns_404_with_the_standard_error_body() {
        when(service.findById("missing")).thenReturn(Mono.error(new ProductNotFoundException("missing")));

        client.get().uri("/api/products/missing")
                .header("X-Correlation-Id", "req-42")
                .exchange()
                .expectStatus().isNotFound()
                .expectBody()
                .jsonPath("$.status").isEqualTo(404)
                .jsonPath("$.message").isEqualTo("Product missing not found")
                .jsonPath("$.path").isEqualTo("/api/products/missing")
                .jsonPath("$.correlationId").isEqualTo("req-42");
    }

    @Test
    void duplicate_sku_returns_409() {
        when(service.create(any())).thenReturn(Mono.error(new DuplicateSkuException("SKU-1")));

        client.post().uri("/api/products")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(validBody())
                .exchange()
                .expectStatus().isEqualTo(409)
                .expectBody()
                .jsonPath("$.message").isEqualTo("A product with SKU 'SKU-1' already exists");
    }

    @Test
    void page_size_above_100_is_rejected_by_method_validation() {
        client.get().uri("/api/products?size=500")
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody()
                .jsonPath("$.violations[0].field").isEqualTo("size");

        verifyNoInteractions(service);
    }

    @Test
    void malformed_json_returns_400() {
        client.post().uri("/api/products")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{ not json")
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody()
                .jsonPath("$.status").isEqualTo(400);
    }
}

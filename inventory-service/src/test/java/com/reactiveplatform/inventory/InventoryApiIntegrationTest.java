package com.reactiveplatform.inventory;

import com.reactiveplatform.inventory.dto.ReservationLine;
import com.reactiveplatform.inventory.dto.ReservationRequest;
import com.reactiveplatform.inventory.dto.StockLevelResponse;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Behaviour of the HTTP API against a real MySQL: transactions, idempotency, state transitions.
 */
class InventoryApiIntegrationTest extends AbstractInventoryIntegrationTest {

    @Autowired
    WebTestClient client;

    @Test
    void restock_creates_then_updates_a_stock_level() {
        setStock("SKU-A", 10).expectStatus().isOk()
                .expectBody().jsonPath("$.available").isEqualTo(10).jsonPath("$.version").isEqualTo(0);
        setStock("SKU-A", 25).expectStatus().isOk()
                .expectBody().jsonPath("$.available").isEqualTo(25).jsonPath("$.version").isEqualTo(1);

        client.get().uri("/api/inventory?skus=SKU-A,SKU-UNKNOWN")
                .exchange()
                .expectStatus().isOk()
                .expectBody().jsonPath("$.length()").isEqualTo(1);
    }

    @Test
    void reservation_moves_stock_from_available_to_reserved() {
        givenStock("SKU-A", 10);
        givenStock("SKU-B", 5);

        reserve("order-1", line("SKU-A", 3), line("SKU-B", 2))
                .expectStatus().isCreated()
                .expectHeader().location("/api/inventory/reservations/order-1")
                .expectBody()
                .jsonPath("$.status").isEqualTo("RESERVED")
                .jsonPath("$.items.length()").isEqualTo(2);

        assertStock("SKU-A", 7, 3);
        assertStock("SKU-B", 3, 2);
    }

    @Test
    void sending_the_same_order_twice_reserves_only_once() {
        givenStock("SKU-A", 10);

        reserve("order-1", line("SKU-A", 3)).expectStatus().isCreated();
        reserve("order-1", line("SKU-A", 3)).expectStatus().isOk();     // replay -> 200, not 201

        assertStock("SKU-A", 7, 3);
    }

    @Test
    void reservation_is_all_or_nothing() {
        givenStock("SKU-A", 10);
        givenStock("SKU-B", 1);

        // SKU-A is reserved first (lines are sorted), then SKU-B fails -> the transaction rolls BOTH back
        reserve("order-2", line("SKU-A", 5), line("SKU-B", 2))
                .expectStatus().isEqualTo(409)
                .expectBody().jsonPath("$.message").isEqualTo("Insufficient stock for SKU-B: requested 2, available 1");

        assertStock("SKU-A", 10, 0);
        client.get().uri("/api/inventory/reservations/order-2").exchange().expectStatus().isNotFound();
    }

    @Test
    void unknown_sku_returns_404_and_nothing_is_reserved() {
        givenStock("SKU-A", 10);

        reserve("order-3", line("SKU-A", 1), line("ZZZ-NOPE", 1)).expectStatus().isNotFound();

        assertStock("SKU-A", 10, 0);
    }

    @Test
    void duplicate_skus_in_one_request_are_merged() {
        givenStock("SKU-A", 10);

        reserve("order-4", line("SKU-A", 2), line("SKU-A", 3))
                .expectStatus().isCreated()
                .expectBody()
                .jsonPath("$.items.length()").isEqualTo(1)
                .jsonPath("$.items[0].quantity").isEqualTo(5);

        assertStock("SKU-A", 5, 5);
    }

    @Test
    void release_returns_the_stock_and_is_idempotent() {
        givenStock("SKU-A", 10);
        reserve("order-5", line("SKU-A", 4)).expectStatus().isCreated();

        post("/api/inventory/reservations/order-5/release")
                .expectStatus().isOk().expectBody().jsonPath("$.status").isEqualTo("RELEASED");
        assertStock("SKU-A", 10, 0);

        post("/api/inventory/reservations/order-5/release")                 // second release: no double refund
                .expectStatus().isOk().expectBody().jsonPath("$.status").isEqualTo("RELEASED");
        assertStock("SKU-A", 10, 0);

        post("/api/inventory/reservations/order-5/confirm").expectStatus().isEqualTo(409);
    }

    @Test
    void confirm_turns_reserved_stock_into_sold_stock() {
        givenStock("SKU-A", 10);
        reserve("order-6", line("SKU-A", 4)).expectStatus().isCreated();

        post("/api/inventory/reservations/order-6/confirm")
                .expectStatus().isOk().expectBody().jsonPath("$.status").isEqualTo("CONFIRMED");
        assertStock("SKU-A", 6, 0);

        post("/api/inventory/reservations/order-6/release").expectStatus().isEqualTo(409);
    }

    @Test
    void invalid_request_returns_400_with_one_violation_per_field() {
        client.post().uri("/api/inventory/reservations")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(Map.of("orderId", "", "items", List.of(Map.of("sku", "SKU-A", "quantity", 0))))
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody()
                .jsonPath("$.violations[?(@.field == 'orderId')]").exists()
                .jsonPath("$.violations[?(@.field == 'items[0].quantity')]").exists();
    }

    @Test
    void database_check_constraint_blocks_negative_stock_even_if_code_is_buggy() {
        givenStock("SKU-A", 1);

        // bypass the service entirely and try to oversell with raw SQL
        StepVerifier.create(db.sql("UPDATE inventory_item SET available = available - 5 WHERE sku = 'SKU-A'").then())
                .expectErrorSatisfies(e -> assertThat(e).hasStackTraceContaining("chk_available_non_negative"))
                .verify();

        assertStock("SKU-A", 1, 0);
    }

    // ------------------------------------------------------------------------------------------- helpers

    private static ReservationLine line(String sku, int quantity) {
        return new ReservationLine(sku, quantity);
    }

    private WebTestClient.ResponseSpec reserve(String orderId, ReservationLine... lines) {
        return client.post().uri("/api/inventory/reservations")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(new ReservationRequest(orderId, List.of(lines)))
                .exchange();
    }

    private WebTestClient.ResponseSpec setStock(String sku, int available) {
        return client.put().uri("/api/inventory/{sku}", sku)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(Map.of("available", available))
                .exchange();
    }

    private WebTestClient.ResponseSpec post(String uri) {
        return client.post().uri(uri).exchange();
    }

    private void assertStock(String sku, int available, int reserved) {
        StockLevelResponse stock = client.get().uri("/api/inventory/{sku}", sku)
                .exchange()
                .expectStatus().isOk()
                .expectBody(StockLevelResponse.class)
                .returnResult()
                .getResponseBody();
        assertThat(stock).isNotNull();
        assertThat(stock.available()).as("available").isEqualTo(available);
        assertThat(stock.reserved()).as("reserved").isEqualTo(reserved);
    }
}

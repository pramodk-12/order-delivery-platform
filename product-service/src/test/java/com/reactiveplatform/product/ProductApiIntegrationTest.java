package com.reactiveplatform.product;

import com.reactiveplatform.product.domain.Product;
import com.reactiveplatform.product.dto.PriceUpdate;
import com.reactiveplatform.product.dto.ProductResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.boot.webtestclient.autoconfigure.AutoConfigureWebTestClient;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * FULL integration test: real HTTP server on a random port + a real MongoDB in Docker (Testcontainers).
 * <p>
 * {@code @ServiceConnection} makes Spring Boot point the app at the container automatically.
 * The container runs as a single-node replica set ({@code withReplicaSet()}), so change streams
 * (SSE price updates) work too.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "catalog.seed.enabled=false",
                "catalog.sse.heartbeat=1s",
                // safety net: if the container wiring ever broke, fail instead of using your local MongoDB
                "spring.mongodb.uri=mongodb://no-such-host.invalid:1/never"
        })
@AutoConfigureWebTestClient(timeout = "20s")
@Testcontainers
class ProductApiIntegrationTest {

    // Testcontainers 2.x starts MongoDB STANDALONE by default; change streams need a replica set.
    @Container
    @ServiceConnection
    static MongoDBContainer mongo = new MongoDBContainer("mongo:8.0").withReplicaSet();

    @Autowired
    WebTestClient client;

    @Autowired
    ReactiveMongoTemplate template;

    @BeforeEach
    void emptyCollection() {
        // remove documents but keep the collection and its unique index
        template.remove(new Query(), Product.class).block();
    }

    @Test
    void full_crud_lifecycle() {
        ProductResponse created = create("SKU-100", "Reactive Spring", "books", "39.90");
        assertThat(created.id()).isNotBlank();

        client.get().uri("/api/products/{id}", created.id())
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.name").isEqualTo("Reactive Spring")
                .jsonPath("$.price").isEqualTo(39.90);

        client.put().uri("/api/products/{id}", created.id())
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(Map.of("sku", "SKU-100", "name", "Reactive Spring, 2nd ed.", "category", "books"))
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.name").isEqualTo("Reactive Spring, 2nd ed.")
                .jsonPath("$.price").isEqualTo(39.90);          // untouched by PUT

        client.delete().uri("/api/products/{id}", created.id())
                .exchange()
                .expectStatus().isNoContent();

        client.get().uri("/api/products/{id}", created.id())
                .exchange()
                .expectStatus().isNotFound();
    }

    @Test
    void duplicate_sku_is_rejected_by_the_unique_index_with_409() {
        create("SKU-DUP", "First", "books", "10.00");

        client.post().uri("/api/products")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(body("SKU-DUP", "Second", "books", "12.00"))
                .exchange()
                .expectStatus().isEqualTo(409);
    }

    @Test
    void search_filters_by_category_and_price_range_and_paginates() {
        // As STRINGS, "5" would fall between "10" and "50" - this test proves prices are stored as decimals.
        create("B-1", "Book A", "books", "5.00");
        create("B-2", "Book B", "books", "10.00");
        create("B-3", "Book C", "books", "25.00");
        create("B-4", "Book D", "books", "50.00");
        create("B-5", "Book E", "books", "51.00");
        create("E-1", "Gadget", "electronics", "30.00");

        client.get().uri("/api/products?category=books&minPrice=10&maxPrice=50&page=0&size=2")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.totalElements").isEqualTo(3)
                .jsonPath("$.totalPages").isEqualTo(2)
                .jsonPath("$.content.length()").isEqualTo(2)
                .jsonPath("$.content[0].name").isEqualTo("Book B")
                .jsonPath("$.content[1].name").isEqualTo("Book C");

        client.get().uri("/api/products?category=books&minPrice=10&maxPrice=50&page=1&size=2")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.content.length()").isEqualTo(1)
                .jsonPath("$.content[0].name").isEqualTo("Book D");
    }

    @Test
    void ndjson_stream_emits_every_product() {
        List<Product> products = IntStream.rangeClosed(1, 50)
                .mapToObj(i -> Product.newProduct("S-" + i, "Item " + i, null, "toys", BigDecimal.TEN))
                .toList();
        template.insert(products, Product.class).blockLast();

        Flux<ProductResponse> stream = client.get().uri("/api/catalog/stream")
                .accept(MediaType.APPLICATION_NDJSON)
                .exchange()
                .expectStatus().isOk()
                .expectHeader().contentTypeCompatibleWith(MediaType.APPLICATION_NDJSON)
                .returnResult(ProductResponse.class)
                .getResponseBody();

        StepVerifier.create(stream)
                .expectNextCount(50)
                .verifyComplete();
    }

    @Test
    void invalid_query_param_on_a_functional_route_returns_400() {
        client.get().uri("/api/catalog/stream?delayMs=abc")
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody()
                .jsonPath("$.status").isEqualTo(400)
                .jsonPath("$.path").isEqualTo("/api/catalog/stream");
    }

    @Test
    void price_change_is_pushed_to_sse_clients_by_the_mongodb_change_stream() {
        ProductResponse product = create("SKU-SSE", "Live Price Book", "books", "10.00");

        Flux<ServerSentEvent<PriceUpdate>> events = client.get().uri("/api/catalog/price-updates")
                .accept(MediaType.TEXT_EVENT_STREAM)
                .exchange()
                .expectStatus().isOk()
                .returnResult(new ParameterizedTypeReference<ServerSentEvent<PriceUpdate>>() {
                })
                .getResponseBody()
                .filter(event -> event.data() != null);        // skip keep-alive comments

        StepVerifier.create(events)
                .then(() -> {
                    pause(Duration.ofSeconds(1));               // let the change stream cursor open
                    changePrice(product.id(), "19.99");
                })
                .assertNext(event -> {
                    assertThat(event.event()).isEqualTo("price-update");
                    assertThat(event.data().productId()).isEqualTo(product.id());
                    assertThat(event.data().newPrice()).isEqualByComparingTo("19.99");
                })
                .thenCancel()                                   // a client disconnecting
                .verify(Duration.ofSeconds(20));
    }

    // ------------------------------------------------------------------------------------------- helpers

    private ProductResponse create(String sku, String name, String category, String price) {
        return client.post().uri("/api/products")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(body(sku, name, category, price))
                .exchange()
                .expectStatus().isCreated()
                .expectBody(ProductResponse.class)
                .returnResult()
                .getResponseBody();
    }

    private void changePrice(String id, String price) {
        client.patch().uri("/api/products/{id}/price", id)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(Map.of("price", new BigDecimal(price)))
                .exchange()
                .expectStatus().isOk();
    }

    private static Map<String, Object> body(String sku, String name, String category, String price) {
        return Map.of("sku", sku, "name", name, "category", category, "price", new BigDecimal(price));
    }

    private static void pause(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}

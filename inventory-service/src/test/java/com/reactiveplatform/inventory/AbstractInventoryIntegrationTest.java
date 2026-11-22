package com.reactiveplatform.inventory;

import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webtestclient.autoconfigure.AutoConfigureWebTestClient;
import org.springframework.context.annotation.Import;
import org.springframework.r2dbc.core.DatabaseClient;

/**
 * Shared setup for all integration tests. Identical annotations => Spring caches ONE application context
 * (and therefore one MySQL container) for every subclass.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "inventory.seed.enabled=false",
                // safety net: if container wiring ever broke, fail instead of using a real local MySQL
                "spring.r2dbc.url=r2dbc:mysql://no-such-host.invalid:1/never",
                "spring.flyway.url=jdbc:mysql://no-such-host.invalid:1/never"
        })
@AutoConfigureWebTestClient(timeout = "30s")
@Import(MySqlTestcontainersConfiguration.class)
public abstract class AbstractInventoryIntegrationTest {

    @Autowired
    protected DatabaseClient db;

    @BeforeEach
    void emptyTables() {
        db.sql("DELETE FROM stock_reservation").then()
                .then(db.sql("DELETE FROM inventory_item").then())
                .block();
    }

    protected void givenStock(String sku, int available) {
        db.sql("INSERT INTO inventory_item (sku, available, reserved, version) VALUES (:sku, :available, 0, 0)")
                .bind("sku", sku)
                .bind("available", available)
                .then()
                .block();
    }
}

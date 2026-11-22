package com.reactiveplatform.inventory.bootstrap;

import com.reactiveplatform.inventory.domain.InventoryItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.relational.core.query.Query;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Random;

/**
 * Gives every SKU seeded by product-service (SKU-00001 ... SKU-01000) a stock level of 0..50, so the two
 * services line up. Some SKUs get 0 on purpose, to see "out of stock" behaviour.
 * <p>
 * {@code block()} is fine here: startup, main thread, never a Netty event-loop thread.
 */
@Component
@ConditionalOnProperty(name = "inventory.seed.enabled", havingValue = "true", matchIfMissing = true)
public class InventorySeeder implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(InventorySeeder.class);

    private final R2dbcEntityTemplate template;
    private final int count;

    public InventorySeeder(R2dbcEntityTemplate template, @Value("${inventory.seed.count:1000}") int count) {
        this.template = template;
        this.count = count;
    }

    @Override
    public void run(ApplicationArguments args) {
        Random random = new Random(7);   // one Random for the whole run: deterministic AND well spread

        Long inserted = template.count(Query.empty(), InventoryItem.class)
                .filter(existing -> existing == 0)
                .flatMapMany(empty -> Flux.range(1, count)
                        .map(i -> InventoryItem.newItem("SKU-%05d".formatted(i), random.nextInt(51)))
                        // up to 8 inserts in flight; each one borrows a pooled connection
                        .flatMap(template::insert, 8))
                .count()
                .block(Duration.ofMinutes(2));

        if (inserted != null && inserted > 0) {
            log.info("Seeded stock for {} SKUs", inserted);
        } else {
            log.info("Inventory already has data - seeding skipped");
        }
    }
}

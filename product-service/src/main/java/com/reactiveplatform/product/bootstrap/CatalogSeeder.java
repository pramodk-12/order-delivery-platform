package com.reactiveplatform.product.bootstrap;

import com.reactiveplatform.product.domain.Product;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.util.List;
import java.util.Random;

/**
 * Loads sample products on startup when the collection is empty, so you have data to search and stream.
 * Disable with {@code catalog.seed.enabled=false}; change the amount with {@code catalog.seed.count}.
 * <p>
 * {@code block()} is acceptable HERE only: this runs once on the main thread during startup,
 * never on a Netty event-loop thread.
 */
@Component
@ConditionalOnProperty(name = "catalog.seed.enabled", havingValue = "true", matchIfMissing = true)
public class CatalogSeeder implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(CatalogSeeder.class);
    private static final List<String> CATEGORIES = List.of("books", "electronics", "grocery", "home", "sports", "toys");
    private static final int BATCH_SIZE = 1_000;

    private final ReactiveMongoTemplate template;
    private final int count;

    public CatalogSeeder(ReactiveMongoTemplate template, @Value("${catalog.seed.count:1000}") int count) {
        this.template = template;
        this.count = count;
    }

    @Override
    public void run(ApplicationArguments args) {
        // ONE Random with a fixed seed: same data on every fresh start, but well-spread prices.
        // (new Random(i) per product would give nearly identical first values for neighbouring seeds.)
        Random random = new Random(42);

        Long inserted = template.count(new Query(), Product.class)
                .filter(existing -> existing == 0)
                .flatMapMany(empty -> Flux.range(1, count)
                        .map(i -> sampleProduct(i, random))
                        .buffer(BATCH_SIZE)                                            // insert in batches
                        .concatMap(batch -> template.insert(batch, Product.class)))   // one batch at a time
                .count()
                .block(Duration.ofMinutes(2));

        if (inserted != null && inserted > 0) {
            log.info("Seeded {} sample products", inserted);
        } else {
            log.info("Catalog already has data - seeding skipped");
        }
    }

    private static Product sampleProduct(int i, Random random) {
        String category = CATEGORIES.get(i % CATEGORIES.size());
        BigDecimal price = BigDecimal.valueOf(1 + random.nextDouble() * 499).setScale(2, RoundingMode.HALF_UP);
        return Product.newProduct(
                "SKU-%05d".formatted(i),
                "%s item %05d".formatted(capitalize(category), i),
                "Sample %s product number %d".formatted(category, i),
                category,
                price);
    }

    private static String capitalize(String s) {
        return Character.toUpperCase(s.charAt(0)) + s.substring(1);
    }
}

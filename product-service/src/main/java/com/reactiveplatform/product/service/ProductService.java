package com.reactiveplatform.product.service;

import com.reactiveplatform.product.domain.Product;
import com.reactiveplatform.product.domain.ProductRepository;
import com.reactiveplatform.product.dto.CreateProductRequest;
import com.reactiveplatform.product.dto.PageResponse;
import com.reactiveplatform.product.dto.ProductResponse;
import com.reactiveplatform.product.dto.UpdateProductRequest;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.regex.Pattern;

import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

/**
 * Business logic for the catalog.
 * <p>
 * Every method returns Mono/Flux and never blocks: it only DESCRIBES the work. WebFlux subscribes when
 * the HTTP response is written, and the MongoDB driver calls us back when results arrive.
 * <p>
 * Two data-access styles on purpose:
 * <ul>
 *   <li>{@link ProductRepository} - simple CRUD, like Spring Data JPA</li>
 *   <li>{@link ReactiveMongoTemplate} - dynamic queries and atomic updates</li>
 * </ul>
 */
@Service
public class ProductService {

    private final ProductRepository repository;
    private final ReactiveMongoTemplate template;

    public ProductService(ProductRepository repository, ReactiveMongoTemplate template) {
        this.repository = repository;
        this.template = template;
    }

    public Mono<ProductResponse> create(CreateProductRequest request) {
        Product product = Product.newProduct(request.sku(), request.name(), request.description(),
                request.category(), request.price());

        return repository.save(product)
                .map(ProductResponse::from)
                // the unique index on sku raises E11000 -> Spring translates it to DuplicateKeyException
                .onErrorMap(DuplicateKeyException.class, e -> new DuplicateSkuException(request.sku()));
    }

    public Mono<ProductResponse> findById(String id) {
        return repository.findById(id)
                .map(ProductResponse::from)
                // "not found" in reactive = an EMPTY Mono. Turn it into an error the API layer maps to 404.
                // Mono.error(Supplier) is lazy: the exception is only created if the Mono really is empty.
                .switchIfEmpty(Mono.error(() -> new ProductNotFoundException(id)));
    }

    /**
     * Dynamic search with pagination.
     * <p>
     * Reactive repositories can't return {@code Page<T>}, so we run two queries IN PARALLEL with
     * {@code Mono.zip}: one for the page content, one for the total count.
     */
    public Mono<PageResponse<ProductResponse>> search(String category, String nameContains,
                                                      BigDecimal minPrice, BigDecimal maxPrice,
                                                      int page, int size) {
        Query filter = buildFilter(category, nameContains, minPrice, maxPrice);

        Mono<List<ProductResponse>> content = template
                .find(Query.of(filter).with(PageRequest.of(page, size, Sort.by("name"))), Product.class)
                .map(ProductResponse::from)
                .collectList();

        Mono<Long> total = template.count(filter, Product.class);

        return Mono.zip(content, total)
                .map(tuple -> PageResponse.of(tuple.getT1(), page, size, tuple.getT2()));
    }

    /**
     * Updates details with ONE atomic MongoDB operation ($set + return the new document).
     * No "read, modify in Java, save" round trip, so two concurrent updates can't overwrite each other.
     */
    public Mono<ProductResponse> update(String id, UpdateProductRequest request) {
        Update update = new Update()
                .set("sku", request.sku())
                .set("name", request.name())
                .set("description", request.description())
                .set("category", request.category())
                .set("updatedAt", Instant.now());

        return findAndModify(id, update)
                .onErrorMap(DuplicateKeyException.class, e -> new DuplicateSkuException(request.sku()));
    }

    /** Changing the price also feeds the live price stream (see PriceChangeStream). */
    public Mono<ProductResponse> updatePrice(String id, BigDecimal newPrice) {
        Update update = new Update()
                .set("price", newPrice)
                .set("updatedAt", Instant.now());
        return findAndModify(id, update);
    }

    public Mono<Void> delete(String id) {
        return template.remove(query(where("id").is(id)), Product.class)
                .flatMap(result -> result.getDeletedCount() == 0
                        ? Mono.<Void>error(new ProductNotFoundException(id))
                        : Mono.<Void>empty());
    }

    /**
     * The whole catalog as a stream. The driver reads it from a MongoDB cursor in batches, and only asks
     * for the next batch when downstream (ultimately the HTTP client's TCP connection) wants more:
     * memory stays flat whether there are 100 or 10 million products.
     */
    public Flux<ProductResponse> streamAll() {
        return repository.findAll().map(ProductResponse::from);
    }

    // ------------------------------------------------------------------------------------------- helpers

    private Mono<ProductResponse> findAndModify(String id, Update update) {
        return template.findAndModify(query(where("id").is(id)), update,
                        FindAndModifyOptions.options().returnNew(true), Product.class)
                .map(ProductResponse::from)
                .switchIfEmpty(Mono.error(() -> new ProductNotFoundException(id)));
    }

    private static Query buildFilter(String category, String nameContains, BigDecimal minPrice, BigDecimal maxPrice) {
        Query query = new Query();
        if (category != null && !category.isBlank()) {
            query.addCriteria(where("category").is(category));
        }
        if (nameContains != null && !nameContains.isBlank()) {
            // case-insensitive "contains"; Pattern.quote stops users injecting regex syntax.
            // (A regex can't use an index well - real full-text search would use a text index / Atlas Search.)
            query.addCriteria(where("name").regex(Pattern.quote(nameContains), "i"));
        }
        if (minPrice != null || maxPrice != null) {
            Criteria price = where("price");
            if (minPrice != null) {
                price = price.gte(minPrice);
            }
            if (maxPrice != null) {
                price = price.lte(maxPrice);
            }
            query.addCriteria(price);
        }
        return query;
    }
}

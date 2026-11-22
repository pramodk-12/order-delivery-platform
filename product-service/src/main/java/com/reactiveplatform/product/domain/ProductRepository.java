package com.reactiveplatform.product.domain;

import org.springframework.data.mongodb.repository.ReactiveMongoRepository;

/**
 * Same idea as JpaRepository, but every method returns Mono/Flux and talks to MongoDB through the
 * non-blocking Reactive Streams driver (no thread waits for the database).
 * <p>
 * Note: reactive repositories have no {@code Page<T>} return type - see ProductService#search for how
 * pagination is built with {@code Mono.zip}.
 */
public interface ProductRepository extends ReactiveMongoRepository<Product, String> {
}

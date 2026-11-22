package com.reactiveplatform.inventory.domain;

import org.springframework.data.r2dbc.repository.R2dbcRepository;
import reactor.core.publisher.Flux;

public interface StockReservationRepository extends R2dbcRepository<StockReservation, Long> {

    /** Derived query, same naming rules as Spring Data JPA - but it returns a Flux. */
    Flux<StockReservation> findByOrderIdOrderBySku(String orderId);

    Flux<StockReservation> findBySku(String sku);
}

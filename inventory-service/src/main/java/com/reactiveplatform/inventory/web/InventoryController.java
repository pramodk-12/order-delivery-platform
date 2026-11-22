package com.reactiveplatform.inventory.web;

import com.reactiveplatform.inventory.dto.ReservationRequest;
import com.reactiveplatform.inventory.dto.ReservationResponse;
import com.reactiveplatform.inventory.dto.SetStockRequest;
import com.reactiveplatform.inventory.dto.StockLevelResponse;
import com.reactiveplatform.inventory.service.InventoryService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Size;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.util.List;

@RestController
@RequestMapping("/api/inventory")
public class InventoryController {

    private final InventoryService inventory;

    public InventoryController(InventoryService inventory) {
        this.inventory = inventory;
    }

    // ------------------------------------------------------------------------------------- stock levels

    @GetMapping("/{sku}")
    public Mono<StockLevelResponse> stock(@PathVariable String sku) {
        return inventory.getStock(sku);
    }

    /** GET /api/inventory?skus=SKU-00001,SKU-00002 - unknown SKUs are simply absent from the result. */
    @GetMapping
    public Flux<StockLevelResponse> stock(@RequestParam @Size(min = 1, max = 100) List<String> skus) {
        return inventory.getStock(skus);
    }

    @PutMapping("/{sku}")
    public Mono<StockLevelResponse> setStock(@PathVariable String sku, @Valid @RequestBody SetStockRequest request) {
        return inventory.setStock(sku, request.available());
    }

    // ------------------------------------------------------------------------------------- reservations

    /** 201 Created for a new reservation, 200 OK when the same orderId is sent again (idempotent replay). */
    @PostMapping("/reservations")
    public Mono<ResponseEntity<ReservationResponse>> reserve(@Valid @RequestBody ReservationRequest request) {
        return inventory.reserve(request)
                .map(outcome -> outcome.created()
                        ? ResponseEntity.created(URI.create("/api/inventory/reservations/" + request.orderId()))
                                .body(outcome.reservation())
                        : ResponseEntity.ok(outcome.reservation()));
    }

    @GetMapping("/reservations/{orderId}")
    public Mono<ReservationResponse> reservation(@PathVariable String orderId) {
        return inventory.getReservation(orderId);
    }

    @PostMapping("/reservations/{orderId}/release")
    public Mono<ReservationResponse> release(@PathVariable String orderId) {
        return inventory.release(orderId);
    }

    @PostMapping("/reservations/{orderId}/confirm")
    public Mono<ReservationResponse> confirm(@PathVariable String orderId) {
        return inventory.confirm(orderId);
    }
}

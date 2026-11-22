package com.reactiveplatform.product.web;

import com.reactiveplatform.product.dto.CreateProductRequest;
import com.reactiveplatform.product.dto.PageResponse;
import com.reactiveplatform.product.dto.PriceUpdateRequest;
import com.reactiveplatform.product.dto.ProductResponse;
import com.reactiveplatform.product.dto.UpdateProductRequest;
import com.reactiveplatform.product.service.ProductService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.net.URI;

/**
 * STYLE 1 - annotated controller.
 * <p>
 * Looks exactly like Spring MVC. The only difference: methods return Mono/Flux instead of plain objects.
 * The method returns IMMEDIATELY with a "recipe"; the Netty thread is free to serve other requests while
 * MongoDB works, and WebFlux writes the response when the Mono emits.
 * <p>
 * Errors thrown/signalled here are turned into JSON by {@link GlobalErrorHandler}.
 */
@RestController
@RequestMapping("/api/products")
public class ProductController {

    private final ProductService service;

    public ProductController(ProductService service) {
        this.service = service;
    }

    @PostMapping
    public Mono<ResponseEntity<ProductResponse>> create(@Valid @RequestBody CreateProductRequest request) {
        return service.create(request)
                .map(created -> ResponseEntity
                        .created(URI.create("/api/products/" + created.id()))
                        .body(created));
    }

    @GetMapping("/{id}")
    public Mono<ProductResponse> get(@PathVariable String id) {
        return service.findById(id);
    }

    @GetMapping
    public Mono<PageResponse<ProductResponse>> search(
            @RequestParam(required = false) String category,
            @RequestParam(required = false) String q,
            @RequestParam(required = false) BigDecimal minPrice,
            @RequestParam(required = false) BigDecimal maxPrice,
            @RequestParam(defaultValue = "0") @Min(0) int page,
            @RequestParam(defaultValue = "20") @Min(1) @Max(100) int size) {
        return service.search(category, q, minPrice, maxPrice, page, size);
    }

    @PutMapping("/{id}")
    public Mono<ProductResponse> update(@PathVariable String id, @Valid @RequestBody UpdateProductRequest request) {
        return service.update(id, request);
    }

    @PatchMapping("/{id}/price")
    public Mono<ProductResponse> updatePrice(@PathVariable String id, @Valid @RequestBody PriceUpdateRequest request) {
        return service.updatePrice(id, request.price());
    }

    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public Mono<Void> delete(@PathVariable String id) {
        return service.delete(id);
    }
}

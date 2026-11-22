package com.reactiveplatform.product.service;

public class ProductNotFoundException extends RuntimeException {

    public ProductNotFoundException(String id) {
        super("Product " + id + " not found");
    }
}

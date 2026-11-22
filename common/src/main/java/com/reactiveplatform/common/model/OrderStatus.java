package com.reactiveplatform.common.model;

/** Order lifecycle used across the saga. */
public enum OrderStatus {
    PENDING,            // request accepted (202), processing started
    STOCK_RESERVED,     // inventory reserved, OrderCreatedEvent published
    CONFIRMED,          // payment completed
    CANCELLED,          // payment failed / compensation done
    OUT_FOR_DELIVERY,
    DELIVERED;

    public boolean isTerminal() {
        return this == CANCELLED || this == DELIVERED;
    }
}

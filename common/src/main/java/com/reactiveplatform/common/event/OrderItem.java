package com.reactiveplatform.common.event;

import java.math.BigDecimal;

public record OrderItem(String productId, int quantity, BigDecimal unitPrice) {

    public BigDecimal lineTotal() {
        return unitPrice.multiply(BigDecimal.valueOf(quantity));
    }
}

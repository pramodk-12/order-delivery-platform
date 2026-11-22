package com.reactiveplatform.inventory.service;

import com.reactiveplatform.inventory.domain.ReservationStatus;

/** All domain errors of the inventory service, each mapped to an HTTP status by GlobalErrorHandler. */
public final class InventoryExceptions {

    private InventoryExceptions() {
    }

    /** 409 - not enough stock for one line; the whole reservation is rolled back. */
    public static class InsufficientStockException extends RuntimeException {
        private final String sku;

        public InsufficientStockException(String sku, int requested, int available) {
            super("Insufficient stock for " + sku + ": requested " + requested + ", available " + available);
            this.sku = sku;
        }

        public String sku() {
            return sku;
        }
    }

    /** 404 */
    public static class UnknownSkuException extends RuntimeException {
        public UnknownSkuException(String sku) {
            super("No inventory record for SKU " + sku);
        }
    }

    /** 404 */
    public static class ReservationNotFoundException extends RuntimeException {
        public ReservationNotFoundException(String orderId) {
            super("No reservation for order " + orderId);
        }
    }

    /** 409 - e.g. confirming an order whose stock was already released. */
    public static class InvalidReservationStateException extends RuntimeException {
        public InvalidReservationStateException(String orderId, ReservationStatus current, ReservationStatus target) {
            super("Reservation for order " + orderId + " is " + current + " and cannot become " + target);
        }
    }

    /** 409 - optimistic locking kept losing to concurrent buyers and we gave up retrying. */
    public static class StockContentionException extends RuntimeException {
        public StockContentionException(String orderId, long attempts) {
            super("Stock for order " + orderId + " is being updated concurrently; gave up after "
                    + attempts + " retries - please retry");
        }
    }
}

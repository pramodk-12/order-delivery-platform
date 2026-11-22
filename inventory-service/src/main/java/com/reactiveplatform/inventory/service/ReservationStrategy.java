package com.reactiveplatform.inventory.service;

/**
 * Two ways to stop overselling. See OversellingConcurrencyTest for measured results.
 */
public enum ReservationStrategy {

    /**
     * One conditional UPDATE: {@code SET available = available - q ... WHERE sku = ? AND available >= q}.
     * The database checks and changes the row in one step while holding its row lock.
     * No retries needed, correct under any contention. Best for "hot" items (flash sales).
     */
    ATOMIC,

    /**
     * Read the row, check stock in Java, save with {@code WHERE version = ?}; if another transaction won,
     * retry the whole transaction. No locks held between read and write, but under heavy contention most
     * attempts lose and requests may give up. Fine when conflicts are rare.
     */
    OPTIMISTIC
}

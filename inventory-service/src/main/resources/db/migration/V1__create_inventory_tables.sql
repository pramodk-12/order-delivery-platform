-- Flyway migration: runs once at startup over JDBC (Flyway has no R2DBC support).
-- After that, every request uses the non-blocking R2DBC driver.

CREATE TABLE inventory_item
(
    -- SKU is the business key shared with product-service, so services never depend
    -- on each other's internal ids (MongoDB ObjectId vs MySQL key).
    sku        VARCHAR(40)  NOT NULL PRIMARY KEY,
    available  INT          NOT NULL,                 -- can still be reserved
    reserved   INT          NOT NULL DEFAULT 0,       -- held for orders that aren't paid yet
    version    BIGINT       NOT NULL DEFAULT 0,       -- optimistic locking (@Version)
    updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),

    -- Last line of defence: even buggy application code can't oversell.
    CONSTRAINT chk_available_non_negative CHECK (available >= 0),
    CONSTRAINT chk_reserved_non_negative CHECK (reserved >= 0)
);

CREATE TABLE stock_reservation
(
    id         BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,
    order_id   VARCHAR(64)  NOT NULL,
    sku        VARCHAR(40)  NOT NULL,
    quantity   INT          NOT NULL,
    status     VARCHAR(16)  NOT NULL,                 -- RESERVED -> CONFIRMED | RELEASED
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),

    -- one line per SKU per order: makes "reserve" idempotent even under concurrent duplicates
    CONSTRAINT uq_reservation_order_sku UNIQUE (order_id, sku),
    CONSTRAINT fk_reservation_item FOREIGN KEY (sku) REFERENCES inventory_item (sku),
    CONSTRAINT chk_quantity_positive CHECK (quantity > 0)
);

-- Runs once, on first start of an empty MySQL volume.
-- One schema per service ("database per service" pattern), all on a single MySQL instance for local dev.

CREATE DATABASE IF NOT EXISTS inventory_db;  -- Phase 3: inventory-service (R2DBC)
CREATE DATABASE IF NOT EXISTS order_db;      -- Phase 4: order-service    (R2DBC)
CREATE DATABASE IF NOT EXISTS payment_db;    -- Phase 5: payment-service  (JPA / JDBC)

-- The 'app' user is created by the MYSQL_USER / MYSQL_PASSWORD env vars of the container.
GRANT ALL PRIVILEGES ON inventory_db.* TO 'app'@'%';
GRANT ALL PRIVILEGES ON order_db.*     TO 'app'@'%';
GRANT ALL PRIVILEGES ON payment_db.*   TO 'app'@'%';
FLUSH PRIVILEGES;

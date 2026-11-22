package com.reactiveplatform.inventory;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.mysql.MySQLContainer;

/**
 * A real MySQL in Docker for tests, declared as a Spring bean.
 * <p>
 * {@code @ServiceConnection} wires it into BOTH connection paths automatically:
 * R2DBC (the app's runtime access) and JDBC (Flyway migrations).
 * As a bean, it lives as long as the cached Spring context, so every test class sharing the same
 * configuration reuses one container instead of starting a new one.
 */
@TestConfiguration(proxyBeanMethods = false)
public class MySqlTestcontainersConfiguration {

    @Bean
    @ServiceConnection
    MySQLContainer mysql() {
        return new MySQLContainer("mysql:8.4");
    }
}

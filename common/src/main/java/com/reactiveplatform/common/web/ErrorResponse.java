package com.reactiveplatform.common.web;

import java.time.Instant;
import java.util.List;

/** Uniform error body returned by every service (MVC and WebFlux alike). */
public record ErrorResponse(
        Instant timestamp,
        int status,
        String error,
        String message,
        String path,
        String correlationId,
        List<FieldViolation> violations
) {

    public record FieldViolation(String field, String message) {
    }

    public static ErrorResponse of(int status, String error, String message, String path, String correlationId) {
        return new ErrorResponse(Instant.now(), status, error, message, path, correlationId, List.of());
    }

    public static ErrorResponse withViolations(int status, String error, String message, String path,
                                               String correlationId, List<FieldViolation> violations) {
        return new ErrorResponse(Instant.now(), status, error, message, path, correlationId, List.copyOf(violations));
    }
}

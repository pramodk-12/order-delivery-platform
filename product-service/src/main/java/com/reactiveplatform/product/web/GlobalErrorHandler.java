package com.reactiveplatform.product.web;

import com.reactiveplatform.common.web.CorrelationId;
import com.reactiveplatform.common.web.ErrorResponse;
import com.reactiveplatform.product.service.DuplicateSkuException;
import com.reactiveplatform.product.service.ProductNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.bind.support.WebExchangeBindException;
import org.springframework.web.method.annotation.HandlerMethodValidationException;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.server.ServerWebInputException;

import java.util.List;

/**
 * Maps exceptions from ANNOTATED controllers to the platform-wide {@link ErrorResponse}.
 * <p>
 * Interview gotcha: {@code @RestControllerAdvice} does NOT apply to functional endpoints (RouterFunction).
 * Those handle errors in the router itself - see CatalogRouter.
 */
@RestControllerAdvice
public class GlobalErrorHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalErrorHandler.class);

    @ExceptionHandler(ProductNotFoundException.class)
    ResponseEntity<ErrorResponse> notFound(ProductNotFoundException ex, ServerHttpRequest request) {
        return error(HttpStatus.NOT_FOUND, ex.getMessage(), request);
    }

    @ExceptionHandler(DuplicateSkuException.class)
    ResponseEntity<ErrorResponse> duplicate(DuplicateSkuException ex, ServerHttpRequest request) {
        return error(HttpStatus.CONFLICT, ex.getMessage(), request);
    }

    /** @Valid @RequestBody failed: one violation per invalid field. */
    @ExceptionHandler(WebExchangeBindException.class)
    ResponseEntity<ErrorResponse> invalidBody(WebExchangeBindException ex, ServerHttpRequest request) {
        List<ErrorResponse.FieldViolation> violations = ex.getFieldErrors().stream()
                .map(fe -> new ErrorResponse.FieldViolation(fe.getField(), fe.getDefaultMessage()))
                .toList();
        return ResponseEntity.badRequest().body(ErrorResponse.withViolations(400, "Bad Request",
                "Validation failed", request.getPath().value(), correlationId(request), violations));
    }

    /** @Min/@Max on @RequestParam failed (built-in method validation, Spring 6.1+). */
    @ExceptionHandler(HandlerMethodValidationException.class)
    ResponseEntity<ErrorResponse> invalidParams(HandlerMethodValidationException ex, ServerHttpRequest request) {
        List<ErrorResponse.FieldViolation> violations = ex.getParameterValidationResults().stream()
                .flatMap(result -> result.getResolvableErrors().stream()
                        .map(err -> new ErrorResponse.FieldViolation(
                                result.getMethodParameter().getParameterName(), err.getDefaultMessage())))
                .toList();
        return ResponseEntity.badRequest().body(ErrorResponse.withViolations(400, "Bad Request",
                "Validation failed", request.getPath().value(), correlationId(request), violations));
    }

    /** Malformed JSON, wrong type (e.g. page=abc), missing body. */
    @ExceptionHandler(ServerWebInputException.class)
    ResponseEntity<ErrorResponse> badInput(ServerWebInputException ex, ServerHttpRequest request) {
        return error(HttpStatus.BAD_REQUEST, ex.getReason(), request);
    }

    @ExceptionHandler(ResponseStatusException.class)
    ResponseEntity<ErrorResponse> statusException(ResponseStatusException ex, ServerHttpRequest request) {
        return error(ex.getStatusCode(), ex.getReason(), request);
    }

    @ExceptionHandler(Exception.class)
    ResponseEntity<ErrorResponse> unexpected(Exception ex, ServerHttpRequest request) {
        log.error("Unhandled error on {}", request.getPath().value(), ex);
        return error(HttpStatus.INTERNAL_SERVER_ERROR, "Unexpected error", request);
    }

    // ---------------------------------------------------------------------------------------------------

    private static ResponseEntity<ErrorResponse> error(HttpStatusCode status, String message, ServerHttpRequest request) {
        String reason = status instanceof HttpStatus hs ? hs.getReasonPhrase() : String.valueOf(status.value());
        return ResponseEntity.status(status).body(ErrorResponse.of(status.value(), reason, message,
                request.getPath().value(), correlationId(request)));
    }

    private static String correlationId(ServerHttpRequest request) {
        return request.getHeaders().getFirst(CorrelationId.HEADER);
    }
}

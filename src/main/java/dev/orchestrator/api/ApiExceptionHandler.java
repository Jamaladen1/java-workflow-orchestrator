package dev.orchestrator.api;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

import java.util.Map;

@RestControllerAdvice
public class ApiExceptionHandler {
    @ExceptionHandler(ApiException.class)
    ResponseEntity<Map<String, Object>> api(ApiException e) {
        return ResponseEntity.status(e.status()).body(Map.of("error", e.getMessage(), "status", e.status().value()));
    }

    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    ResponseEntity<Map<String, Object>> badParam(MethodArgumentTypeMismatchException e) {
        return ResponseEntity.badRequest().body(Map.of("error", "invalid parameter '" + e.getName() + "'", "status", 400));
    }
}

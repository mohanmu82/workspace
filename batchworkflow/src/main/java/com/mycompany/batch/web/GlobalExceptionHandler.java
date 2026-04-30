package com.mycompany.batch.web;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, Object>> handleException(Exception e) {
        String message = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
        Map<String, Object> err = new LinkedHashMap<>();
        err.put("activity", "error");
        err.put("message", message);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("Errors", List.of(err));
        return ResponseEntity.internalServerError().body(body);
    }
}

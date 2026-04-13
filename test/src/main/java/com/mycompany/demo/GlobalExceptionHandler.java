package com.mycompany.demo;

import com.anthropic.errors.AnthropicException;
import com.anthropic.errors.AnthropicServiceException;
import com.fasterxml.jackson.core.JsonProcessingException;
import jakarta.validation.ConstraintViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

@RestControllerAdvice
public class GlobalExceptionHandler {

    // @NotBlank on @RequestParam triggers ConstraintViolationException
    @ExceptionHandler(ConstraintViolationException.class)
    public ResponseEntity<Map<String, Object>> handleConstraintViolation(ConstraintViolationException ex) {
        String message = ex.getConstraintViolations().stream()
                .map(cv -> cv.getMessage())
                .collect(Collectors.joining("; "));
        return error(HttpStatus.BAD_REQUEST, "VALIDATION_ERROR", message);
    }

    // Missing required @RequestParam entirely
    @ExceptionHandler(MissingServletRequestParameterException.class)
    public ResponseEntity<Map<String, Object>> handleMissingParam(MissingServletRequestParameterException ex) {
        return error(HttpStatus.BAD_REQUEST, "MISSING_PARAMETER",
                "Required parameter '" + ex.getParameterName() + "' is missing");
    }

    // Anthropic HTTP API errors — have a status code (401, 403, 429, 5xx, etc.)
    @ExceptionHandler(AnthropicServiceException.class)
    public ResponseEntity<Map<String, Object>> handleAnthropicServiceException(AnthropicServiceException ex) {
        int statusCode = ex.statusCode();
        String code = switch (statusCode) {
            case 401 -> "INVALID_API_KEY";
            case 403 -> "PERMISSION_DENIED";
            case 429 -> "RATE_LIMIT_EXCEEDED";
            case 529 -> "API_OVERLOADED";
            default  -> statusCode >= 500 ? "CLAUDE_API_ERROR" : "CLAUDE_REQUEST_ERROR";
        };
        String message = switch (statusCode) {
            case 401 -> "The API key is invalid or missing. Check the key and try again.";
            case 403 -> "Access denied. The API key does not have permission for this request.";
            case 429 -> "Too many requests. You have exceeded the Claude API rate limit or quota.";
            case 529 -> "Claude's API is temporarily overloaded. Please try again shortly.";
            default  -> "Claude API error (" + statusCode + "): " + ex.getMessage();
        };
        HttpStatus status = statusCode == 429 ? HttpStatus.TOO_MANY_REQUESTS
                : statusCode >= 500 ? HttpStatus.BAD_GATEWAY
                : HttpStatus.BAD_REQUEST;
        return error(status, code, message);
    }

    // Anthropic non-HTTP errors — network failures, unexpected response shapes, etc.
    @ExceptionHandler(AnthropicException.class)
    public ResponseEntity<Map<String, Object>> handleAnthropicException(AnthropicException ex) {
        return error(HttpStatus.BAD_GATEWAY, "CLAUDE_CONNECTION_ERROR",
                "Could not reach the Claude API: " + ex.getMessage());
    }

    // Claude returned text that is not valid JSON
    @ExceptionHandler(JsonProcessingException.class)
    public ResponseEntity<Map<String, Object>> handleJsonProcessing(JsonProcessingException ex) {
        return error(HttpStatus.INTERNAL_SERVER_ERROR, "INVALID_CLAUDE_RESPONSE",
                "Claude returned a response that could not be parsed as JSON: " + ex.getOriginalMessage());
    }

    // Catch-all for anything else
    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, Object>> handleGeneric(Exception ex) {
        String message = ex.getMessage() != null ? ex.getMessage() : ex.getClass().getSimpleName();
        return error(HttpStatus.INTERNAL_SERVER_ERROR, "INTERNAL_ERROR",
                "An unexpected error occurred: " + message);
    }

    private ResponseEntity<Map<String, Object>> error(HttpStatus status, String code, String message) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("error", true);
        body.put("status", status.value());
        body.put("code", code);
        body.put("message", message);
        return ResponseEntity.status(status).body(body);
    }
}

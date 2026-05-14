package com.example.dice.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

/**
 * Generates structured OTLP logs per request.
 *
 * log_category : svc   → log_type : http | db
 * log_category : audit → log_type : auth | personalinfo
 *
 * Log levels are distributed randomly (INFO 50%, DEBUG 20%, WARN 20%, ERROR 10%).
 * ERROR events include a full stack trace via SLF4J throwable argument.
 *
 * MDC keys (captured by OTel Java agent as OTLP log record attributes):
 *   log_category, log_type, user_id, request_id, and type-specific attributes.
 */
@Service
public class LogGeneratorService {

    private static final Logger logger = LoggerFactory.getLogger(LogGeneratorService.class);
    private static final Random random = new Random();

    // -------------------------------------------------------------------------
    // Enumerations
    // -------------------------------------------------------------------------

    private static final String[] CATEGORIES = {"svc", "audit"};

    private static final Map<String, String[]> TYPES = Map.of(
            "svc",   new String[]{"http", "db"},
            "audit", new String[]{"auth", "personalinfo"}
    );

    // -------------------------------------------------------------------------
    // Message templates per log_type
    // -------------------------------------------------------------------------

    private static final Map<String, List<String>> MESSAGES = Map.of(
            "http", List.of(
                    "HTTP GET /api/users completed",
                    "HTTP POST /api/orders accepted",
                    "HTTP GET /api/products listed",
                    "HTTP DELETE /api/sessions expired",
                    "HTTP PUT /api/cart updated"
            ),
            "db", List.of(
                    "SELECT executed on users table",
                    "INSERT executed on orders table",
                    "UPDATE executed on products table",
                    "DELETE executed on sessions table",
                    "BULK INSERT executed on events table"
            ),
            "auth", List.of(
                    "User login successful",
                    "User logout completed",
                    "Login failed - invalid credentials",
                    "Token refresh completed",
                    "MFA verification succeeded",
                    "Password change completed"
            ),
            "personalinfo", List.of(
                    "Personal information viewed",
                    "Personal information updated",
                    "Personal information export requested",
                    "Personal information deletion requested",
                    "Address information accessed"
            )
    );

    // -------------------------------------------------------------------------
    // Error scenario classes (inner)
    // -------------------------------------------------------------------------

    private static class HttpProcessingException extends RuntimeException {
        HttpProcessingException(String msg, Throwable cause) { super(msg, cause); }
    }

    private static class DatabaseAccessException extends RuntimeException {
        DatabaseAccessException(String msg, Throwable cause) { super(msg, cause); }
    }

    private static class AuthenticationException extends SecurityException {
        AuthenticationException(String msg, Throwable cause) { super(msg, cause); }
    }

    private static class PersonalInfoAccessException extends RuntimeException {
        PersonalInfoAccessException(String msg, Throwable cause) { super(msg, cause); }
    }

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    /**
     * Called per HTTP request. Generates 1-3 log records with varied categories/types/levels.
     */
    public void generateRandomLogs(String player, int diceResult) {
        int count = random.nextInt(3) + 1;
        for (int i = 0; i < count; i++) {
            generateSingleLog(player, diceResult);
        }
    }

    // -------------------------------------------------------------------------
    // Internal logic
    // -------------------------------------------------------------------------

    private void generateSingleLog(String player, int diceResult) {
        String category = CATEGORIES[random.nextInt(CATEGORIES.length)];
        String[] types  = TYPES.get(category);
        String type     = types[random.nextInt(types.length)];
        String userId   = (player != null) ? player : "user-" + (1000 + random.nextInt(9000));
        String requestId = UUID.randomUUID().toString().substring(0, 8);

        // Common MDC attributes → become OTLP log record attributes via OTel Java agent
        MDC.put("log_category", category);
        MDC.put("log_type",     type);
        MDC.put("user_id",      userId);
        MDC.put("request_id",   requestId);
        MDC.put("dice_result",  String.valueOf(diceResult));

        // Type-specific MDC attributes
        addTypeAttributes(type, userId);

        try {
            emitLog(category, type, userId, diceResult);
        } finally {
            MDC.clear();
        }
    }

    private void addTypeAttributes(String type, String userId) {
        switch (type) {
            case "http" -> {
                String[] methods  = {"GET", "POST", "PUT", "DELETE"};
                String[] paths    = {"/api/users", "/api/orders", "/api/products", "/api/cart"};
                String[] statuses = {"200", "201", "400", "404", "500"};
                MDC.put("http_method",      methods[random.nextInt(methods.length)]);
                MDC.put("http_path",        paths[random.nextInt(paths.length)]);
                MDC.put("http_status_code", statuses[random.nextInt(statuses.length)]);
                MDC.put("response_time_ms", String.valueOf(random.nextInt(500) + 10));
            }
            case "db" -> {
                String[] ops     = {"SELECT", "INSERT", "UPDATE", "DELETE"};
                String[] tables  = {"users", "orders", "products", "sessions", "events"};
                MDC.put("db_operation",   ops[random.nextInt(ops.length)]);
                MDC.put("db_table",       tables[random.nextInt(tables.length)]);
                MDC.put("db_duration_ms", String.valueOf(random.nextInt(200) + 1));
                MDC.put("db_rows",        String.valueOf(random.nextInt(100)));
            }
            case "auth" -> {
                String[] results  = {"success", "success", "success", "failure"};  // 75% success
                String[] methods  = {"password", "oauth2", "mfa", "sso"};
                String[] ips      = {"10.0.1.25", "10.0.2.41", "192.168.1.100", "172.16.0.55"};
                MDC.put("auth_result", results[random.nextInt(results.length)]);
                MDC.put("auth_method", methods[random.nextInt(methods.length)]);
                MDC.put("ip_address",  ips[random.nextInt(ips.length)]);
            }
            case "personalinfo" -> {
                String[] dataTypes  = {"profile", "address", "payment", "health", "contact"};
                String[] operations = {"read", "write", "export", "delete"};
                MDC.put("data_type",   dataTypes[random.nextInt(dataTypes.length)]);
                MDC.put("operation",   operations[random.nextInt(operations.length)]);
                MDC.put("requestor",   userId);
                MDC.put("data_subject", "user-" + (1000 + random.nextInt(9000)));
            }
        }
    }

    /**
     * Emits a log record with a randomly selected severity.
     *
     * Distribution:
     *   0-4  (50%) → INFO
     *   5-6  (20%) → DEBUG
     *   7-8  (20%) → WARN
     *   9    (10%) → ERROR  (with stack trace)
     */
    private void emitLog(String category, String type, String userId, int diceResult) {
        List<String> messages = MESSAGES.get(type);
        String message = messages.get(random.nextInt(messages.size()));
        String prefix  = "[" + category + "/" + type + "]";

        int level = random.nextInt(10);

        if (level < 5) {
            logger.info("{} {} - user={} dice={}", prefix, message, userId, diceResult);
        } else if (level < 7) {
            logger.debug("{} {} - user={} dice={}", prefix, message, userId, diceResult);
        } else if (level < 9) {
            logger.warn("{} {} - user={} dice={} (elevated latency detected)", prefix, message, userId, diceResult);
        } else {
            // ERROR path: generate a multi-layer exception for a realistic stack trace
            try {
                triggerError(category, type, userId);
            } catch (Exception e) {
                logger.error("{} Error processing request - user={} dice={}: {}",
                        prefix, userId, diceResult, e.getMessage(), e);
            }
        }
    }

    /**
     * Simulates a failure with nested cause chains so the OTLP log carries a real stack trace.
     */
    private void triggerError(String category, String type, String userId) {
        try {
            performOperation(category, type, userId);
        } catch (Exception root) {
            switch (type) {
                case "http"        -> throw new HttpProcessingException(
                        "HTTP layer failed for " + userId, root);
                case "db"          -> throw new DatabaseAccessException(
                        "DB layer failed for " + userId, root);
                case "auth"        -> throw new AuthenticationException(
                        "Auth layer failed for " + userId, root);
                case "personalinfo"-> throw new PersonalInfoAccessException(
                        "PII access layer failed for " + userId, root);
                default            -> throw new RuntimeException(
                        "Unknown layer failed for " + userId, root);
            }
        }
    }

    /** Inner operation to provide a real call-stack depth. */
    private void performOperation(String category, String type, String userId) {
        validateRequest(userId);
    }

    private void validateRequest(String userId) {
        executeBackend(userId);
    }

    private void executeBackend(String userId) {
        // Simulate the root cause
        throw new IllegalStateException("Backend unavailable (simulated) for user: " + userId);
    }
}

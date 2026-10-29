package dev.orchestrator.execution;

import com.fasterxml.jackson.databind.JsonNode;
import dev.orchestrator.validation.ValidationError;

import java.util.List;

/**
 * A built-in, deterministic task type. Handlers never execute user-supplied code.
 * Handlers must be idempotent (or demo-only) because execution is at-least-once across restarts.
 */
public interface TaskHandler {
    String type();

    void validateConfig(JsonNode config, String path, List<ValidationError> errors);

    JsonNode execute(TaskContext ctx) throws Exception;
}

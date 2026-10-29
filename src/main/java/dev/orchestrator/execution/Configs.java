package dev.orchestrator.execution;

import com.fasterxml.jackson.databind.JsonNode;
import dev.orchestrator.validation.ValidationError;

import java.util.List;

final class Configs {
    private Configs() {}

    static void requireIntRange(JsonNode config, String field, long min, long max, boolean required,
                                String path, List<ValidationError> errors) {
        JsonNode n = config.get(field);
        if (n == null || n.isNull()) {
            if (required) errors.add(new ValidationError("missing_field", path + "." + field, field + " is required"));
            return;
        }
        if (!n.isIntegralNumber() || n.asLong() < min || n.asLong() > max) {
            errors.add(new ValidationError("invalid_value", path + "." + field,
                    field + " must be an integer between " + min + " and " + max));
        }
    }
}

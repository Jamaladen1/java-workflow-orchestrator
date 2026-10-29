package dev.orchestrator.execution;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import dev.orchestrator.validation.ValidationError;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@Component
public class MathTaskHandler implements TaskHandler {
    static final Set<String> OPERATIONS = Set.of("sum", "product", "min", "max", "fail");

    @Override public String type() { return "math"; }

    @Override
    public void validateConfig(JsonNode config, String path, List<ValidationError> errors) {
        String op = config.path("operation").asText("");
        if (!OPERATIONS.contains(op)) {
            errors.add(new ValidationError("invalid_value", path + ".operation",
                    "operation must be one of " + OPERATIONS.stream().sorted().toList()));
            return;
        }
        if (op.equals("fail")) return;
        JsonNode values = config.get("values");
        if (values == null || !values.isArray() || values.isEmpty() || values.size() > 100) {
            errors.add(new ValidationError("invalid_value", path + ".values", "values must be an array of 1-100 numbers"));
            return;
        }
        for (int i = 0; i < values.size(); i++) {
            if (!values.get(i).isNumber()) {
                errors.add(new ValidationError("invalid_value", path + ".values[" + i + "]", "must be a number"));
            }
        }
    }

    @Override
    public JsonNode execute(TaskContext ctx) {
        String op = ctx.config().path("operation").asText();
        if (op.equals("fail")) throw new TaskFailure("math operation 'fail' always fails (demo)");
        List<Double> vals = new ArrayList<>();
        ctx.config().path("values").forEach(v -> vals.add(v.asDouble()));
        double result = switch (op) {
            case "sum" -> vals.stream().mapToDouble(Double::doubleValue).sum();
            case "product" -> vals.stream().reduce(1.0, (a, b) -> a * b);
            case "min" -> vals.stream().mapToDouble(Double::doubleValue).min().orElseThrow();
            case "max" -> vals.stream().mapToDouble(Double::doubleValue).max().orElseThrow();
            default -> throw new TaskFailure("unsupported operation " + op);
        };
        return JsonNodeFactory.instance.objectNode().put("operation", op).put("value", result);
    }
}

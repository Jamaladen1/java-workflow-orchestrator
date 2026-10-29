package dev.orchestrator.execution;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import dev.orchestrator.validation.ValidationError;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Locale;
import java.util.Set;

@Component
public class TransformTaskHandler implements TaskHandler {
    static final Set<String> OPERATIONS = Set.of("upper", "lower", "reverse", "length");

    @Override public String type() { return "transform"; }

    @Override
    public void validateConfig(JsonNode config, String path, List<ValidationError> errors) {
        if (!OPERATIONS.contains(config.path("operation").asText(""))) {
            errors.add(new ValidationError("invalid_value", path + ".operation",
                    "operation must be one of " + OPERATIONS.stream().sorted().toList()));
        }
        JsonNode input = config.get("input");
        if (input == null || !input.isTextual() || input.asText().length() > 1000) {
            errors.add(new ValidationError("invalid_value", path + ".input", "input must be a string of at most 1000 chars"));
        }
    }

    @Override
    public JsonNode execute(TaskContext ctx) {
        String in = ctx.config().path("input").asText();
        String op = ctx.config().path("operation").asText();
        var out = JsonNodeFactory.instance.objectNode().put("operation", op);
        switch (op) {
            case "upper" -> out.put("value", in.toUpperCase(Locale.ROOT));
            case "lower" -> out.put("value", in.toLowerCase(Locale.ROOT));
            case "reverse" -> out.put("value", new StringBuilder(in).reverse().toString());
            case "length" -> out.put("value", in.length());
            default -> throw new TaskFailure("unsupported operation " + op);
        }
        return out;
    }
}

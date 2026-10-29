package dev.orchestrator.execution;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import dev.orchestrator.validation.ValidationError;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class DelayTaskHandler implements TaskHandler {
    public static final long MAX_DURATION_MS = 10_000;

    @Override public String type() { return "delay"; }

    @Override
    public void validateConfig(JsonNode config, String path, List<ValidationError> errors) {
        Configs.requireIntRange(config, "duration_ms", 0, MAX_DURATION_MS, true, path, errors);
    }

    @Override
    public JsonNode execute(TaskContext ctx) throws InterruptedException {
        long ms = ctx.config().path("duration_ms").asLong();
        Thread.sleep(ms);
        return JsonNodeFactory.instance.objectNode().put("slept_ms", ms);
    }
}

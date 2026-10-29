package dev.orchestrator.execution;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import dev.orchestrator.validation.ValidationError;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.function.IntSupplier;

/**
 * Simulated HTTP request. It ONLY calls this service's own /internal/mock endpoint on localhost;
 * no user-controlled URL is ever requested (no SSRF surface).
 */
@Component
public class HttpTaskHandler implements TaskHandler {
    private final IntSupplier port;
    private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();

    @org.springframework.beans.factory.annotation.Autowired
    public HttpTaskHandler(Environment env) {
        this(() -> Integer.parseInt(env.getProperty("local.server.port", env.getProperty("server.port", "8090"))));
    }

    public HttpTaskHandler(IntSupplier port) { this.port = port; }

    @Override public String type() { return "http"; }

    @Override
    public void validateConfig(JsonNode config, String path, List<ValidationError> errors) {
        Configs.requireIntRange(config, "status", 100, 599, true, path, errors);
        Configs.requireIntRange(config, "delay_ms", 0, 10_000, false, path, errors);
        if (config.has("url")) {
            errors.add(new ValidationError("unsupported_field", path + ".url",
                    "arbitrary URLs are not allowed; http tasks call the internal mock endpoint"));
        }
    }

    @Override
    public JsonNode execute(TaskContext ctx) throws Exception {
        int status = ctx.config().path("status").asInt();
        long delay = ctx.config().path("delay_ms").asLong(0);
        URI uri = URI.create("http://127.0.0.1:" + port.getAsInt() + "/internal/mock/" + status + "?delay_ms=" + delay);
        HttpResponse<String> resp = client.send(
                HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(15)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() >= 400) {
            throw new TaskFailure("mock endpoint returned HTTP " + resp.statusCode());
        }
        return JsonNodeFactory.instance.objectNode().put("http_status", resp.statusCode()).put("delay_ms", delay);
    }
}

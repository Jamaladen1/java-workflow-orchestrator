package dev.orchestrator.integration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.http.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.PostgreSQLContainer;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Runs with an API key and a short orphan-sweep interval, which the main suite can't use. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "orchestrator.tick-interval-ms=50",
        "orchestrator.retry-base-ms=200",
        "orchestrator.seed-demo-data=false",
        "orchestrator.orphan-sweep-interval-ms=500",
        "orchestrator.api-key=s3cret"
})
class OrphanSweepAndApiKeyIT {

    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");
    static { postgres.start(); }

    @Autowired TestRestTemplate http;
    @Autowired JdbcTemplate jdbc;
    @Autowired ObjectMapper mapper;
    @LocalServerPort int port;

    private static final String DEF =
            "{\"name\":\"it-key\",\"tasks\":[{\"id\":\"a\",\"type\":\"delay\",\"config\":{\"duration_ms\":50}}]}";

    // java.net.http instead of TestRestTemplate: HttpURLConnection can't read a 401 on a streamed POST.
    private ResponseEntity<JsonNode> post(String path, String body, String key) {
        try {
            HttpRequest.Builder b = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                    .header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString(body));
            if (key != null) b.header("X-API-Key", key);
            HttpResponse<String> r = HttpClient.newHttpClient().send(b.build(), HttpResponse.BodyHandlers.ofString());
            return ResponseEntity.status(r.statusCode()).body(r.body().isEmpty() ? null : mapper.readTree(r.body()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void writesRequireTheApiKeyAndReadsDoNot() {
        assertThat(post("/api/workflows", DEF, null).getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        assertThat(post("/api/workflows", DEF, "wrong").getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        ResponseEntity<JsonNode> created = post("/api/workflows", DEF, "s3cret");
        assertThat(created.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        String id = created.getBody().get("id").asText();

        assertThat(post("/api/workflows/" + id + "/runs", "", null).getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        ResponseEntity<JsonNode> run = post("/api/workflows/" + id + "/runs", "", "s3cret");
        assertThat(run.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        String runId = run.getBody().get("run_id").asText();
        assertThat(post("/api/runs/" + runId + "/cancel", "", null).getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);

        assertThat(http.getForEntity("/api/workflows", JsonNode.class).getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(http.getForEntity("/api/runs/" + runId, JsonNode.class).getStatusCode()).isEqualTo(HttpStatus.OK);
        // dry-run validation is deliberately open (nothing is persisted)
        assertThat(post("/api/workflows/validate", DEF, null).getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    void apiKeyCannotBeBypassedWithEncodedOrParameterisedPaths() {
        for (String path : List.of("/api;x=1/workflows", "/%61pi/workflows", "/api/workflows;x=1", "/api/%77orkflows")) {
            assertThat(post(path, DEF, null).getStatusCode()).as(path).isIn(HttpStatus.UNAUTHORIZED, HttpStatus.NOT_FOUND);
        }
        assertThat(jdbc.queryForObject("SELECT count(*) FROM workflow_definitions WHERE name='it-key'", Integer.class))
                .isLessThanOrEqualTo(1); // at most the one created with the key in the other test
    }

    @Test
    void taskOrphanedWhileTheProcessKeepsRunningIsRecoveredWithoutRestart() {
        ResponseEntity<JsonNode> created = post("/api/workflows",
                "{\"name\":\"it-sweep\",\"tasks\":[{\"id\":\"a\",\"type\":\"delay\",\"config\":{\"duration_ms\":50}}]}", "s3cret");
        UUID wf = UUID.fromString(created.getBody().get("id").asText());
        // e.g. the worker could not persist its outcome during a database outage and gave up
        UUID runId = UUID.randomUUID();
        jdbc.update("INSERT INTO workflow_runs(id, workflow_definition_id, status, started_at) VALUES (?,?, 'running', now())", runId, wf);
        jdbc.update("INSERT INTO task_runs(id, workflow_run_id, task_key, task_type, config, status, attempts, max_attempts, started_at) "
                + "VALUES (?,?,'a','delay','{\"duration_ms\":50}'::jsonb,'running',1,3, now())", UUID.randomUUID(), runId);

        await().atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofMillis(100))
                .until(() -> "succeeded".equals(jdbc.queryForObject("SELECT status FROM workflow_runs WHERE id=?", String.class, runId)));
        assertThat(jdbc.queryForObject("SELECT attempts FROM task_runs WHERE workflow_run_id=?", Integer.class, runId)).isEqualTo(2);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM task_events WHERE workflow_run_id=? AND type='task_recovered'",
                Integer.class, runId)).isEqualTo(1);
    }
}

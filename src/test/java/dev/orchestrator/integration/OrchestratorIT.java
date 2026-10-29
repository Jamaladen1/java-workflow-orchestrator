package dev.orchestrator.integration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.orchestrator.persistence.TaskRunRepository;
import dev.orchestrator.scheduler.WorkflowScheduler;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.http.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "orchestrator.tick-interval-ms=50",
        "orchestrator.retry-base-ms=200",
        "orchestrator.seed-demo-data=false",
        "orchestrator.max-concurrent-tasks=4"
})
class OrchestratorIT {

    // Started once and left to Ryuk to stop, so the Spring context (and scheduler) shuts down first.
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");
    static { postgres.start(); }

    @Autowired TestRestTemplate http;
    @Autowired ObjectMapper mapper;
    @Autowired JdbcTemplate jdbc;
    @Autowired WorkflowScheduler scheduler;
    @Autowired TaskRunRepository taskRuns;
    @Autowired TransactionTemplate tx;

    // ---------- helpers ----------

    private ResponseEntity<JsonNode> post(String path, String body) {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        return http.exchange(path, HttpMethod.POST, new HttpEntity<>(body, h), JsonNode.class);
    }

    private UUID createWorkflow(String json) {
        ResponseEntity<JsonNode> r = post("/api/workflows", json);
        assertThat(r.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        return UUID.fromString(r.getBody().get("id").asText());
    }

    private UUID startRun(UUID workflowId) {
        ResponseEntity<JsonNode> r = post("/api/workflows/" + workflowId + "/runs", "");
        assertThat(r.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(r.getBody().get("status").asText()).isEqualTo("pending");
        return UUID.fromString(r.getBody().get("run_id").asText());
    }

    private JsonNode run(UUID runId) {
        return http.getForObject("/api/runs/" + runId, JsonNode.class);
    }

    private JsonNode awaitStatus(UUID runId, String status, int seconds) {
        await().atMost(Duration.ofSeconds(seconds)).pollInterval(Duration.ofMillis(100))
                .until(() -> run(runId).get("status").asText().equals(status));
        // allow trailing transitions (e.g. skipped marks) to settle
        return run(runId);
    }

    private static Map<String, JsonNode> tasksByKey(JsonNode run) {
        Map<String, JsonNode> m = new HashMap<>();
        run.get("tasks").forEach(t -> m.put(t.get("task_key").asText(), t));
        return m;
    }

    private static OffsetDateTime ts(JsonNode t, String field) {
        return OffsetDateTime.parse(t.get(field).asText());
    }

    private List<String> eventTypes(UUID runId, String taskKey) {
        JsonNode events = http.getForObject("/api/runs/" + runId + "/events", JsonNode.class);
        return StreamSupport.stream(events.spliterator(), false)
                .filter(e -> taskKey == null || taskKey.equals(e.path("task_key").asText(null)))
                .map(e -> e.get("type").asText()).toList();
    }

    // ---------- tests ----------

    @Test
    void parallelBranchesRunConcurrentlyAndJoinWaitsForBoth() {
        UUID wf = createWorkflow("""
                {"name":"it-parallel","tasks":[
                  {"id":"root","type":"delay","config":{"duration_ms":100}},
                  {"id":"left","type":"delay","config":{"duration_ms":1200},"depends_on":["root"]},
                  {"id":"right","type":"delay","config":{"duration_ms":800},"depends_on":["root"]},
                  {"id":"join","type":"math","config":{"operation":"sum","values":[2,3]},"depends_on":["left","right"]}]}""");
        UUID runId = startRun(wf);
        Map<String, JsonNode> t = tasksByKey(awaitStatus(runId, "succeeded", 20));

        // Overlap: each branch started before the other finished.
        assertThat(ts(t.get("left"), "started_at")).isBefore(ts(t.get("right"), "finished_at"));
        assertThat(ts(t.get("right"), "started_at")).isBefore(ts(t.get("left"), "finished_at"));
        // Join barrier: join started only after BOTH branches finished.
        assertThat(ts(t.get("join"), "started_at")).isAfterOrEqualTo(ts(t.get("left"), "finished_at"));
        assertThat(ts(t.get("join"), "started_at")).isAfterOrEqualTo(ts(t.get("right"), "finished_at"));
        assertThat(t.get("join").get("result").get("value").asDouble()).isEqualTo(5.0);
        t.values().forEach(task -> assertThat(task.get("attempts").asInt()).isEqualTo(1));
    }

    @Test
    void failingTaskRetriesWithBackoffThenSucceeds() {
        UUID wf = createWorkflow("""
                {"name":"it-retry","tasks":[
                  {"id":"flaky","type":"math","config":{"operation":"sum","values":[1],"fail_until_attempt":3},"max_attempts":3},
                  {"id":"after","type":"delay","config":{"duration_ms":10},"depends_on":["flaky"]}]}""");
        UUID runId = startRun(wf);
        Map<String, JsonNode> t = tasksByKey(awaitStatus(runId, "succeeded", 20));
        assertThat(t.get("flaky").get("attempts").asInt()).isEqualTo(3);
        assertThat(eventTypes(runId, "flaky")).filteredOn("task_retry_scheduled"::equals).hasSize(2);
        assertThat(ts(t.get("after"), "started_at")).isAfterOrEqualTo(ts(t.get("flaky"), "finished_at"));
    }

    @Test
    void terminalFailurePropagatesSkipsAndFailsRun() {
        UUID wf = createWorkflow("""
                {"name":"it-failfast","tasks":[
                  {"id":"root","type":"delay","config":{"duration_ms":10}},
                  {"id":"bad","type":"math","config":{"operation":"fail"},"max_attempts":2,"depends_on":["root"]},
                  {"id":"slow","type":"delay","config":{"duration_ms":1500},"depends_on":["root"]},
                  {"id":"join","type":"delay","config":{"duration_ms":10},"depends_on":["bad","slow"]},
                  {"id":"tail","type":"delay","config":{"duration_ms":10},"depends_on":["join"]}]}""");
        UUID runId = startRun(wf);
        awaitStatus(runId, "failed", 20);
        // the in-flight 'slow' branch is allowed to finish; outcome stays failed
        await().atMost(Duration.ofSeconds(10)).until(() -> tasksByKey(run(runId)).get("slow").get("status").asText().equals("succeeded"));
        JsonNode r = run(runId);
        Map<String, JsonNode> t = tasksByKey(r);
        assertThat(r.get("status").asText()).isEqualTo("failed");
        assertThat(t.get("bad").get("status").asText()).isEqualTo("failed");
        assertThat(t.get("bad").get("attempts").asInt()).isEqualTo(2);
        assertThat(t.get("join").get("status").asText()).isEqualTo("skipped");
        assertThat(t.get("tail").get("status").asText()).isEqualTo("skipped");
        assertThat(t.get("join").get("attempts").asInt()).isZero();
    }

    @Test
    void restartRecoveryRetriesOrphanedRunningTask() {
        UUID wf = createWorkflow("""
                {"name":"it-recovery","tasks":[
                  {"id":"a","type":"delay","config":{"duration_ms":50}},
                  {"id":"b","type":"delay","config":{"duration_ms":50},"depends_on":["a"]}]}""");
        // Simulate a JVM that crashed mid-task: rows say 'running' but no executor thread owns them.
        UUID runId = UUID.randomUUID();
        jdbc.update("INSERT INTO workflow_runs(id, workflow_definition_id, status, started_at) VALUES (?,?, 'running', now())", runId, wf);
        jdbc.update("INSERT INTO task_runs(id, workflow_run_id, task_key, task_type, config, depends_on, status, attempts, max_attempts, started_at) "
                + "VALUES (?,?,'a','delay','{\"duration_ms\":50}'::jsonb,'[]'::jsonb,'running',1,3, now())", UUID.randomUUID(), runId);
        jdbc.update("INSERT INTO task_runs(id, workflow_run_id, task_key, task_type, config, depends_on, status, max_attempts) "
                + "VALUES (?,?,'b','delay','{\"duration_ms\":50}'::jsonb,'[\"a\"]'::jsonb,'pending',3)", UUID.randomUUID(), runId);

        // Without recovery the run is stuck: the scheduler never touches 'running' rows.
        await().pollDelay(Duration.ofMillis(500)).atMost(Duration.ofSeconds(2)).until(() -> true);
        assertThat(run(runId).get("status").asText()).isEqualTo("running");

        // What a freshly started scheduler does on boot against existing PostgreSQL state.
        assertThat(scheduler.recoverOrphanedTasks()).isEqualTo(1);
        Map<String, JsonNode> t = tasksByKey(awaitStatus(runId, "succeeded", 15));
        assertThat(t.get("a").get("attempts").asInt()).isEqualTo(2);
        assertThat(eventTypes(runId, "a")).contains("task_recovered", "task_started", "task_succeeded");
    }

    @Test
    void restartRecoveryFailsOrphanWhenAttemptsExhausted() {
        UUID wf = createWorkflow("""
                {"name":"it-recovery-exhausted","tasks":[
                  {"id":"a","type":"delay","config":{"duration_ms":50}},
                  {"id":"b","type":"delay","config":{"duration_ms":50},"depends_on":["a"]}]}""");
        UUID runId = UUID.randomUUID();
        jdbc.update("INSERT INTO workflow_runs(id, workflow_definition_id, status, started_at) VALUES (?,?, 'running', now())", runId, wf);
        jdbc.update("INSERT INTO task_runs(id, workflow_run_id, task_key, task_type, config, depends_on, status, attempts, max_attempts, started_at) "
                + "VALUES (?,?,'a','delay','{\"duration_ms\":50}'::jsonb,'[]'::jsonb,'running',3,3, now())", UUID.randomUUID(), runId);
        jdbc.update("INSERT INTO task_runs(id, workflow_run_id, task_key, task_type, config, depends_on, status, max_attempts) "
                + "VALUES (?,?,'b','delay','{\"duration_ms\":50}'::jsonb,'[\"a\"]'::jsonb,'pending',3)", UUID.randomUUID(), runId);
        scheduler.recoverOrphanedTasks();
        Map<String, JsonNode> t = tasksByKey(awaitStatus(runId, "failed", 10));
        assertThat(t.get("a").get("status").asText()).isEqualTo("failed");
        await().atMost(Duration.ofSeconds(5)).until(() -> tasksByKey(run(runId)).get("b").get("status").asText().equals("skipped"));
    }

    @Test
    void cancelStopsPendingTasksAndRejectsSecondCancel() {
        UUID wf = createWorkflow("""
                {"name":"it-cancel","tasks":[
                  {"id":"long","type":"delay","config":{"duration_ms":5000}},
                  {"id":"next","type":"delay","config":{"duration_ms":10},"depends_on":["long"]}]}""");
        UUID runId = startRun(wf);
        await().atMost(Duration.ofSeconds(5)).until(() -> tasksByKey(run(runId)).get("long").get("status").asText().equals("running"));
        ResponseEntity<JsonNode> c = post("/api/runs/" + runId + "/cancel", "");
        assertThat(c.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(post("/api/runs/" + runId + "/cancel", "").getStatusCode()).isEqualTo(HttpStatus.CONFLICT);

        Map<String, JsonNode> t = tasksByKey(run(runId));
        assertThat(run(runId).get("status").asText()).isEqualTo("cancelled");
        assertThat(t.get("long").get("status").asText()).isEqualTo("cancelled");
        assertThat(t.get("next").get("status").asText()).isEqualTo("cancelled");
        // The interrupted thread's late write is refused by CAS and recorded.
        await().atMost(Duration.ofSeconds(3)).until(() -> eventTypes(runId, "long").contains("late_completion_ignored"));
        assertThat(tasksByKey(run(runId)).get("long").get("status").asText()).isEqualTo("cancelled");
    }

    @Test
    void stateTransitionsAreCompareAndSet() {
        UUID wf = createWorkflow("""
                {"name":"it-cas","tasks":[{"id":"a","type":"delay","config":{"duration_ms":1}}]}""");
        UUID runId = UUID.randomUUID();
        UUID taskId = UUID.randomUUID();
        // A terminal run is ignored by the scheduler, so only this test touches the task.
        jdbc.update("INSERT INTO workflow_runs(id, workflow_definition_id, status) VALUES (?,?, 'cancelled')", runId, wf);
        jdbc.update("INSERT INTO task_runs(id, workflow_run_id, task_key, task_type, config, status, max_attempts) "
                + "VALUES (?,?,'a','delay','{}'::jsonb,'ready',3)", taskId, runId);
        assertThat((Integer) tx.execute(st -> taskRuns.claim(taskId))).isEqualTo(1);
        assertThat((Integer) tx.execute(st -> taskRuns.claim(taskId))).isZero();            // no duplicate dispatch
        assertThat((Integer) tx.execute(st -> taskRuns.completeSuccess(taskId, "{}"))).isEqualTo(1);
        assertThat((Integer) tx.execute(st -> taskRuns.completeSuccess(taskId, "{}"))).isZero(); // idempotent completion
        assertThat((Integer) tx.execute(st -> taskRuns.failTerminal(taskId, "late"))).isZero();  // cannot overwrite a terminal state
        assertThat(jdbc.queryForObject("SELECT attempts FROM task_runs WHERE id=?", Integer.class, taskId)).isEqualTo(1);
    }

    @Test
    void validationAndErrorSemantics() {
        ResponseEntity<JsonNode> cyc = post("/api/workflows", """
                {"name":"it-cycle","tasks":[
                  {"id":"a","type":"delay","config":{"duration_ms":1},"depends_on":["b"]},
                  {"id":"b","type":"delay","config":{"duration_ms":1},"depends_on":["a"]},
                  {"id":"c","type":"shell","config":{},"depends_on":["ghost"]}]}""");
        assertThat(cyc.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
        List<String> codes = StreamSupport.stream(cyc.getBody().get("errors").spliterator(), false)
                .map(e -> e.get("code").asText()).toList();
        assertThat(codes).contains("cycle_detected", "unknown_dependency", "unsupported_task_type");

        assertThat(post("/api/workflows", "{not json").getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(http.getForEntity("/api/runs/" + UUID.randomUUID(), JsonNode.class).getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(post("/api/runs/" + UUID.randomUUID() + "/cancel", "").getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);

        // versioning: same name -> version 2
        String def = "{\"name\":\"it-versioned\",\"tasks\":[{\"id\":\"a\",\"type\":\"delay\",\"config\":{\"duration_ms\":1}}]}";
        assertThat(post("/api/workflows", def).getBody().get("version").asInt()).isEqualTo(1);
        assertThat(post("/api/workflows", def).getBody().get("version").asInt()).isEqualTo(2);

        assertThat(http.getForEntity("/health/ready", JsonNode.class).getStatusCode()).isEqualTo(HttpStatus.OK);
    }
}

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

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "orchestrator.tick-interval-ms=50",
        "orchestrator.retry-base-ms=200",
        "orchestrator.seed-demo-data=false",
        "orchestrator.max-concurrent-tasks=4",
        // the restart-recovery tests below drive recovery explicitly
        "orchestrator.orphan-sweep-interval-ms=3600000"
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
    @Autowired DataSource dataSource;

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
        synchronized (scheduler) { // no scheduler tick touches this run while the test drives it
            jdbc.update("INSERT INTO workflow_runs(id, workflow_definition_id, status) VALUES (?,?, 'running')", runId, wf);
            jdbc.update("INSERT INTO task_runs(id, workflow_run_id, task_key, task_type, config, status, max_attempts) "
                    + "VALUES (?,?,'a','delay','{}'::jsonb,'ready',3)", taskId, runId);
            assertThat((Integer) tx.execute(st -> taskRuns.claim(taskId))).isEqualTo(1);
            assertThat((Integer) tx.execute(st -> taskRuns.claim(taskId))).isZero();            // no duplicate dispatch
            assertThat((Integer) tx.execute(st -> taskRuns.completeSuccess(taskId, "{}"))).isEqualTo(1);
            assertThat((Integer) tx.execute(st -> taskRuns.completeSuccess(taskId, "{}"))).isZero(); // idempotent completion
            assertThat((Integer) tx.execute(st -> taskRuns.failTerminal(taskId, "late"))).isZero();  // cannot overwrite a terminal state
            jdbc.update("UPDATE workflow_runs SET status='succeeded' WHERE id=?", runId);
        }
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

    @Test
    void retryableTaskStillRunningWhenRunFailsIsNotLeftWaitingForRetry() {
        UUID wf = createWorkflow("""
                {"name":"it-failfast-inflight-retry","tasks":[
                  {"id":"bad","type":"math","config":{"operation":"fail"},"max_attempts":1},
                  {"id":"slow","type":"delay","config":{"duration_ms":1000,"fail_until_attempt":2},"max_attempts":3}]}""");
        UUID runId = startRun(wf);
        awaitStatus(runId, "failed", 10);
        await().atMost(Duration.ofSeconds(10))
                .until(() -> !tasksByKey(run(runId)).get("slow").get("status").asText().equals("running"));
        // past the 200 ms retry backoff: nothing may be re-dispatched inside a terminal run
        await().pollDelay(Duration.ofMillis(800)).atMost(Duration.ofSeconds(2)).until(() -> true);
        JsonNode r = run(runId);
        Map<String, JsonNode> t = tasksByKey(r);
        assertThat(r.get("status").asText()).isEqualTo("failed");
        assertThat(t.get("slow").get("status").asText()).isEqualTo("failed");
        assertThat(t.get("slow").get("attempts").asInt()).isEqualTo(1);
        r.get("tasks").forEach(task -> assertThat(task.get("status").asText()).isNotIn("pending", "ready", "running"));
    }

    @Test
    void cancelRacingWithClaimStillCancelsTheClaimedTask() throws Exception {
        UUID wf = createWorkflow("""
                {"name":"it-cancel-claim-race","tasks":[{"id":"a","type":"delay","config":{"duration_ms":50}}]}""");
        UUID runId = UUID.randomUUID();
        UUID taskId = UUID.randomUUID();
        synchronized (scheduler) { // keeps scheduler ticks out of this run while the race is staged
            jdbc.update("INSERT INTO workflow_runs(id, workflow_definition_id, status, started_at) VALUES (?,?, 'running', now())", runId, wf);
            jdbc.update("INSERT INTO task_runs(id, workflow_run_id, task_key, task_type, config, status, max_attempts) "
                    + "VALUES (?,?,'a','delay','{\"duration_ms\":50}'::jsonb,'ready',3)", taskId, runId);
            try (Connection claimer = dataSource.getConnection()) {
                claimer.setAutoCommit(false);
                // A dispatcher claim that is in flight (row locked, not yet committed) when the cancel arrives.
                try (PreparedStatement ps = claimer.prepareStatement(
                        "UPDATE task_runs SET status='running', attempts=attempts+1, started_at=now() WHERE id=? AND status='ready'")) {
                    ps.setObject(1, taskId);
                    assertThat(ps.executeUpdate()).isEqualTo(1);
                }
                CompletableFuture<ResponseEntity<JsonNode>> cancel =
                        CompletableFuture.supplyAsync(() -> post("/api/runs/" + runId + "/cancel", ""));
                await().atMost(Duration.ofSeconds(5)).until(() -> jdbc.queryForObject(
                        "SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock'", Integer.class) > 0);
                claimer.commit();
                assertThat(cancel.get(10, TimeUnit.SECONDS).getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
            }
        }
        assertThat(jdbc.queryForObject("SELECT status FROM task_runs WHERE id=?", String.class, taskId)).isEqualTo("cancelled");
    }

    @Test
    void claimIsRefusedOnceTheRunIsNoLongerRunning() {
        UUID wf = createWorkflow("""
                {"name":"it-claim-guard","tasks":[{"id":"a","type":"delay","config":{"duration_ms":1}}]}""");
        UUID runId = UUID.randomUUID();
        UUID taskId = UUID.randomUUID();
        jdbc.update("INSERT INTO workflow_runs(id, workflow_definition_id, status) VALUES (?,?, 'failed')", runId, wf);
        jdbc.update("INSERT INTO task_runs(id, workflow_run_id, task_key, task_type, config, status, max_attempts) "
                + "VALUES (?,?,'a','delay','{}'::jsonb,'ready',3)", taskId, runId);
        assertThat((Integer) tx.execute(st -> taskRuns.claim(taskId))).isZero();
        assertThat(jdbc.queryForObject("SELECT attempts FROM task_runs WHERE id=?", Integer.class, taskId)).isZero();
    }

    @Test
    void runHistoryPaginationHonoursArbitraryOffsets() {
        UUID wf = createWorkflow("""
                {"name":"it-pagination","tasks":[{"id":"a","type":"delay","config":{"duration_ms":1}}]}""");
        for (int i = 0; i < 5; i++) startRun(wf);
        List<String> all = ids(http.getForObject("/api/workflows/" + wf + "/runs?limit=10", JsonNode.class));
        assertThat(all).hasSize(5);
        assertThat(ids(http.getForObject("/api/workflows/" + wf + "/runs?limit=2&offset=1", JsonNode.class)))
                .containsExactly(all.get(1), all.get(2));
        assertThat(ids(http.getForObject("/api/runs?workflowId=" + wf + "&limit=3&offset=3", JsonNode.class)))
                .containsExactly(all.get(3), all.get(4));
    }

    @Test
    void httpTaskSucceedsOn2xxAndFailsOnErrorStatus() {
        UUID wf = createWorkflow("""
                {"name":"it-http","tasks":[
                  {"id":"ok","type":"http","config":{"status":200,"delay_ms":50}},
                  {"id":"err","type":"http","config":{"status":503},"max_attempts":1,"depends_on":["ok"]}]}""");
        UUID runId = startRun(wf);
        Map<String, JsonNode> t = tasksByKey(awaitStatus(runId, "failed", 15));
        assertThat(t.get("ok").get("status").asText()).isEqualTo("succeeded");
        assertThat(t.get("ok").get("result").get("http_status").asInt()).isEqualTo(200);
        assertThat(t.get("err").get("status").asText()).isEqualTo("failed");
        assertThat(t.get("err").get("last_error").asText()).contains("HTTP 503");
    }

    @Test
    void concurrentRunsStayWithinExecutorBoundWithoutDuplicateDispatch() {
        UUID wf = createWorkflow("""
                {"name":"it-concurrency","tasks":[
                  {"id":"a","type":"delay","config":{"duration_ms":150}},
                  {"id":"b","type":"delay","config":{"duration_ms":150}},
                  {"id":"c","type":"delay","config":{"duration_ms":150}},
                  {"id":"d","type":"math","config":{"operation":"max","values":[1,9]},"depends_on":["a","b","c"]}]}""");
        List<UUID> runIds = parallel(10, () -> startRun(wf));
        for (UUID runId : runIds) {
            Map<String, JsonNode> t = tasksByKey(awaitStatus(runId, "succeeded", 30));
            t.values().forEach(task -> assertThat(task.get("attempts").asInt()).isEqualTo(1));
            for (String key : t.keySet()) assertThat(eventTypes(runId, key)).filteredOn("task_started"::equals).hasSize(1);
        }
        JsonNode m = http.getForObject("/api/metrics", JsonNode.class);
        assertThat(m.get("peak_concurrency").asInt()).isBetween(2, m.get("max_concurrent_tasks").asInt());
    }

    @Test
    void concurrentCancelsAcceptExactlyOne() {
        UUID wf = createWorkflow("""
                {"name":"it-cancel-concurrent","tasks":[{"id":"long","type":"delay","config":{"duration_ms":3000}}]}""");
        UUID runId = startRun(wf);
        List<HttpStatusCode> codes = parallel(8, () -> post("/api/runs/" + runId + "/cancel", "").getStatusCode());
        assertThat(codes).filteredOn(HttpStatus.ACCEPTED::equals).hasSize(1);
        assertThat(codes).filteredOn(HttpStatus.CONFLICT::equals).hasSize(7);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM task_events WHERE workflow_run_id=? AND type='run_cancelled'",
                Integer.class, runId)).isEqualTo(1);
    }

    @Test
    void concurrentSavesOfOneNameNeverDuplicateAVersion() {
        String def = "{\"name\":\"it-concurrent-save\",\"tasks\":[{\"id\":\"a\",\"type\":\"delay\",\"config\":{\"duration_ms\":1}}]}";
        List<ResponseEntity<JsonNode>> res = parallel(10, () -> post("/api/workflows", def));
        assertThat(res).allMatch(r -> r.getStatusCode().equals(HttpStatus.CREATED) || r.getStatusCode().equals(HttpStatus.CONFLICT));
        List<Integer> versions = res.stream().filter(r -> r.getStatusCode().equals(HttpStatus.CREATED))
                .map(r -> r.getBody().get("version").asInt()).sorted().toList();
        assertThat(versions).isNotEmpty().doesNotHaveDuplicates();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM workflow_definitions WHERE name='it-concurrent-save'",
                Integer.class)).isEqualTo(versions.size());
    }

    @Test
    void readEndpointsAndRequestEdgeCases() {
        String def = "{\"name\":\"it-read\",\"tasks\":[{\"id\":\"a\",\"type\":\"transform\",\"config\":{\"operation\":\"reverse\",\"input\":\"abc\"}}]}";
        assertThat(post("/api/workflows/validate", def).getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM workflow_definitions WHERE name='it-read'", Integer.class)).isZero();

        UUID wf = createWorkflow(def);
        JsonNode detail = http.getForObject("/api/workflows/" + wf, JsonNode.class);
        assertThat(detail.get("definition").get("tasks").get(0).get("id").asText()).isEqualTo("a");
        JsonNode list = http.getForObject("/api/workflows", JsonNode.class);
        assertThat(StreamSupport.stream(list.spliterator(), false).anyMatch(w -> w.get("id").asText().equals(wf.toString()))).isTrue();

        UUID runId = startRun(wf);
        JsonNode r = awaitStatus(runId, "succeeded", 10);
        assertThat(r.get("tasks").get(0).get("result").get("value").asText()).isEqualTo("cba");
        assertThat(eventTypes(runId, null)).startsWith("run_created", "run_started").endsWith("run_succeeded");

        ResponseEntity<JsonNode> empty = post("/api/workflows", "");
        assertThat(empty.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(empty.getBody().get("error").asText()).isEqualTo("request body is required");
        assertThat(post("/api/workflows", "{\"name\":\"x\",\"pad\":\"" + "x".repeat(70_000) + "\"}").getStatusCode())
                .isEqualTo(HttpStatus.PAYLOAD_TOO_LARGE);
        assertThat(http.getForEntity("/api/runs/not-a-uuid", JsonNode.class).getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(post("/api/workflows/" + UUID.randomUUID() + "/runs", "").getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    private static List<String> ids(JsonNode arr) {
        return StreamSupport.stream(arr.spliterator(), false).map(n -> n.get("id").asText()).toList();
    }

    private static <T> List<T> parallel(int n, Callable<T> call) {
        ExecutorService pool = Executors.newFixedThreadPool(n);
        try {
            CountDownLatch go = new CountDownLatch(1);
            List<Future<T>> fs = new ArrayList<>();
            for (int i = 0; i < n; i++) fs.add(pool.submit(() -> { go.await(); return call.call(); }));
            go.countDown();
            List<T> out = new ArrayList<>();
            for (Future<T> f : fs) out.add(f.get(30, TimeUnit.SECONDS));
            return out;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            pool.shutdownNow();
        }
    }
}

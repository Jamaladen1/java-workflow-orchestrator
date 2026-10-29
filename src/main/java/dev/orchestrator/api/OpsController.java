package dev.orchestrator.api;

import dev.orchestrator.observability.OutcomeCounters;
import dev.orchestrator.persistence.TaskRunRepository;
import dev.orchestrator.persistence.WorkflowRunRepository;
import dev.orchestrator.scheduler.TaskDispatcher;
import dev.orchestrator.scheduler.WorkflowScheduler;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@RestController
public class OpsController {
    private final JdbcTemplate jdbc;
    private final WorkflowScheduler scheduler;
    private final TaskDispatcher dispatcher;
    private final WorkflowRunRepository runs;
    private final TaskRunRepository tasks;
    private final OutcomeCounters counters;

    public OpsController(JdbcTemplate jdbc, WorkflowScheduler scheduler, TaskDispatcher dispatcher,
                         WorkflowRunRepository runs, TaskRunRepository tasks, OutcomeCounters counters) {
        this.jdbc = jdbc;
        this.scheduler = scheduler;
        this.dispatcher = dispatcher;
        this.runs = runs;
        this.tasks = tasks;
        this.counters = counters;
    }

    @GetMapping("/health/live")
    public Map<String, String> live() { return Map.of("status", "ok"); }

    @GetMapping("/health/ready")
    public ResponseEntity<Map<String, Object>> ready() {
        try {
            jdbc.queryForObject("SELECT 1", Integer.class);
        } catch (Exception e) {
            return ResponseEntity.status(503).body(Map.of("status", "unavailable", "database", "down"));
        }
        if (!scheduler.isStarted()) {
            return ResponseEntity.status(503).body(Map.of("status", "starting", "database", "up"));
        }
        return ResponseEntity.ok(Map.of("status", "ok", "database", "up", "scheduler", "running"));
    }

    @GetMapping("/api/metrics")
    public Map<String, Object> metrics() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("mode", "single-node");
        out.put("max_concurrent_tasks", dispatcher.maxConcurrent());
        out.put("in_flight_tasks", dispatcher.inFlight());
        out.put("peak_concurrency", dispatcher.peakConcurrency());
        out.put("runs_by_status", toMap(runs.countByStatus()));
        out.put("tasks_by_status", toMap(tasks.countByStatus()));
        out.put("counters_since_start", counters.snapshot());
        return out;
    }

    /** Internal mock endpoint used by the 'http' task type. */
    @GetMapping("/internal/mock/{status}")
    public ResponseEntity<Map<String, Object>> mock(@PathVariable int status,
                                                    @RequestParam(name = "delay_ms", defaultValue = "0") long delayMs)
            throws InterruptedException {
        if (status < 100 || status > 599) status = 400;
        Thread.sleep(Math.max(0, Math.min(delayMs, 10_000)));
        return ResponseEntity.status(status).body(Map.of("mock", true, "status", status));
    }

    private static Map<String, Long> toMap(List<Object[]> rows) {
        Map<String, Long> m = new TreeMap<>();
        for (Object[] r : rows) m.put((String) r[0], ((Number) r[1]).longValue());
        return m;
    }
}

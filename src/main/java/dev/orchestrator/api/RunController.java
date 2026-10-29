package dev.orchestrator.api;

import dev.orchestrator.api.Dtos.*;
import dev.orchestrator.scheduler.RunStateService;
import dev.orchestrator.scheduler.TaskDispatcher;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/runs")
public class RunController {
    private final RunQueryService queries;
    private final RunStateService state;
    private final TaskDispatcher dispatcher;

    public RunController(RunQueryService queries, RunStateService state, TaskDispatcher dispatcher) {
        this.queries = queries;
        this.state = state;
        this.dispatcher = dispatcher;
    }

    @GetMapping
    public List<RunSummary> list(@RequestParam(required = false) UUID workflowId,
                                 @RequestParam(defaultValue = "20") int limit,
                                 @RequestParam(defaultValue = "0") int offset) {
        return queries.listRuns(workflowId, limit, offset);
    }

    @GetMapping("/{runId}")
    public RunDetail get(@PathVariable UUID runId) {
        return queries.getRun(runId).orElseThrow(() -> new ApiException(HttpStatus.NOT_FOUND, "run not found"));
    }

    @GetMapping("/{runId}/events")
    public List<EventView> events(@PathVariable UUID runId) {
        get(runId);
        return queries.events(runId);
    }

    @PostMapping("/{runId}/cancel")
    public ResponseEntity<Map<String, Object>> cancel(@PathVariable UUID runId) {
        RunStateService.Cancellation c = state.cancelRun(runId);
        return switch (c.result()) {
            case NOT_FOUND -> throw new ApiException(HttpStatus.NOT_FOUND, "run not found");
            case ALREADY_TERMINAL -> throw new ApiException(HttpStatus.CONFLICT, "run is already in a terminal state");
            case CANCELLED -> {
                dispatcher.interrupt(c.runningTaskIds()); // after the cancel transaction committed
                yield ResponseEntity.accepted().body(Map.of("run_id", runId, "status", "cancelled",
                        "interrupted_tasks", c.runningTaskIds().size()));
            }
        };
    }
}

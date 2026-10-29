package dev.orchestrator.scheduler;

import com.fasterxml.jackson.databind.JsonNode;
import dev.orchestrator.config.OrchestratorProperties;
import dev.orchestrator.domain.RetryPolicy;
import dev.orchestrator.domain.RunStatus;
import dev.orchestrator.domain.TaskDefinition;
import dev.orchestrator.domain.TaskStatus;
import dev.orchestrator.domain.WorkflowDefinition;
import dev.orchestrator.observability.OutcomeCounters;
import dev.orchestrator.persistence.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;

/**
 * Every workflow/task state transition lives here, one transaction per transition, persisted as a
 * compare-and-set update plus an event row. In-memory futures are never the source of truth.
 */
@Service
public class RunStateService {
    private static final Logger log = LoggerFactory.getLogger(RunStateService.class);

    private final WorkflowRunRepository runs;
    private final TaskRunRepository tasks;
    private final EventRecorder events;
    private final OutcomeCounters counters;
    private final RetryPolicy retryPolicy;

    public RunStateService(WorkflowRunRepository runs, TaskRunRepository tasks, EventRecorder events,
                           OutcomeCounters counters, OrchestratorProperties props) {
        this.runs = runs;
        this.tasks = tasks;
        this.events = events;
        this.counters = counters;
        this.retryPolicy = new RetryPolicy(props.retryBaseMs(), props.retryMaxMs());
    }

    public RetryPolicy retryPolicy() { return retryPolicy; }

    @Transactional
    public UUID createRun(UUID definitionId, WorkflowDefinition def) {
        UUID runId = UUID.randomUUID();
        runs.save(new WorkflowRunEntity(runId, definitionId, RunStatus.PENDING.db()));
        runs.flush();
        for (TaskDefinition t : def.tasks()) {
            tasks.save(new TaskRunEntity(UUID.randomUUID(), runId, t.id(), t.type(), t.config(), t.dependsOn(), t.maxAttempts()));
        }
        tasks.flush();
        events.record(runId, null, "run_created", Map.of("workflow", def.name(), "tasks", def.tasks().size()));
        counters.inc("runs_created");
        return runId;
    }

    /** Re-evaluates one run: start it, promote ready tasks, apply fail-fast, or finish it. */
    @Transactional
    public void evaluateRun(UUID runId) {
        RunStatus status = runs.lockStatus(runId).map(RunStatus::of).orElse(null);
        if (status == null || status.isTerminal()) return;
        if (status == RunStatus.PENDING && runs.markRunning(runId) == 1) {
            events.record(runId, null, "run_started", Map.of());
        }
        List<TaskRunEntity> taskRows = tasks.findByWorkflowRunIdOrderByTaskKey(runId);
        Map<String, TaskRunEntity> byKey = new HashMap<>();
        List<ReadyTaskResolver.TaskState> states = new ArrayList<>();
        for (TaskRunEntity t : taskRows) {
            byKey.put(t.getTaskKey(), t);
            states.add(new ReadyTaskResolver.TaskState(t.getTaskKey(), TaskStatus.of(t.getStatus()), t.getDependsOn()));
        }
        ReadyTaskResolver.Decision d = ReadyTaskResolver.evaluate(states);
        for (String key : d.promote()) {
            TaskRunEntity t = byKey.get(key);
            if (tasks.markReady(t.getId()) == 1) events.record(runId, t.getId(), "task_ready", Map.of("task", key));
        }
        d.skip().forEach((key, reason) -> {
            TaskRunEntity t = byKey.get(key);
            if (tasks.markNotStarted(t.getId(), TaskStatus.SKIPPED.db(), reason) == 1) {
                events.record(runId, t.getId(), "task_skipped", Map.of("task", key, "reason", reason));
                counters.inc("tasks_skipped");
            }
        });
        if (d.outcome() == ReadyTaskResolver.Outcome.SUCCEEDED && runs.finish(runId, RunStatus.SUCCEEDED.db()) == 1) {
            events.record(runId, null, "run_succeeded", Map.of());
            counters.inc("runs_succeeded");
            log.info("workflow run succeeded run_id={}", runId);
        } else if (d.outcome() == ReadyTaskResolver.Outcome.FAILED && runs.finish(runId, RunStatus.FAILED.db()) == 1) {
            events.record(runId, null, "run_failed", Map.of("skipped", d.skip().size()));
            counters.inc("runs_failed");
            log.info("workflow run failed run_id={}", runId);
        }
    }

    /** ready -> running (CAS). Returns the claimed row, or empty if another tick/thread got there first. */
    @Transactional
    public Optional<TaskRunEntity> claim(UUID taskRunId) {
        if (tasks.claim(taskRunId) != 1) return Optional.empty();
        TaskRunEntity t = tasks.findById(taskRunId).orElseThrow();
        events.record(t.getWorkflowRunId(), t.getId(), "task_started",
                Map.of("task", t.getTaskKey(), "attempt", t.getAttempts(), "max_attempts", t.getMaxAttempts()));
        return Optional.of(t);
    }

    @Transactional
    public boolean recordSuccess(TaskRunEntity t, JsonNode result, long durationMs) {
        if (tasks.completeSuccess(t.getId(), result.toString()) == 1) {
            events.record(t.getWorkflowRunId(), t.getId(), "task_succeeded",
                    Map.of("task", t.getTaskKey(), "attempt", t.getAttempts(), "duration_ms", durationMs, "result", result));
            counters.inc("tasks_succeeded");
            return true;
        }
        lateCompletion(t, "succeeded");
        return false;
    }

    /**
     * running -> ready (retry with backoff) or running -> failed when attempts are exhausted. A run that has already
     * finished (fail-fast) never gets a retry: nothing would dispatch it, so the task fails instead.
     */
    @Transactional
    public boolean recordFailure(TaskRunEntity t, String error, long durationMs) {
        String err = error == null ? "unknown error" : (error.length() > 2000 ? error.substring(0, 2000) : error);
        RunStatus runStatus = runs.lockStatus(t.getWorkflowRunId()).map(RunStatus::of).orElse(RunStatus.FAILED);
        if (runStatus.isTerminal() && retryPolicy.canRetry(t.getAttempts(), t.getMaxAttempts())) {
            err = err + " (not retried: run already " + runStatus.db() + ")";
        }
        if (!runStatus.isTerminal() && retryPolicy.canRetry(t.getAttempts(), t.getMaxAttempts())) {
            long backoff = retryPolicy.backoffMs(t.getAttempts());
            if (tasks.scheduleRetry(t.getId(), err, backoff) == 1) {
                events.record(t.getWorkflowRunId(), t.getId(), "task_retry_scheduled",
                        Map.of("task", t.getTaskKey(), "attempt", t.getAttempts(), "error", err,
                                "backoff_ms", backoff, "duration_ms", durationMs));
                counters.inc("tasks_retried");
                return true;
            }
        } else if (tasks.failTerminal(t.getId(), err) == 1) {
            events.record(t.getWorkflowRunId(), t.getId(), "task_failed",
                    Map.of("task", t.getTaskKey(), "attempt", t.getAttempts(), "error", err, "duration_ms", durationMs));
            counters.inc("tasks_failed");
            return true;
        }
        lateCompletion(t, "failed");
        return false;
    }

    private void lateCompletion(TaskRunEntity t, String outcome) {
        // The row is no longer 'running' (e.g. cancelled, or recovered after restart): CAS refused the write.
        events.record(t.getWorkflowRunId(), t.getId(), "late_completion_ignored",
                Map.of("task", t.getTaskKey(), "attempt", t.getAttempts(), "outcome", outcome));
        counters.inc("late_completions_ignored");
    }

    public enum CancelResult { CANCELLED, NOT_FOUND, ALREADY_TERMINAL }

    public record Cancellation(CancelResult result, List<UUID> runningTaskIds) {}

    @Transactional
    public Cancellation cancelRun(UUID runId) {
        WorkflowRunEntity run = runs.findById(runId).orElse(null);
        if (run == null) return new Cancellation(CancelResult.NOT_FOUND, List.of());
        if (runs.finish(runId, RunStatus.CANCELLED.db()) != 1) return new Cancellation(CancelResult.ALREADY_TERMINAL, List.of());
        List<UUID> running = new ArrayList<>();
        for (TaskRunEntity t : tasks.findByWorkflowRunIdOrderByTaskKey(runId)) {
            // The snapshot may be stale (a claim can commit ready->running after it was read), so let each CAS
            // decide instead of branching on the status read above.
            if (TaskStatus.of(t.getStatus()).isTerminal()) continue;
            if (tasks.markNotStarted(t.getId(), TaskStatus.CANCELLED.db(), "run cancelled") == 1) {
                events.record(runId, t.getId(), "task_cancelled", Map.of("task", t.getTaskKey()));
            } else if (tasks.cancelRunning(t.getId(), "run cancelled while running (interrupt requested)") == 1) {
                running.add(t.getId());
                events.record(runId, t.getId(), "task_cancelled",
                        Map.of("task", t.getTaskKey(), "interrupt_requested", true));
            }
        }
        events.record(runId, null, "run_cancelled", Map.of("interrupted", running.size()));
        counters.inc("runs_cancelled");
        return new Cancellation(CancelResult.CANCELLED, running);
    }

    /**
     * Restart recovery for a task left 'running' with no live executor thread: retry it after backoff if
     * attempts remain, otherwise fail it terminally (fail-fast then applies on the next tick).
     */
    @Transactional
    public boolean recoverOrphan(TaskRunEntity t) {
        RunStatus runStatus = runs.lockStatus(t.getWorkflowRunId()).map(RunStatus::of).orElse(RunStatus.FAILED);
        boolean recovered;
        if (runStatus.isTerminal()) {
            recovered = tasks.cancelRunning(t.getId(), "orphaned task (no live worker); run already " + runStatus.db()) == 1;
            if (recovered) {
                events.record(t.getWorkflowRunId(), t.getId(), "task_recovered",
                        Map.of("task", t.getTaskKey(), "action", "cancelled"));
            }
        } else {
            String reason = "task orphaned while running (attempt " + t.getAttempts()
                    + "): orchestrator restarted or its outcome could not be saved";
            if (retryPolicy.canRetry(t.getAttempts(), t.getMaxAttempts())) {
                long backoff = retryPolicy.backoffMs(t.getAttempts());
                recovered = tasks.scheduleRetry(t.getId(), reason, backoff) == 1;
                if (recovered) {
                    events.record(t.getWorkflowRunId(), t.getId(), "task_recovered",
                            Map.of("task", t.getTaskKey(), "action", "retry", "backoff_ms", backoff));
                }
            } else {
                recovered = tasks.failTerminal(t.getId(), reason + "; attempts exhausted") == 1;
                if (recovered) {
                    events.record(t.getWorkflowRunId(), t.getId(), "task_recovered",
                            Map.of("task", t.getTaskKey(), "action", "failed"));
                }
            }
        }
        if (recovered) counters.inc("tasks_recovered");
        return recovered;
    }
}

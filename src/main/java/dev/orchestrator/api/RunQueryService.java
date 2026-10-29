package dev.orchestrator.api;

import dev.orchestrator.api.Dtos.*;
import dev.orchestrator.persistence.*;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
@Transactional(readOnly = true)
public class RunQueryService {
    private final WorkflowRunRepository runs;
    private final TaskRunRepository tasks;
    private final TaskEventRepository events;
    private final WorkflowDefinitionRepository definitions;

    public RunQueryService(WorkflowRunRepository runs, TaskRunRepository tasks, TaskEventRepository events,
                           WorkflowDefinitionRepository definitions) {
        this.runs = runs;
        this.tasks = tasks;
        this.events = events;
        this.definitions = definitions;
    }

    public List<RunSummary> listRuns(UUID definitionId, int limit, int offset) {
        int size = Math.max(1, Math.min(limit, 100));
        int page = Math.max(0, offset) / size;
        List<WorkflowRunEntity> rows = definitionId == null
                ? runs.findAllByOrderByCreatedAtDesc(PageRequest.of(page, size))
                : runs.findByWorkflowDefinitionIdOrderByCreatedAtDesc(definitionId, PageRequest.of(page, size));
        Map<UUID, WorkflowDefinitionEntity> defs = definitions.findAllById(
                rows.stream().map(WorkflowRunEntity::getWorkflowDefinitionId).collect(Collectors.toSet()))
                .stream().collect(Collectors.toMap(WorkflowDefinitionEntity::getId, Function.identity()));
        return rows.stream().map(r -> summary(r, defs.get(r.getWorkflowDefinitionId()))).toList();
    }

    public Optional<RunDetail> getRun(UUID runId) {
        return runs.findById(runId).map(r -> {
            WorkflowDefinitionEntity d = definitions.findById(r.getWorkflowDefinitionId()).orElse(null);
            List<TaskView> taskViews = tasks.findByWorkflowRunIdOrderByTaskKey(runId).stream()
                    .map(t -> new TaskView(t.getId(), t.getTaskKey(), t.getTaskType(), t.getDependsOn(), t.getStatus(),
                            t.getAttempts(), t.getMaxAttempts(), t.getConfig(), t.getResult(), t.getLastError(),
                            t.getStartedAt(), t.getFinishedAt(), t.getNextRunAt(),
                            duration(t.getStartedAt(), t.getFinishedAt(), "running".equals(t.getStatus()))))
                    .toList();
            RunSummary s = summary(r, d);
            return new RunDetail(s.id(), s.workflowDefinitionId(), s.workflowName(), s.workflowVersion(), s.status(),
                    s.createdAt(), s.startedAt(), s.finishedAt(), s.durationMs(), taskViews);
        });
    }

    public List<EventView> events(UUID runId) {
        Map<UUID, String> keys = tasks.findByWorkflowRunIdOrderByTaskKey(runId).stream()
                .collect(Collectors.toMap(TaskRunEntity::getId, TaskRunEntity::getTaskKey));
        return events.findByWorkflowRunIdOrderByIdAsc(runId).stream()
                .map(e -> new EventView(e.getId(), e.getTaskRunId(), e.getTaskRunId() == null ? null : keys.get(e.getTaskRunId()),
                        e.getType(), e.getDetail(), e.getCreatedAt()))
                .toList();
    }

    private static RunSummary summary(WorkflowRunEntity r, WorkflowDefinitionEntity d) {
        boolean active = "running".equals(r.getStatus()) || "pending".equals(r.getStatus());
        return new RunSummary(r.getId(), r.getWorkflowDefinitionId(), d == null ? null : d.getName(),
                d == null ? 0 : d.getVersion(), r.getStatus(), r.getCreatedAt(), r.getStartedAt(), r.getFinishedAt(),
                duration(r.getStartedAt(), r.getFinishedAt(), active));
    }

    private static Long duration(OffsetDateTime start, OffsetDateTime end, boolean live) {
        if (start == null) return null;
        if (end == null) return live ? Duration.between(start, OffsetDateTime.now()).toMillis() : null;
        return Duration.between(start, end).toMillis();
    }
}

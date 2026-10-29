package dev.orchestrator.api;

import com.fasterxml.jackson.databind.JsonNode;
import dev.orchestrator.validation.ValidationError;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

public final class Dtos {
    private Dtos() {}

    public record WorkflowCreated(UUID id, String name, int version) {}

    public record ValidationFailure(String error, List<ValidationError> errors) {}

    public record WorkflowSummary(UUID id, String name, int version, int taskCount, OffsetDateTime createdAt) {}

    public record WorkflowDetail(UUID id, String name, int version, OffsetDateTime createdAt, JsonNode definition) {}

    public record RunCreated(UUID runId, String status) {}

    public record RunSummary(UUID id, UUID workflowDefinitionId, String workflowName, int workflowVersion, String status,
                             OffsetDateTime createdAt, OffsetDateTime startedAt, OffsetDateTime finishedAt, Long durationMs) {}

    public record TaskView(UUID id, String taskKey, String type, List<String> dependsOn, String status, int attempts,
                           int maxAttempts, JsonNode config, JsonNode result, String lastError, OffsetDateTime startedAt,
                           OffsetDateTime finishedAt, OffsetDateTime nextRunAt, Long durationMs) {}

    public record RunDetail(UUID id, UUID workflowDefinitionId, String workflowName, int workflowVersion, String status,
                            OffsetDateTime createdAt, OffsetDateTime startedAt, OffsetDateTime finishedAt, Long durationMs,
                            List<TaskView> tasks) {}

    public record EventView(long id, UUID taskRunId, String taskKey, String type, JsonNode detail, OffsetDateTime createdAt) {}
}

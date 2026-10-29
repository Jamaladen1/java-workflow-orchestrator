package dev.orchestrator.persistence;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.persistence.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

/**
 * Task state is only ever changed through compare-and-set native updates in {@link TaskRunRepository};
 * the mutable columns are therefore read-only from JPA's point of view.
 */
@Entity
@Table(name = "task_runs")
public class TaskRunEntity {
    @Id private UUID id;
    @Column(name = "workflow_run_id", nullable = false) private UUID workflowRunId;
    @Column(name = "task_key", nullable = false) private String taskKey;
    @Column(name = "task_type", nullable = false) private String taskType;
    @JdbcTypeCode(SqlTypes.JSON) @Column(nullable = false, columnDefinition = "jsonb") private JsonNode config;
    @JdbcTypeCode(SqlTypes.JSON) @Column(name = "depends_on", nullable = false, columnDefinition = "jsonb") private List<String> dependsOn;
    @Column(nullable = false, updatable = false) private String status;
    @Column(name = "max_attempts", nullable = false) private int maxAttempts;
    @Column(insertable = false, updatable = false) private int attempts;
    @JdbcTypeCode(SqlTypes.JSON) @Column(insertable = false, updatable = false, columnDefinition = "jsonb") private JsonNode result;
    @Column(name = "last_error", insertable = false, updatable = false) private String lastError;
    @Column(name = "started_at", insertable = false, updatable = false) private OffsetDateTime startedAt;
    @Column(name = "finished_at", insertable = false, updatable = false) private OffsetDateTime finishedAt;
    @Column(name = "next_run_at", insertable = false, updatable = false) private OffsetDateTime nextRunAt;

    protected TaskRunEntity() {}

    public TaskRunEntity(UUID id, UUID workflowRunId, String taskKey, String taskType, JsonNode config,
                         List<String> dependsOn, int maxAttempts) {
        this.id = id; this.workflowRunId = workflowRunId; this.taskKey = taskKey; this.taskType = taskType;
        this.config = config; this.dependsOn = dependsOn; this.maxAttempts = maxAttempts; this.status = "pending";
    }

    public UUID getId() { return id; }
    public UUID getWorkflowRunId() { return workflowRunId; }
    public String getTaskKey() { return taskKey; }
    public String getTaskType() { return taskType; }
    public JsonNode getConfig() { return config; }
    public List<String> getDependsOn() { return dependsOn; }
    public String getStatus() { return status; }
    public int getAttempts() { return attempts; }
    public int getMaxAttempts() { return maxAttempts; }
    public JsonNode getResult() { return result; }
    public String getLastError() { return lastError; }
    public OffsetDateTime getStartedAt() { return startedAt; }
    public OffsetDateTime getFinishedAt() { return finishedAt; }
    public OffsetDateTime getNextRunAt() { return nextRunAt; }
}

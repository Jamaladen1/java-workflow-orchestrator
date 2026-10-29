package dev.orchestrator.persistence;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.persistence.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.OffsetDateTime;
import java.util.UUID;

@Entity
@Table(name = "task_events")
public class TaskEventEntity {
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY) private Long id;
    @Column(name = "workflow_run_id", nullable = false) private UUID workflowRunId;
    @Column(name = "task_run_id") private UUID taskRunId;
    @Column(nullable = false) private String type;
    @JdbcTypeCode(SqlTypes.JSON) @Column(columnDefinition = "jsonb") private JsonNode detail;
    @Column(name = "created_at", insertable = false, updatable = false) private OffsetDateTime createdAt;

    protected TaskEventEntity() {}

    public TaskEventEntity(UUID workflowRunId, UUID taskRunId, String type, JsonNode detail) {
        this.workflowRunId = workflowRunId; this.taskRunId = taskRunId; this.type = type; this.detail = detail;
    }

    public Long getId() { return id; }
    public UUID getWorkflowRunId() { return workflowRunId; }
    public UUID getTaskRunId() { return taskRunId; }
    public String getType() { return type; }
    public JsonNode getDetail() { return detail; }
    public OffsetDateTime getCreatedAt() { return createdAt; }
}

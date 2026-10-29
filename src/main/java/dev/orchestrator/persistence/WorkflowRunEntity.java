package dev.orchestrator.persistence;

import jakarta.persistence.*;

import java.time.OffsetDateTime;
import java.util.UUID;

@Entity
@Table(name = "workflow_runs")
public class WorkflowRunEntity {
    @Id private UUID id;
    @Column(name = "workflow_definition_id", nullable = false) private UUID workflowDefinitionId;
    @Column(nullable = false, updatable = false) private String status;
    @Column(name = "created_at", insertable = false, updatable = false) private OffsetDateTime createdAt;
    @Column(name = "started_at", insertable = false, updatable = false) private OffsetDateTime startedAt;
    @Column(name = "finished_at", insertable = false, updatable = false) private OffsetDateTime finishedAt;

    protected WorkflowRunEntity() {}

    public WorkflowRunEntity(UUID id, UUID workflowDefinitionId, String status) {
        this.id = id; this.workflowDefinitionId = workflowDefinitionId; this.status = status;
    }

    public UUID getId() { return id; }
    public UUID getWorkflowDefinitionId() { return workflowDefinitionId; }
    public String getStatus() { return status; }
    public OffsetDateTime getCreatedAt() { return createdAt; }
    public OffsetDateTime getStartedAt() { return startedAt; }
    public OffsetDateTime getFinishedAt() { return finishedAt; }
}

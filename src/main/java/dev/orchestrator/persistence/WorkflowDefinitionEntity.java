package dev.orchestrator.persistence;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.persistence.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.OffsetDateTime;
import java.util.UUID;

@Entity
@Table(name = "workflow_definitions")
public class WorkflowDefinitionEntity {
    @Id private UUID id;
    @Column(nullable = false) private String name;
    @Column(nullable = false) private int version;
    @JdbcTypeCode(SqlTypes.JSON) @Column(nullable = false, columnDefinition = "jsonb") private JsonNode definition;
    @Column(name = "created_at", nullable = false, insertable = false, updatable = false) private OffsetDateTime createdAt;

    protected WorkflowDefinitionEntity() {}

    public WorkflowDefinitionEntity(UUID id, String name, int version, JsonNode definition) {
        this.id = id; this.name = name; this.version = version; this.definition = definition;
    }

    public UUID getId() { return id; }
    public String getName() { return name; }
    public int getVersion() { return version; }
    public JsonNode getDefinition() { return definition; }
    public OffsetDateTime getCreatedAt() { return createdAt; }
}

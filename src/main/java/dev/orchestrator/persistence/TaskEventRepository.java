package dev.orchestrator.persistence;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface TaskEventRepository extends JpaRepository<TaskEventEntity, Long> {
    List<TaskEventEntity> findByWorkflowRunIdOrderByIdAsc(UUID runId);
}

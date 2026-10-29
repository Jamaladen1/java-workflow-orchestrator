package dev.orchestrator.persistence;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;

import java.util.List;
import java.util.UUID;

public interface WorkflowRunRepository extends JpaRepository<WorkflowRunEntity, UUID> {

    @Query("select r.id from WorkflowRunEntity r where r.status in ('pending','running') order by r.createdAt")
    List<UUID> findActiveRunIds();

    List<WorkflowRunEntity> findAllByOrderByCreatedAtDesc(Pageable page);

    List<WorkflowRunEntity> findByWorkflowDefinitionIdOrderByCreatedAtDesc(UUID definitionId, Pageable page);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = "UPDATE workflow_runs SET status='running', started_at=now() WHERE id=:id AND status='pending'",
            nativeQuery = true)
    int markRunning(UUID id);

    /** CAS to a terminal state; only succeeds while the run is still active. */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = "UPDATE workflow_runs SET status=:status, finished_at=now(), started_at=coalesce(started_at, now()) "
            + "WHERE id=:id AND status IN ('pending','running')", nativeQuery = true)
    int finish(UUID id, String status);

    @Query(value = "SELECT status, count(*) FROM workflow_runs GROUP BY status", nativeQuery = true)
    List<Object[]> countByStatus();
}

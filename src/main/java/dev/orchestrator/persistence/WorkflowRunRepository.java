package dev.orchestrator.persistence;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface WorkflowRunRepository extends JpaRepository<WorkflowRunEntity, UUID> {

    @Query("select r.id from WorkflowRunEntity r where r.status in ('pending','running') order by r.createdAt")
    List<UUID> findActiveRunIds();

    @Query(value = "SELECT * FROM workflow_runs ORDER BY created_at DESC, id LIMIT :limit OFFSET :offset", nativeQuery = true)
    List<WorkflowRunEntity> page(int limit, int offset);

    @Query(value = "SELECT * FROM workflow_runs WHERE workflow_definition_id = :definitionId "
            + "ORDER BY created_at DESC, id LIMIT :limit OFFSET :offset", nativeQuery = true)
    List<WorkflowRunEntity> pageByDefinition(UUID definitionId, int limit, int offset);

    /**
     * Locks the run row for the rest of the transaction. Taken before deciding a run's outcome or a task's retry,
     * so a retry can't be scheduled into a run that fail-fast is concurrently finishing.
     */
    @Query(value = "SELECT status FROM workflow_runs WHERE id = :id FOR UPDATE", nativeQuery = true)
    Optional<String> lockStatus(UUID id);

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

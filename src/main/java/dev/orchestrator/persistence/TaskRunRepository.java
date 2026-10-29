package dev.orchestrator.persistence;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;

import java.util.List;
import java.util.UUID;

/**
 * All task state transitions are compare-and-set updates: they name the expected current status in the
 * WHERE clause and return the number of rows changed (0 = someone else already moved it).
 * Database time (now()) is authoritative for all timestamps.
 */
public interface TaskRunRepository extends JpaRepository<TaskRunEntity, UUID> {

    List<TaskRunEntity> findByWorkflowRunIdOrderByTaskKey(UUID runId);

    /** Ready tasks whose backoff has elapsed, in runs that are executing, oldest first. */
    @Query(value = "SELECT t.id FROM task_runs t JOIN workflow_runs r ON r.id = t.workflow_run_id "
            + "WHERE t.status='ready' AND r.status='running' AND (t.next_run_at IS NULL OR t.next_run_at <= now()) "
            + "ORDER BY coalesce(t.next_run_at, r.created_at), t.task_key LIMIT :limit", nativeQuery = true)
    List<UUID> findDispatchable(int limit);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = "UPDATE task_runs SET status='ready' WHERE id=:id AND status='pending'", nativeQuery = true)
    int markReady(UUID id);

    /** ready -> running. Increments attempts on dispatch. */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = "UPDATE task_runs SET status='running', attempts=attempts+1, started_at=now(), finished_at=NULL "
            + "WHERE id=:id AND status='ready' AND (next_run_at IS NULL OR next_run_at <= now())", nativeQuery = true)
    int claim(UUID id);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = "UPDATE task_runs SET status='succeeded', result=cast(:result as jsonb), last_error=NULL, "
            + "finished_at=now(), next_run_at=NULL WHERE id=:id AND status='running'", nativeQuery = true)
    int completeSuccess(UUID id, String result);

    /** running -> ready with next_run_at in the future (retry after backoff). */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = "UPDATE task_runs SET status='ready', last_error=:error, finished_at=now(), "
            + "next_run_at=now() + make_interval(secs => :backoffMs / 1000.0) WHERE id=:id AND status='running'",
            nativeQuery = true)
    int scheduleRetry(UUID id, String error, long backoffMs);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = "UPDATE task_runs SET status='failed', last_error=:error, finished_at=now(), next_run_at=NULL "
            + "WHERE id=:id AND status='running'", nativeQuery = true)
    int failTerminal(UUID id, String error);

    /** Fail-fast / cancellation of tasks that have not started. */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = "UPDATE task_runs SET status=:status, last_error=:reason, finished_at=now(), next_run_at=NULL "
            + "WHERE id=:id AND status IN ('pending','ready')", nativeQuery = true)
    int markNotStarted(UUID id, String status, String reason);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = "UPDATE task_runs SET status='cancelled', last_error=:reason, finished_at=now() "
            + "WHERE id=:id AND status='running'", nativeQuery = true)
    int cancelRunning(UUID id, String reason);

    @Query(value = "SELECT t.* FROM task_runs t WHERE t.status='running'", nativeQuery = true)
    List<TaskRunEntity> findAllRunning();

    @Query(value = "SELECT status, count(*) FROM task_runs GROUP BY status", nativeQuery = true)
    List<Object[]> countByStatus();
}

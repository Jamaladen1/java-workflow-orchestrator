package dev.orchestrator.scheduler;

import dev.orchestrator.config.OrchestratorProperties;
import dev.orchestrator.persistence.TaskRunEntity;
import dev.orchestrator.persistence.TaskRunRepository;
import dev.orchestrator.persistence.WorkflowRunRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Single-node scheduler loop. Every tick: re-evaluate active runs (promote / fail-fast / finish), then dispatch
 * ready tasks into free executor slots. Repeated or overlapping ticks are safe because every transition is CAS.
 */
@Component
public class WorkflowScheduler {
    private static final Logger log = LoggerFactory.getLogger(WorkflowScheduler.class);

    private final WorkflowRunRepository runs;
    private final TaskRunRepository tasks;
    private final RunStateService state;
    private final TaskDispatcher dispatcher;
    private final long orphanSweepIntervalMs;
    private volatile boolean started;
    private long lastOrphanSweep;

    public WorkflowScheduler(WorkflowRunRepository runs, TaskRunRepository tasks, RunStateService state,
                             TaskDispatcher dispatcher, OrchestratorProperties props) {
        this.runs = runs;
        this.tasks = tasks;
        this.state = state;
        this.dispatcher = dispatcher;
        this.orphanSweepIntervalMs = props.orphanSweepIntervalMs();
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onStartup() {
        int recovered = recoverOrphanedTasks();
        log.info("scheduler starting; recovered {} orphaned running task(s)", recovered);
        started = true;
    }

    /**
     * Any 'running' row with no live executor thread in this JVM is an orphan: left by a previous process, or by a
     * worker that could not persist its outcome (e.g. during a database outage). Runs under the tick lock, so a task
     * is never seen between its claim and its registration with the dispatcher.
     */
    public synchronized int recoverOrphanedTasks() {
        lastOrphanSweep = System.currentTimeMillis();
        int n = 0;
        for (TaskRunEntity t : tasks.findAllRunning()) {
            if (dispatcher.isRunningLocally(t.getId())) continue;
            if (state.recoverOrphan(t)) n++;
        }
        return n;
    }

    @Scheduled(fixedDelayString = "${orchestrator.tick-interval-ms:200}")
    public synchronized void tick() {
        if (!started) return;
        for (UUID runId : runs.findActiveRunIds()) {
            try {
                state.evaluateRun(runId);
            } catch (Exception e) {
                log.warn("failed to evaluate run {}: {}", runId, e.toString());
            }
        }
        if (System.currentTimeMillis() - lastOrphanSweep >= orphanSweepIntervalMs) {
            try {
                int n = recoverOrphanedTasks();
                if (n > 0) log.warn("recovered {} orphaned running task(s) with no live worker", n);
            } catch (Exception e) {
                log.warn("orphan sweep failed: {}", e.toString());
            }
        }
        try {
            dispatcher.dispatchReady();
        } catch (Exception e) {
            log.warn("dispatch failed: {}", e.toString());
        }
    }

    public boolean isStarted() { return started; }
}

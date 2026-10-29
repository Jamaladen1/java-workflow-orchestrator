package dev.orchestrator.scheduler;

import com.fasterxml.jackson.databind.JsonNode;
import dev.orchestrator.config.OrchestratorProperties;
import dev.orchestrator.execution.HandlerRegistry;
import dev.orchestrator.execution.TaskContext;
import dev.orchestrator.execution.TaskFailure;
import dev.orchestrator.execution.TaskHandler;
import dev.orchestrator.persistence.TaskRunEntity;
import dev.orchestrator.persistence.TaskRunRepository;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Bounded executor (MAX_CONCURRENT_TASKS threads). Only claims as many ready tasks as there are free slots,
 * so the queue never grows. Each claim is a CAS ready->running, so a task is dispatched at most once at a time.
 */
@Component
public class TaskDispatcher {
    private static final Logger log = LoggerFactory.getLogger(TaskDispatcher.class);

    private final TaskRunRepository tasks;
    private final RunStateService state;
    private final HandlerRegistry handlers;
    private final int maxConcurrent;
    private final ThreadPoolExecutor executor;
    private final AtomicInteger inFlight = new AtomicInteger();
    private final Map<UUID, Future<?>> running = new ConcurrentHashMap<>();
    private final AtomicInteger peakConcurrency = new AtomicInteger();

    public TaskDispatcher(TaskRunRepository tasks, RunStateService state, HandlerRegistry handlers,
                          OrchestratorProperties props) {
        this.tasks = tasks;
        this.state = state;
        this.handlers = handlers;
        this.maxConcurrent = props.maxConcurrentTasks();
        AtomicInteger n = new AtomicInteger();
        this.executor = new ThreadPoolExecutor(maxConcurrent, maxConcurrent, 0, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(), r -> {
            Thread t = new Thread(r, "task-worker-" + n.incrementAndGet());
            t.setDaemon(true);
            return t;
        });
    }

    public int inFlight() { return inFlight.get(); }
    public int maxConcurrent() { return maxConcurrent; }
    public int peakConcurrency() { return peakConcurrency.get(); }
    public boolean isRunningLocally(UUID taskRunId) { return running.containsKey(taskRunId); }

    public int dispatchReady() {
        int free = maxConcurrent - inFlight.get();
        if (free <= 0 || executor.isShutdown()) return 0;
        int dispatched = 0;
        for (UUID id : tasks.findDispatchable(free)) {
            if (inFlight.get() >= maxConcurrent) break;
            Optional<TaskRunEntity> claimed = state.claim(id);
            if (claimed.isEmpty()) continue;
            TaskRunEntity t = claimed.get();
            peakConcurrency.accumulateAndGet(inFlight.incrementAndGet(), Math::max);
            // Register before the task can finish so the finally-block removal always wins.
            CompletableFuture<Void> gate = new CompletableFuture<>();
            Future<?> f = executor.submit(() -> {
                gate.join();
                execute(t);
            });
            running.put(t.getId(), f);
            gate.complete(null);
            dispatched++;
        }
        return dispatched;
    }

    private void execute(TaskRunEntity t) {
        MDC.put("run_id", t.getWorkflowRunId().toString());
        MDC.put("task_key", t.getTaskKey());
        long start = System.nanoTime();
        try {
            TaskHandler handler = handlers.find(t.getTaskType())
                    .orElseThrow(() -> new TaskFailure("no handler for type " + t.getTaskType()));
            JsonNode result = handler.execute(new TaskContext(t.getWorkflowRunId(), t.getTaskKey(), t.getAttempts(), t.getConfig()));
            int failUntil = t.getConfig().path("fail_until_attempt").asInt(0);
            if (t.getAttempts() < failUntil) {
                throw new TaskFailure("simulated failure on attempt " + t.getAttempts() + " (fail_until_attempt=" + failUntil + ")");
            }
            state.recordSuccess(t, result, elapsedMs(start));
            log.info("task succeeded attempt={}", t.getAttempts());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            state.recordFailure(t, "interrupted", elapsedMs(start));
        } catch (Exception e) {
            log.info("task failed attempt={} error={}", t.getAttempts(), e.getMessage());
            state.recordFailure(t, e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage(), elapsedMs(start));
        } finally {
            running.remove(t.getId());
            inFlight.decrementAndGet();
            MDC.clear();
        }
    }

    /** Best-effort interrupt of running tasks; their DB rows were already moved to 'cancelled'. */
    public void interrupt(Collection<UUID> taskRunIds) {
        for (UUID id : taskRunIds) {
            Future<?> f = running.get(id);
            if (f != null) f.cancel(true);
        }
    }

    private static long elapsedMs(long startNanos) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
    }

    @PreDestroy
    public void shutdown() throws InterruptedException {
        executor.shutdown();
        if (!executor.awaitTermination(10, TimeUnit.SECONDS)) executor.shutdownNow();
    }
}

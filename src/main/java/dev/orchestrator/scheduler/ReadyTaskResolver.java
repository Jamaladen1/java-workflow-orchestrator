package dev.orchestrator.scheduler;

import dev.orchestrator.domain.TaskStatus;

import java.util.*;

/**
 * Pure decision logic for one workflow run, given a snapshot of its task states.
 * <ul>
 *   <li>A task is ready iff every dependency has succeeded.</li>
 *   <li>Fail-fast: once any task has terminally failed, the run fails and every not-yet-started task is skipped.
 *       Running tasks may finish but cannot change the outcome.</li>
 *   <li>The run succeeds only if every task succeeded.</li>
 * </ul>
 */
public final class ReadyTaskResolver {
    private ReadyTaskResolver() {}

    public enum Outcome { CONTINUE, SUCCEEDED, FAILED }

    public record TaskState(String key, TaskStatus status, List<String> dependsOn) {}

    public record Decision(Outcome outcome, List<String> promote, Map<String, String> skip) {}

    public static Decision evaluate(List<TaskState> tasks) {
        Map<String, TaskState> byKey = new LinkedHashMap<>();
        tasks.forEach(t -> byKey.put(t.key(), t));

        List<String> failed = tasks.stream().filter(t -> t.status() == TaskStatus.FAILED).map(TaskState::key).sorted().toList();
        if (!failed.isEmpty()) {
            Map<String, String> skip = new LinkedHashMap<>();
            for (TaskState t : tasks) {
                if (t.status() == TaskStatus.PENDING || t.status() == TaskStatus.READY) {
                    Optional<String> upstream = failedAncestor(t.key(), byKey, new HashSet<>());
                    skip.put(t.key(), upstream.map(u -> "upstream task '" + u + "' failed")
                            .orElse("fail-fast: workflow failed (task '" + failed.get(0) + "')"));
                }
            }
            return new Decision(Outcome.FAILED, List.of(), skip);
        }
        if (!tasks.isEmpty() && tasks.stream().allMatch(t -> t.status() == TaskStatus.SUCCEEDED)) {
            return new Decision(Outcome.SUCCEEDED, List.of(), Map.of());
        }
        List<String> promote = new ArrayList<>();
        for (TaskState t : tasks) {
            if (t.status() != TaskStatus.PENDING) continue;
            boolean depsDone = t.dependsOn().stream()
                    .allMatch(d -> byKey.containsKey(d) && byKey.get(d).status() == TaskStatus.SUCCEEDED);
            if (depsDone) promote.add(t.key());
        }
        return new Decision(Outcome.CONTINUE, promote, Map.of());
    }

    private static Optional<String> failedAncestor(String key, Map<String, TaskState> byKey, Set<String> seen) {
        if (!seen.add(key)) return Optional.empty();
        TaskState t = byKey.get(key);
        if (t == null) return Optional.empty();
        for (String d : t.dependsOn()) {
            TaskState dep = byKey.get(d);
            if (dep == null) continue;
            if (dep.status() == TaskStatus.FAILED) return Optional.of(d);
            Optional<String> up = failedAncestor(d, byKey, seen);
            if (up.isPresent()) return up;
        }
        return Optional.empty();
    }
}

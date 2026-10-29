package dev.orchestrator.validation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import dev.orchestrator.domain.TaskDefinition;
import dev.orchestrator.domain.WorkflowDefinition;
import dev.orchestrator.execution.HandlerRegistry;
import dev.orchestrator.execution.TaskHandler;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.regex.Pattern;

/**
 * Validates a workflow definition: structure, unique ids, known dependencies, no self-dependencies,
 * no cycles (Kahn topological sort), node/edge limits, and handler-specific config.
 */
@Component
public class DagValidator {
    public static final int MAX_NODES = 30;
    public static final int MAX_EDGES = 100;
    public static final int MAX_ATTEMPTS_LIMIT = 5;
    private static final Pattern NAME = Pattern.compile("^[a-z0-9][a-z0-9-]{0,63}$");
    private static final Pattern TASK_ID = Pattern.compile("^[A-Za-z0-9_-]{1,64}$");

    private final HandlerRegistry handlers;

    public DagValidator(HandlerRegistry handlers) {
        this.handlers = handlers;
    }

    public ValidationResult validate(JsonNode root) {
        List<ValidationError> errors = new ArrayList<>();
        if (root == null || !root.isObject()) {
            errors.add(new ValidationError("invalid_type", "$", "definition must be a JSON object"));
            return new ValidationResult(errors, null);
        }
        String name = root.path("name").asText("");
        if (!root.path("name").isTextual() || !NAME.matcher(name).matches()) {
            errors.add(new ValidationError("invalid_name", "$.name",
                    "name is required: lowercase letters, digits and '-', max 64 chars"));
        }
        JsonNode tasksNode = root.get("tasks");
        if (tasksNode == null || !tasksNode.isArray() || tasksNode.isEmpty()) {
            errors.add(new ValidationError("missing_tasks", "$.tasks", "tasks must be a non-empty array"));
            return new ValidationResult(errors, null);
        }
        if (tasksNode.size() > MAX_NODES) {
            errors.add(new ValidationError("too_many_nodes", "$.tasks",
                    "at most " + MAX_NODES + " tasks allowed, got " + tasksNode.size()));
        }

        List<TaskDefinition> tasks = new ArrayList<>();
        Set<String> ids = new HashSet<>();
        for (int i = 0; i < tasksNode.size(); i++) {
            String path = "$.tasks[" + i + "]";
            JsonNode t = tasksNode.get(i);
            if (!t.isObject()) {
                errors.add(new ValidationError("invalid_type", path, "task must be an object"));
                continue;
            }
            String id = t.path("id").asText("");
            if (!t.path("id").isTextual() || !TASK_ID.matcher(id).matches()) {
                errors.add(new ValidationError("invalid_task_id", path + ".id",
                        "id is required: letters, digits, '_' or '-', max 64 chars"));
            } else if (!ids.add(id)) {
                errors.add(new ValidationError("duplicate_task_id", path + ".id", "duplicate task id '" + id + "'"));
            }

            String type = t.path("type").asText("");
            JsonNode config = t.has("config") ? t.get("config") : JsonNodeFactory.instance.objectNode();
            if (!config.isObject()) {
                errors.add(new ValidationError("invalid_type", path + ".config", "config must be an object"));
                config = JsonNodeFactory.instance.objectNode();
            }
            Optional<TaskHandler> handler = handlers.find(type);
            if (handler.isEmpty()) {
                errors.add(new ValidationError("unsupported_task_type", path + ".type",
                        "unsupported task type '" + type + "'; supported: " + handlers.types()));
            } else {
                handler.get().validateConfig(config, path + ".config", errors);
            }
            JsonNode fua = config.get("fail_until_attempt");
            if (fua != null && (!fua.isIntegralNumber() || fua.asInt() < 1 || fua.asInt() > 10)) {
                errors.add(new ValidationError("invalid_value", path + ".config.fail_until_attempt",
                        "fail_until_attempt must be an integer between 1 and 10"));
            }

            int maxAttempts = TaskDefinition.DEFAULT_MAX_ATTEMPTS;
            if (t.has("max_attempts")) {
                JsonNode ma = t.get("max_attempts");
                if (!ma.isIntegralNumber() || ma.asInt() < 1 || ma.asInt() > MAX_ATTEMPTS_LIMIT) {
                    errors.add(new ValidationError("invalid_value", path + ".max_attempts",
                            "max_attempts must be an integer between 1 and " + MAX_ATTEMPTS_LIMIT));
                } else {
                    maxAttempts = ma.asInt();
                }
            }

            List<String> deps = new ArrayList<>();
            JsonNode depsNode = t.get("depends_on");
            if (depsNode != null && !depsNode.isNull()) {
                if (!depsNode.isArray()) {
                    errors.add(new ValidationError("invalid_type", path + ".depends_on", "depends_on must be an array"));
                } else {
                    for (int j = 0; j < depsNode.size(); j++) {
                        JsonNode d = depsNode.get(j);
                        if (!d.isTextual()) {
                            errors.add(new ValidationError("invalid_type", path + ".depends_on[" + j + "]",
                                    "dependency must be a task id string"));
                        } else if (deps.contains(d.asText())) {
                            errors.add(new ValidationError("duplicate_dependency", path + ".depends_on[" + j + "]",
                                    "duplicate dependency '" + d.asText() + "'"));
                        } else {
                            deps.add(d.asText());
                        }
                    }
                }
            }
            tasks.add(new TaskDefinition(id, type, config, List.copyOf(deps), maxAttempts));
        }

        int edges = 0;
        for (int i = 0; i < tasks.size(); i++) {
            TaskDefinition t = tasks.get(i);
            for (String dep : t.dependsOn()) {
                edges++;
                if (dep.equals(t.id())) {
                    errors.add(new ValidationError("self_dependency", "$.tasks[" + i + "].depends_on",
                            "task '" + t.id() + "' depends on itself"));
                } else if (!ids.contains(dep)) {
                    errors.add(new ValidationError("unknown_dependency", "$.tasks[" + i + "].depends_on",
                            "task '" + t.id() + "' depends on unknown task '" + dep + "'"));
                }
            }
        }
        if (edges > MAX_EDGES) {
            errors.add(new ValidationError("too_many_edges", "$.tasks",
                    "at most " + MAX_EDGES + " dependency edges allowed, got " + edges));
        }

        List<String> cycle = findCycleNodes(tasks);
        if (!cycle.isEmpty()) {
            errors.add(new ValidationError("cycle_detected", "$.tasks",
                    "dependency cycle detected among tasks " + cycle));
        }
        return new ValidationResult(errors, errors.isEmpty() ? new WorkflowDefinition(name, List.copyOf(tasks)) : null);
    }

    /** Kahn's algorithm. Returns topological order, or empty if a cycle exists. Ignores unknown/self edges. */
    public static List<String> topologicalOrder(List<TaskDefinition> tasks) {
        Map<String, Integer> indegree = new LinkedHashMap<>();
        Map<String, List<String>> dependents = new HashMap<>();
        for (TaskDefinition t : tasks) {
            indegree.putIfAbsent(t.id(), 0);
            dependents.putIfAbsent(t.id(), new ArrayList<>());
        }
        for (TaskDefinition t : tasks) {
            for (String dep : t.dependsOn()) {
                if (!indegree.containsKey(dep) || dep.equals(t.id())) continue;
                indegree.merge(t.id(), 1, Integer::sum);
                dependents.get(dep).add(t.id());
            }
        }
        Deque<String> queue = new ArrayDeque<>();
        indegree.forEach((k, v) -> { if (v == 0) queue.add(k); });
        List<String> order = new ArrayList<>();
        while (!queue.isEmpty()) {
            String n = queue.poll();
            order.add(n);
            for (String d : dependents.get(n)) {
                if (indegree.merge(d, -1, Integer::sum) == 0) queue.add(d);
            }
        }
        return order.size() == indegree.size() ? order : List.of();
    }

    static List<String> findCycleNodes(List<TaskDefinition> tasks) {
        if (tasks.isEmpty() || !topologicalOrder(tasks).isEmpty()) return List.of();
        // Nodes Kahn could not drain are on (or downstream of) a cycle.
        Map<String, Integer> indegree = new LinkedHashMap<>();
        Map<String, List<String>> dependents = new HashMap<>();
        for (TaskDefinition t : tasks) { indegree.putIfAbsent(t.id(), 0); dependents.putIfAbsent(t.id(), new ArrayList<>()); }
        for (TaskDefinition t : tasks) for (String dep : t.dependsOn()) {
            if (!indegree.containsKey(dep) || dep.equals(t.id())) continue;
            indegree.merge(t.id(), 1, Integer::sum);
            dependents.get(dep).add(t.id());
        }
        Deque<String> q = new ArrayDeque<>();
        indegree.forEach((k, v) -> { if (v == 0) q.add(k); });
        while (!q.isEmpty()) for (String d : dependents.get(q.poll())) if (indegree.merge(d, -1, Integer::sum) == 0) q.add(d);
        return indegree.entrySet().stream().filter(e -> e.getValue() > 0).map(Map.Entry::getKey).sorted().toList();
    }
}

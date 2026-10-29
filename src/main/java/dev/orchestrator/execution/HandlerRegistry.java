package dev.orchestrator.execution;

import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

@Component
public class HandlerRegistry {
    private final Map<String, TaskHandler> handlers;

    public HandlerRegistry(List<TaskHandler> handlers) {
        this.handlers = handlers.stream().collect(Collectors.toMap(TaskHandler::type, Function.identity(),
                (a, b) -> a, TreeMap::new));
    }

    public Optional<TaskHandler> find(String type) { return Optional.ofNullable(handlers.get(type)); }

    public List<String> types() { return List.copyOf(handlers.keySet()); }
}

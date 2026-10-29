package dev.orchestrator.scheduler;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.orchestrator.persistence.TaskEventEntity;
import dev.orchestrator.persistence.TaskEventRepository;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.UUID;

@Component
public class EventRecorder {
    private final TaskEventRepository events;
    private final ObjectMapper mapper;

    public EventRecorder(TaskEventRepository events, ObjectMapper mapper) {
        this.events = events;
        this.mapper = mapper;
    }

    public void record(UUID runId, UUID taskRunId, String type, Map<String, ?> detail) {
        events.save(new TaskEventEntity(runId, taskRunId, type, mapper.valueToTree(detail == null ? Map.of() : detail)));
    }
}

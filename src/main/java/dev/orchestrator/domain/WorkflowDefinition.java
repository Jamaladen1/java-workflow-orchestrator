package dev.orchestrator.domain;

import java.util.List;

public record WorkflowDefinition(String name, List<TaskDefinition> tasks) {
}

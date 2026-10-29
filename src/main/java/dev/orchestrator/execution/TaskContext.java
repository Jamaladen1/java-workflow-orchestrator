package dev.orchestrator.execution;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.UUID;

public record TaskContext(UUID runId, String taskKey, int attempt, JsonNode config) {
}

package dev.orchestrator.domain;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;

public record TaskDefinition(String id, String type, JsonNode config, List<String> dependsOn, int maxAttempts) {
    public static final int DEFAULT_MAX_ATTEMPTS = 3;
}

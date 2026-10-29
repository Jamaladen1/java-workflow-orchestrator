package dev.orchestrator.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "orchestrator")
public record OrchestratorProperties(
        int maxConcurrentTasks,
        long tickIntervalMs,
        long retryBaseMs,
        long retryMaxMs,
        String apiKey,
        boolean seedDemoData,
        int maxDefinitionBytes,
        long orphanSweepIntervalMs) {

    public OrchestratorProperties {
        if (maxConcurrentTasks <= 0) maxConcurrentTasks = 4;
        if (tickIntervalMs <= 0) tickIntervalMs = 200;
        if (orphanSweepIntervalMs <= 0) orphanSweepIntervalMs = 15000;
        if (retryBaseMs <= 0) retryBaseMs = 2000;
        if (retryMaxMs <= 0) retryMaxMs = 60000;
        if (maxDefinitionBytes <= 0) maxDefinitionBytes = 65536;
        if (apiKey == null) apiKey = "";
    }
}

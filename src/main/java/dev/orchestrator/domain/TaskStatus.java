package dev.orchestrator.domain;

public enum TaskStatus {
    PENDING, READY, RUNNING, SUCCEEDED, FAILED, SKIPPED, CANCELLED;

    public String db() { return name().toLowerCase(); }

    public boolean isTerminal() {
        return this == SUCCEEDED || this == FAILED || this == SKIPPED || this == CANCELLED;
    }

    public static TaskStatus of(String s) { return valueOf(s.toUpperCase()); }
}

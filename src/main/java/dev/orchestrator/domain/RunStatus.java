package dev.orchestrator.domain;

public enum RunStatus {
    PENDING, RUNNING, SUCCEEDED, FAILED, CANCELLED;

    public String db() { return name().toLowerCase(); }

    public boolean isTerminal() { return this == SUCCEEDED || this == FAILED || this == CANCELLED; }

    public static RunStatus of(String s) { return valueOf(s.toUpperCase()); }
}

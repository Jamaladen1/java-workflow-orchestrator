package dev.orchestrator.execution;

public class TaskFailure extends RuntimeException {
    public TaskFailure(String message) { super(message); }
}

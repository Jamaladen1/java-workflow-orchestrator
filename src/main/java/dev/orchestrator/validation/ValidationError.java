package dev.orchestrator.validation;

public record ValidationError(String code, String path, String message) {
}

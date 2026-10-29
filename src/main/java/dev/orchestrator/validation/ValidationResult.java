package dev.orchestrator.validation;

import dev.orchestrator.domain.WorkflowDefinition;

import java.util.List;

public record ValidationResult(List<ValidationError> errors, WorkflowDefinition definition) {
    public boolean isValid() { return errors.isEmpty(); }
}

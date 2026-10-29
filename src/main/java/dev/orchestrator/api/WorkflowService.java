package dev.orchestrator.api;

import com.fasterxml.jackson.databind.JsonNode;
import dev.orchestrator.persistence.WorkflowDefinitionEntity;
import dev.orchestrator.persistence.WorkflowDefinitionRepository;
import dev.orchestrator.validation.DagValidator;
import dev.orchestrator.validation.ValidationResult;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
public class WorkflowService {
    private final DagValidator validator;
    private final WorkflowDefinitionRepository definitions;

    public WorkflowService(DagValidator validator, WorkflowDefinitionRepository definitions) {
        this.validator = validator;
        this.definitions = definitions;
    }

    public record SaveResult(ValidationResult validation, WorkflowDefinitionEntity saved) {}

    /** Validates and stores an immutable new version (version = previous max + 1 for that name). */
    @Transactional
    public SaveResult save(JsonNode raw) {
        ValidationResult result = validator.validate(raw);
        if (!result.isValid()) return new SaveResult(result, null);
        String name = result.definition().name();
        int version = definitions.maxVersion(name) + 1;
        try {
            WorkflowDefinitionEntity e = definitions.saveAndFlush(new WorkflowDefinitionEntity(UUID.randomUUID(), name, version, raw));
            return new SaveResult(result, e);
        } catch (DataIntegrityViolationException e) {
            throw new ApiException(org.springframework.http.HttpStatus.CONFLICT, "concurrent save of '" + name + "', retry");
        }
    }
}

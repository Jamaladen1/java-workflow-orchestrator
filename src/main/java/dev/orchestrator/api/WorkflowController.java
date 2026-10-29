package dev.orchestrator.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.orchestrator.api.Dtos.*;
import dev.orchestrator.config.OrchestratorProperties;
import dev.orchestrator.persistence.WorkflowDefinitionEntity;
import dev.orchestrator.persistence.WorkflowDefinitionRepository;
import dev.orchestrator.scheduler.RunStateService;
import dev.orchestrator.validation.DagValidator;
import dev.orchestrator.validation.ValidationResult;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/workflows")
public class WorkflowController {
    private final WorkflowService service;
    private final WorkflowDefinitionRepository definitions;
    private final DagValidator validator;
    private final RunStateService state;
    private final RunQueryService queries;
    private final ObjectMapper mapper;
    private final OrchestratorProperties props;

    public WorkflowController(WorkflowService service, WorkflowDefinitionRepository definitions, DagValidator validator,
                              RunStateService state, RunQueryService queries, ObjectMapper mapper,
                              OrchestratorProperties props) {
        this.service = service;
        this.definitions = definitions;
        this.validator = validator;
        this.state = state;
        this.queries = queries;
        this.mapper = mapper;
        this.props = props;
    }

    @PostMapping(consumes = "application/json")
    public ResponseEntity<?> create(@RequestBody String body) {
        JsonNode raw = parse(body);
        WorkflowService.SaveResult r = service.save(raw);
        if (!r.validation().isValid()) {
            return ResponseEntity.unprocessableEntity()
                    .body(new ValidationFailure("workflow definition is invalid", r.validation().errors()));
        }
        WorkflowDefinitionEntity e = r.saved();
        return ResponseEntity.status(HttpStatus.CREATED).body(new WorkflowCreated(e.getId(), e.getName(), e.getVersion()));
    }

    /** Dry-run validation used by the editor; never persists. */
    @PostMapping(path = "/validate", consumes = "application/json")
    public ResponseEntity<?> validate(@RequestBody String body) {
        ValidationResult r = validator.validate(parse(body));
        if (!r.isValid()) {
            return ResponseEntity.unprocessableEntity().body(new ValidationFailure("workflow definition is invalid", r.errors()));
        }
        return ResponseEntity.ok(java.util.Map.of("valid", true, "tasks", r.definition().tasks().size()));
    }

    @GetMapping
    public List<WorkflowSummary> list() {
        return definitions.findAllByOrderByNameAscVersionDesc().stream()
                .map(d -> new WorkflowSummary(d.getId(), d.getName(), d.getVersion(),
                        d.getDefinition().path("tasks").size(), d.getCreatedAt()))
                .toList();
    }

    @GetMapping("/{id}")
    public WorkflowDetail get(@PathVariable UUID id) {
        WorkflowDefinitionEntity d = find(id);
        return new WorkflowDetail(d.getId(), d.getName(), d.getVersion(), d.getCreatedAt(), d.getDefinition());
    }

    @PostMapping("/{id}/runs")
    public ResponseEntity<RunCreated> run(@PathVariable UUID id) {
        WorkflowDefinitionEntity d = find(id);
        ValidationResult r = validator.validate(d.getDefinition());
        if (!r.isValid()) throw new ApiException(HttpStatus.CONFLICT, "stored definition no longer validates");
        UUID runId = state.createRun(d.getId(), r.definition());
        return ResponseEntity.accepted().body(new RunCreated(runId, "pending"));
    }

    @GetMapping("/{id}/runs")
    public List<RunSummary> runs(@PathVariable UUID id, @RequestParam(defaultValue = "20") int limit,
                                 @RequestParam(defaultValue = "0") int offset) {
        find(id);
        return queries.listRuns(id, limit, offset);
    }

    private WorkflowDefinitionEntity find(UUID id) {
        return definitions.findById(id).orElseThrow(() -> new ApiException(HttpStatus.NOT_FOUND, "workflow not found"));
    }

    private JsonNode parse(String body) {
        if (body == null || body.isBlank()) throw new ApiException(HttpStatus.BAD_REQUEST, "request body is required");
        if (body.getBytes(StandardCharsets.UTF_8).length > props.maxDefinitionBytes()) {
            throw new ApiException(HttpStatus.PAYLOAD_TOO_LARGE, "definition exceeds " + props.maxDefinitionBytes() + " bytes");
        }
        try {
            return mapper.readTree(body);
        } catch (JsonProcessingException e) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "malformed JSON: " + e.getOriginalMessage());
        }
    }
}

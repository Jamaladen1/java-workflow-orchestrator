package dev.orchestrator.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.orchestrator.api.WorkflowService;
import dev.orchestrator.persistence.WorkflowDefinitionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import java.io.InputStream;

/** Seeds the demo workflow definitions (valid ones only) if no version of that name exists yet. */
@Component
public class DemoSeeder implements ApplicationRunner {
    private static final Logger log = LoggerFactory.getLogger(DemoSeeder.class);
    static final String[] SEEDS = {"parallel-join", "retry-demo", "fail-fast"};

    private final OrchestratorProperties props;
    private final WorkflowDefinitionRepository definitions;
    private final WorkflowService service;
    private final ObjectMapper mapper;

    public DemoSeeder(OrchestratorProperties props, WorkflowDefinitionRepository definitions, WorkflowService service,
                      ObjectMapper mapper) {
        this.props = props;
        this.definitions = definitions;
        this.service = service;
        this.mapper = mapper;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        if (!props.seedDemoData()) return;
        for (String name : SEEDS) {
            if (definitions.existsByName(name)) continue;
            try (InputStream in = new ClassPathResource("seed/" + name + ".json").getInputStream()) {
                JsonNode def = mapper.readTree(in);
                var r = service.save(def);
                if (r.validation().isValid()) log.info("seeded workflow {}", name);
                else log.warn("seed {} invalid: {}", name, r.validation().errors());
            }
        }
    }
}

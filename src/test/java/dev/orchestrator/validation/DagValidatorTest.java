package dev.orchestrator.validation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.orchestrator.domain.TaskDefinition;
import dev.orchestrator.execution.*;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class DagValidatorTest {
    private final ObjectMapper mapper = new ObjectMapper();
    private final DagValidator validator = new DagValidator(new HandlerRegistry(List.of(
            new DelayTaskHandler(), new MathTaskHandler(), new TransformTaskHandler(), new HttpTaskHandler(() -> 8090))));

    private ValidationResult validate(String json) throws Exception {
        return validator.validate(mapper.readTree(json));
    }

    private static List<String> codes(ValidationResult r) {
        return r.errors().stream().map(ValidationError::code).toList();
    }

    @Test
    void acceptsSpecExample() throws Exception {
        ValidationResult r = validate("""
                {"name":"order-pipeline","tasks":[
                  {"id":"validate","type":"delay","config":{"duration_ms":300},"depends_on":[]},
                  {"id":"charge","type":"math","config":{"operation":"sum","values":[2,3]},"depends_on":["validate"]}]}""");
        assertThat(r.isValid()).isTrue();
        assertThat(r.definition().tasks()).hasSize(2);
        assertThat(r.definition().tasks().get(1).maxAttempts()).isEqualTo(3);
    }

    @Test
    void detectsCycle() throws Exception {
        ValidationResult r = validate("""
                {"name":"cyc","tasks":[
                  {"id":"a","type":"delay","config":{"duration_ms":1},"depends_on":["c"]},
                  {"id":"b","type":"delay","config":{"duration_ms":1},"depends_on":["a"]},
                  {"id":"c","type":"delay","config":{"duration_ms":1},"depends_on":["b"]},
                  {"id":"root","type":"delay","config":{"duration_ms":1}}]}""");
        assertThat(codes(r)).containsExactly("cycle_detected");
        assertThat(r.errors().get(0).message()).contains("[a, b, c]").doesNotContain("root");
    }

    @Test
    void detectsMissingAndSelfDependencyAndDuplicates() throws Exception {
        ValidationResult r = validate("""
                {"name":"bad","tasks":[
                  {"id":"a","type":"delay","config":{"duration_ms":1},"depends_on":["a","ghost"]},
                  {"id":"a","type":"delay","config":{"duration_ms":1}}]}""");
        assertThat(codes(r)).contains("self_dependency", "unknown_dependency", "duplicate_task_id");
    }

    @Test
    void rejectsUnsupportedTypeAndBadConfig() throws Exception {
        ValidationResult r = validate("""
                {"name":"bad","tasks":[
                  {"id":"a","type":"shell","config":{"cmd":"rm -rf /"}},
                  {"id":"b","type":"delay","config":{"duration_ms":999999}},
                  {"id":"c","type":"http","config":{"status":200,"url":"http://169.254.169.254"}},
                  {"id":"d","type":"math","config":{"operation":"sum","values":["x"]}}]}""");
        assertThat(codes(r)).contains("unsupported_task_type", "invalid_value", "unsupported_field");
        assertThat(r.errors()).anyMatch(e -> e.path().equals("$.tasks[1].config.duration_ms"));
        assertThat(r.errors()).anyMatch(e -> e.path().equals("$.tasks[3].config.values[0]"));
    }

    @Test
    void enforcesNodeAndEdgeLimits() throws Exception {
        StringBuilder sb = new StringBuilder("{\"name\":\"big\",\"tasks\":[");
        for (int i = 0; i < 31; i++) {
            if (i > 0) sb.append(',');
            sb.append("{\"id\":\"t").append(i).append("\",\"type\":\"delay\",\"config\":{\"duration_ms\":1}}");
        }
        sb.append("]}");
        assertThat(codes(validate(sb.toString()))).contains("too_many_nodes");

        // 15 nodes, each depending on every earlier node = 105 edges, acyclic.
        sb = new StringBuilder("{\"name\":\"dense\",\"tasks\":[");
        for (int i = 0; i < 15; i++) {
            if (i > 0) sb.append(',');
            sb.append("{\"id\":\"t").append(i).append("\",\"type\":\"delay\",\"config\":{\"duration_ms\":1},\"depends_on\":[");
            for (int j = 0; j < i; j++) sb.append(j > 0 ? "," : "").append("\"t").append(j).append('"');
            sb.append("]}");
        }
        sb.append("]}");
        assertThat(codes(validate(sb.toString()))).containsExactly("too_many_edges");
    }

    @Test
    void rejectsMissingNameAndTasks() throws Exception {
        assertThat(codes(validate("{\"tasks\":[]}"))).contains("invalid_name", "missing_tasks");
        assertThat(codes(validate("[1,2]"))).containsExactly("invalid_type");
    }

    @Test
    void topologicalOrderRespectsDependencies() throws Exception {
        JsonNode empty = mapper.createObjectNode();
        List<String> order = DagValidator.topologicalOrder(List.of(
                new TaskDefinition("join", "math", empty, List.of("left", "right"), 3),
                new TaskDefinition("left", "delay", empty, List.of("root"), 3),
                new TaskDefinition("right", "delay", empty, List.of("root"), 3),
                new TaskDefinition("root", "delay", empty, List.of(), 3)));
        assertThat(order.indexOf("root")).isLessThan(order.indexOf("left")).isLessThan(order.indexOf("join"));
        assertThat(order.indexOf("right")).isLessThan(order.indexOf("join"));
    }
}

package dev.orchestrator.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.jupiter.api.Test;

class DatabaseUrlEnvironmentPostProcessorTest {

    @Test
    void convertsHostedPostgresUrl() {
        Map<String, Object> p = DatabaseUrlEnvironmentPostProcessor.toJdbcProperties(
                "postgresql://neon_user:p%40ss@ep-cool-1.us-east-2.aws.neon.tech/orchestrator?sslmode=require&channel_binding=require");
        assertThat(p).containsEntry("spring.datasource.url",
                "jdbc:postgresql://ep-cool-1.us-east-2.aws.neon.tech/orchestrator?sslmode=require");
        assertThat(p).containsEntry("spring.datasource.username", "neon_user");
        assertThat(p).containsEntry("spring.datasource.password", "p@ss");
    }

    @Test
    void keepsPortAndLeavesJdbcUrlsAlone() {
        assertThat(DatabaseUrlEnvironmentPostProcessor.toJdbcProperties("postgres://u:p@db:5432/x"))
                .containsEntry("spring.datasource.url", "jdbc:postgresql://db:5432/x");
        assertThat(DatabaseUrlEnvironmentPostProcessor.toJdbcProperties("jdbc:postgresql://db/x")).isEmpty();
        assertThat(DatabaseUrlEnvironmentPostProcessor.toJdbcProperties(null)).isEmpty();
    }
}

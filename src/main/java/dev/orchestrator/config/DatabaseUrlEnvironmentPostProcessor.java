package dev.orchestrator.config;

import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;

/**
 * Lets DATABASE_URL be a hosted-provider URL (postgres://user:pass@host/db?sslmode=require,
 * as Neon, Render and Heroku hand out) as well as a plain jdbc:postgresql:// URL.
 * Credentials embedded in the URL become spring.datasource.username/password.
 */
public class DatabaseUrlEnvironmentPostProcessor implements EnvironmentPostProcessor {

    @Override
    public void postProcessEnvironment(ConfigurableEnvironment env, SpringApplication app) {
        String raw = env.getProperty("DATABASE_URL");
        Map<String, Object> props = toJdbcProperties(raw);
        if (!props.isEmpty()) {
            env.getPropertySources().addFirst(new MapPropertySource("databaseUrl", props));
        }
    }

    static Map<String, Object> toJdbcProperties(String raw) {
        Map<String, Object> props = new HashMap<>();
        if (raw == null || !(raw.startsWith("postgres://") || raw.startsWith("postgresql://"))) {
            return props;
        }
        URI uri = URI.create(raw);
        String query = uri.getRawQuery() == null ? "" : Arrays.stream(uri.getRawQuery().split("&"))
                .filter(p -> !p.startsWith("channel_binding="))
                .collect(Collectors.joining("&"));
        String port = uri.getPort() > 0 ? ":" + uri.getPort() : "";
        props.put("spring.datasource.url", "jdbc:postgresql://" + uri.getHost() + port + uri.getRawPath()
                + (query.isEmpty() ? "" : "?" + query));
        if (uri.getRawUserInfo() != null) {
            String[] parts = uri.getRawUserInfo().split(":", 2);
            props.put("spring.datasource.username", decode(parts[0]));
            if (parts.length > 1) {
                props.put("spring.datasource.password", decode(parts[1]));
            }
        }
        return props;
    }

    private static String decode(String s) {
        return URLDecoder.decode(s, StandardCharsets.UTF_8);
    }
}

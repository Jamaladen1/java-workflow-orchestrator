package dev.orchestrator.observability;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.UUID;
import java.util.regex.Pattern;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class RequestIdFilter extends OncePerRequestFilter {
    private static final Logger log = LoggerFactory.getLogger("http");
    private static final Pattern SAFE = Pattern.compile("^[A-Za-z0-9-]{1,64}$");

    @Override
    protected void doFilterInternal(HttpServletRequest req, HttpServletResponse res, FilterChain chain)
            throws ServletException, IOException {
        String id = req.getHeader("X-Request-Id");
        if (id == null || !SAFE.matcher(id).matches()) id = UUID.randomUUID().toString();
        MDC.put("request_id", id);
        res.setHeader("X-Request-Id", id);
        long start = System.nanoTime();
        try {
            chain.doFilter(req, res);
        } finally {
            // Log writes only; the UI polls GETs every second and would drown the log.
            if (!"GET".equals(req.getMethod())) {
                log.info("{} {} -> {} ({} ms)", req.getMethod(), req.getRequestURI(), res.getStatus(),
                        (System.nanoTime() - start) / 1_000_000);
            }
            MDC.remove("request_id");
        }
    }
}

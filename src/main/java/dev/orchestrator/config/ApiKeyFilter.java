package dev.orchestrator.config;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

/** When API_KEY is set, every write under /api requires the X-API-Key header. Empty API_KEY = open demo mode. */
@Component
@Order(10)
public class ApiKeyFilter extends OncePerRequestFilter {
    private final byte[] key;

    public ApiKeyFilter(OrchestratorProperties props) {
        this.key = props.apiKey().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest req) {
        // The decoded, ';'-parameter-free path that request mapping uses; getRequestURI() is raw and
        // would let "/api;x=1/..." or "/%61pi/..." through.
        String path = req.getServletPath() + (req.getPathInfo() == null ? "" : req.getPathInfo());
        return key.length == 0 || !path.startsWith("/api/")
                || req.getMethod().equals("GET") || req.getMethod().equals("OPTIONS")
                || path.equals("/api/workflows/validate");
    }

    @Override
    protected void doFilterInternal(HttpServletRequest req, HttpServletResponse res, FilterChain chain)
            throws ServletException, IOException {
        String given = req.getHeader("X-API-Key");
        if (given == null || !MessageDigest.isEqual(key, given.getBytes(StandardCharsets.UTF_8))) {
            res.setStatus(401);
            res.setContentType("application/json");
            res.getWriter().write("{\"error\":\"missing or invalid X-API-Key\",\"status\":401}");
            return;
        }
        chain.doFilter(req, res);
    }
}

package dev.orchestrator.observability;

import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/** In-process counters for outcomes since this JVM started (exposed on /api/metrics). */
@Component
public class OutcomeCounters {
    private final Map<String, AtomicLong> counters = new ConcurrentHashMap<>();

    public void inc(String name) { counters.computeIfAbsent(name, k -> new AtomicLong()).incrementAndGet(); }

    public Map<String, Long> snapshot() {
        Map<String, Long> out = new TreeMap<>();
        counters.forEach((k, v) -> out.put(k, v.get()));
        return out;
    }
}

package dev.orchestrator.domain;

/**
 * Exponential backoff: base * 2^(attempt-1), capped. With base=2s this gives 2s / 4s / 8s ... capped at 60s.
 */
public final class RetryPolicy {
    private final long baseMs;
    private final long maxMs;

    public RetryPolicy(long baseMs, long maxMs) {
        this.baseMs = baseMs;
        this.maxMs = maxMs;
    }

    /** Delay before the next attempt, given the attempt number that just failed (1-based). */
    public long backoffMs(int failedAttempt) {
        int exp = Math.max(0, Math.min(failedAttempt - 1, 30));
        long delay = baseMs * (1L << exp);
        return Math.min(delay, maxMs);
    }

    public boolean canRetry(int attempts, int maxAttempts) {
        return attempts < maxAttempts;
    }
}

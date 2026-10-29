package dev.orchestrator.scheduler;

import dev.orchestrator.domain.RetryPolicy;
import dev.orchestrator.domain.TaskStatus;
import dev.orchestrator.scheduler.ReadyTaskResolver.Outcome;
import dev.orchestrator.scheduler.ReadyTaskResolver.TaskState;
import org.junit.jupiter.api.Test;

import java.util.List;

import static dev.orchestrator.domain.TaskStatus.*;
import static org.assertj.core.api.Assertions.assertThat;

class ReadyTaskResolverTest {

    private static TaskState t(String key, TaskStatus s, String... deps) {
        return new TaskState(key, s, List.of(deps));
    }

    @Test
    void rootsAreReadyImmediately() {
        var d = ReadyTaskResolver.evaluate(List.of(t("a", PENDING), t("b", PENDING), t("c", PENDING, "a", "b")));
        assertThat(d.outcome()).isEqualTo(Outcome.CONTINUE);
        assertThat(d.promote()).containsExactlyInAnyOrder("a", "b");
    }

    @Test
    void joinWaitsForAllDependencies() {
        var d = ReadyTaskResolver.evaluate(List.of(t("a", SUCCEEDED), t("b", RUNNING), t("c", PENDING, "a", "b")));
        assertThat(d.promote()).isEmpty();
        d = ReadyTaskResolver.evaluate(List.of(t("a", SUCCEEDED), t("b", SUCCEEDED), t("c", PENDING, "a", "b")));
        assertThat(d.promote()).containsExactly("c");
    }

    @Test
    void retryingDependencyKeepsDependentsBlocked() {
        // A task waiting for its retry backoff is 'ready', not succeeded -> dependents stay pending.
        var d = ReadyTaskResolver.evaluate(List.of(t("a", READY), t("b", PENDING, "a")));
        assertThat(d.promote()).isEmpty();
        assertThat(d.outcome()).isEqualTo(Outcome.CONTINUE);
    }

    @Test
    void failFastSkipsDependentsAndOtherPendingTasks() {
        var d = ReadyTaskResolver.evaluate(List.of(
                t("root", SUCCEEDED), t("bad", FAILED, "root"), t("slow", RUNNING, "root"),
                t("join", PENDING, "bad", "slow"), t("report", PENDING, "join"), t("side", READY, "root")));
        assertThat(d.outcome()).isEqualTo(Outcome.FAILED);
        assertThat(d.skip()).containsOnlyKeys("join", "report", "side");
        assertThat(d.skip().get("join")).contains("upstream task 'bad' failed");
        assertThat(d.skip().get("report")).contains("upstream task 'bad' failed");
        assertThat(d.skip().get("side")).startsWith("fail-fast");
        assertThat(d.promote()).isEmpty();
    }

    @Test
    void succeedsOnlyWhenEveryTaskSucceeded() {
        assertThat(ReadyTaskResolver.evaluate(List.of(t("a", SUCCEEDED), t("b", SUCCEEDED))).outcome())
                .isEqualTo(Outcome.SUCCEEDED);
        assertThat(ReadyTaskResolver.evaluate(List.of(t("a", SUCCEEDED), t("b", RUNNING))).outcome())
                .isEqualTo(Outcome.CONTINUE);
    }

    @Test
    void retryBackoffScheduleAndEligibility() {
        RetryPolicy p = new RetryPolicy(2000, 60000);
        assertThat(p.backoffMs(1)).isEqualTo(2000);
        assertThat(p.backoffMs(2)).isEqualTo(4000);
        assertThat(p.backoffMs(3)).isEqualTo(8000);
        assertThat(p.backoffMs(10)).isEqualTo(60000);
        assertThat(p.canRetry(1, 3)).isTrue();
        assertThat(p.canRetry(2, 3)).isTrue();
        assertThat(p.canRetry(3, 3)).isFalse();
    }
}

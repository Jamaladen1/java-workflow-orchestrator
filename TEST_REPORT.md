# Test Report: DAG Workflow Orchestrator

Date: 2026-10-29. Tested against the README (the design spec; `DESIGN_REVIEW.md`, referenced by
`IMPLEMENTATION_STATUS.md`, is not in the repository) and `IMPLEMENTATION_STATUS.md`.

**Environment:** macOS (arm64), JDK 22 compiling with `--release 21`, Docker Engine 29.5.2 on Colima, Postgres 16
(Compose and Testcontainers), Python 3 + curl + jq for live HTTP tests.

## Summary

- Baseline: all existing tests passed (15 unit, 8 integration). Live testing then found **5 bugs** the suite
  missed: one security bypass, three state and recovery defects, and one pagination defect. All 5 are fixed, each
  with a regression test that was confirmed to **fail before the fix and pass after**.
- Final suite: **35 tests, 0 failures** (15 unit, 20 integration). The suite passed 4 times in a row after the fixes. The API-key bypass test was added before the last of those runs.
- Every endpoint was exercised over real HTTP against the Docker Compose deployment, before and after the fixes.

## Bugs found and fixed

| # | Severity | Bug | Fix |
|---|---|---|---|
| 1 | **High (security)** | With `API_KEY` set, the key check could be bypassed. `POST /api;x=1/workflows` and `POST /%61pi/workflows` returned **201** with no key. `ApiKeyFilter` matched the raw `getRequestURI()`, but Spring routes on the decoded path with `;` parameters removed. | The filter now checks the servlet path, which is the same normalized path used for routing. |
| 2 | High (stuck run) | If a task's outcome write failed because the database was unavailable longer than the two 30 s connection timeouts, the task stayed `running` with no worker. The run hung until the process restarted, because orphan recovery only ran at boot. Reproduced live with a 75 s Postgres stop. | The scheduler repeats the orphan sweep every `ORPHAN_SWEEP_MS` (default 15 s). It runs under the scheduler lock, so it cannot see a task between its claim and its registration with the dispatcher. `recoverOrphan` now counts only real recoveries. |
| 3 | Medium (data consistency) | Fail-fast left tasks stuck. If a run failed while another task was still running, and that task then failed with retries left, it was rescheduled to `ready` inside the already-`failed` run. It stayed `ready` forever, so the run was terminal while a task was not. Reproduced live. | `recordFailure` locks the run row (`SELECT … FOR UPDATE`). If the run is terminal, the task fails with `(not retried: run already failed)`. `evaluateRun` and `recoverOrphan` take the same lock, so a retry cannot slip in while fail-fast is finishing the run. Lock order is always the run row first, then the task row, so there is no deadlock. |
| 4 | Medium (race) | Cancel could lose a race with dispatch, despite the README's "cancel wins any race". `cancelRun` chose its update from a stale snapshot of task status. If a claim committed `ready → running` in between, neither update applied, and the task kept running and succeeding inside a `cancelled` run. Reproduced deterministically by holding the claim's row lock. | `cancelRun` tries both compare-and-set updates (not-started first, then running) instead of trusting the snapshot. The claim also refuses to start a task whose run is no longer `running`. |
| 5 | Low | `offset` was rounded down to a page boundary. `GET /api/runs?limit=5&offset=3` returned items 1–5, not 4–8. | Native `LIMIT/OFFSET` queries, with `id` as a tiebreaker for stable ordering. |
| 6 | Cosmetic | A `POST` with an empty body returned Spring's generic 400 instead of the API's `{"error","status"}` shape. | `@RequestBody(required = false)`, so the controller's own check runs. |

**Configuration fix:** `compose.yaml` did not pass `RETRY_MAX_MS` or `SCHEDULER_TICK_MS` to the app, so setting them
in `.env` silently did nothing. They are now passed through, along with the new `ORPHAN_SWEEP_MS` and `DB_POOL_SIZE`.
`.env.example` now explains what is read where. `.env` is optional, Compose reads it, and Spring Boot does not.

## Requirements verified

| Requirement (README) | How verified | Result |
|---|---|---|
| DAG validation: cycle, unknown dependency, self-dependency, duplicate ids, unsupported type, bad config, 30-node and 100-edge limits, 64 KB cap | Live curl for each case; `DagValidatorTest` | Pass: structured 422 errors with code and path; 413 over 64 KB |
| Malformed JSON → 400, wrong content type → 415, bad UUID → 400, unknown resource → 404 | Live curl; integration tests | Pass |
| `POST /api/workflows/validate` is a dry run | Integration test confirms nothing is persisted | Pass |
| Immutable versioning (`v1`, `v2`, …) | Integration test; 16 and 10 concurrent saves of one name | Pass: versions unique and gap-free. See remaining issues for the 409s. |
| Parallel branches and join barrier | `demo.sh` live; integration tests | Pass: branches overlap, and the join starts after both finish |
| Bounded executor (`MAX_CONCURRENT_TASKS=4`) | 30 concurrent live runs; integration test with 10 runs | Pass: peak concurrency 4 of 4 |
| Retries with backoff 2 s → 4 s, at most `max_attempts` executions | `demo.sh` (event log shows 2000 ms, then 4000 ms); integration tests | Pass |
| Fail-fast: run fails, not-started tasks skipped, in-flight tasks finish | `demo.sh`; integration tests | Pass after fixing bug 3 |
| Cancellation: pending tasks cancelled, running tasks interrupted, 409 on second cancel, cancel wins | Live: 10 runs × 3 concurrent cancels gave exactly one 202 each, and no task started after cancel; integration tests | Pass after fixing bug 4 |
| Restart recovery | Live `docker compose kill -s KILL app` mid-run, before and after the fixes: "recovered 2 orphaned running task(s)", branches re-ran with `attempts=2`, run succeeded | Pass |
| Recovery from a database outage (beyond the spec) | Live 6 s and 75 s Postgres stops while a task was running | 6 s: recovered. 75 s: hung before the fix; recovered about 7 s after the database returned once fixed |
| Compare-and-set transitions, no duplicate dispatch | Integration test; live check that `task_started` events equal `attempts` for every task in 30 runs | Pass |
| Late completions refused | Integration test; `late_completions_ignored` counter seen live | Pass |
| Run, event, history and metrics endpoints; health checks | Live curl; integration tests | Pass after fixing bug 5. `/health/ready` returned 503 during the database outage and 200 once it recovered |
| `API_KEY` protects writes; reads stay open | Live with `API_KEY=s3cret`; integration tests | Pass after fixing bug 1 |
| `X-Request-Id` echoed | Live curl | Pass |
| `http` task calls only the internal mock; `url` rejected | Live validation; integration test with 200 and 503 | Pass |
| Docker build and Compose deployment | `docker compose up -d --build`, run 3 times, including the frontend stage | Pass: healthy, UI served on `/` |

## Tests executed

- `./mvnw verify` with the final code: **15 unit and 20 integration tests, all passed.** Repeated runs were also green.
- **New integration tests** (12):
  - In `OrchestratorIT`: in-flight retry after fail-fast, cancel/claim race, claim guard, pagination offsets,
    `http` task 2xx and 5xx, concurrent runs within the executor bound, concurrent cancels, concurrent same-name saves,
    and read endpoints with request edge cases.
  - In the new `OrphanSweepAndApiKeyIT`: API key on writes, API key path-bypass attempts, and orphan recovery without a restart.
- **One existing test was changed:** `stateTransitionsAreCompareAndSet` used to claim a task inside a `cancelled`
  run, which the claim guard now correctly refuses. It now uses a `running` run and holds the scheduler lock.
- **Live (Docker Compose, real HTTP):** `scripts/demo.sh`, about 25 hand-run curl edge cases, a Python stress script
  (`/tmp/orch_stress.py`, not committed), a `kill -9` crash/restart, two Postgres outages, and API key bypass attempts.
  Everything was repeated on the fixed image.

## Remaining issues and untested areas

- **Concurrent saves of the same name return `409 retry`.** In the live test, 9–11 of 16 simultaneous requests got
  this response. Data stays correct, but clients must retry. Left as designed.
- **`math` overflow:** `product [1e308, 10]` succeeds with `"value": "Infinity"`, a string instead of a number.
  This is a minor quirk and was left unchanged.
- **Integer overflow in config:** integers larger than a 64-bit `long` in task config wrap around during range
  checks. This was found by code review and not exploited, because the handlers clamp or ignore the values.
- **Old rows are not repaired:** runs left inconsistent by bugs 2–4 before the fix stay as they are. Restarting
  repairs orphaned `running` tasks, but `ready` tasks stuck in already-failed runs (one exists in the local
  development database) are not repaired.
- **Not tested:**
  - The browser UI was not tested end to end. It was only checked as served, with HTTP 200 on `/`.
  - The hosted Render + Neon deployment and its `postgres://` URL conversion were not tested against a real
    provider. The conversion is covered only by its unit test.
  - Multi-instance deployment is not supported, by design.
  - Graceful shutdown with tasks in flight was not tested.
- **Frontend build:** `npm run build` was not run directly because `node_modules` isn't installed locally. It ran
  successfully inside the Docker build.
- **Test data:** live testing left extra definitions and runs (`probe-*`, `stress-*`) in the local Compose
  database volume. Remove them with `docker compose down -v` if needed.

## Deployment readiness

**Ready for the documented scope: a single-node demo or MVP.** All documented behaviour was verified on the real
deployment. The bugs most likely to matter in production, which are the API key bypass and runs hanging after a
database blip, are fixed and covered by regression tests. That database scenario is likely on the free Neon/Render
setup, where connections drop and instances sleep. Before relying on it beyond a demo:

- Set `API_KEY` for any public deployment.
- Keep exactly one instance, as the README says.
- Treat task execution as at-least-once.

# DAG Orchestrator — Workflow Orchestration Engine

**Java 21 · Spring Boot 3.3 · PostgreSQL · React Flow**

A workflow service that accepts declarative DAG definitions and validates the dependency graph.
It runs independent steps concurrently while respecting prerequisites. The UI shows every node's
status live as the run executes.

> **Scope:** **single-node orchestration**. One JVM runs the API, the scheduler, and a bounded executor.
> It is **not** a distributed scheduler and is not horizontally scalable (see [Limitations](#limitations)).

![dashboard](docs/screenshots/parallel-join.png)

| Parallel branches running, join waiting | Fail-fast: downstream tasks skipped | Cycle rejected with a structured 422 |
|---|---|---|
| ![running](docs/screenshots/01-parallel-branches-running.png) | ![fail-fast](docs/screenshots/03-fail-fast-skipped.png) | ![cycle](docs/screenshots/04-cycle-rejected-422.png) |

## Highlights
- **DAG validation**: Kahn topological sort, structured `422` errors (cycle, unknown or self dependency, duplicate ids,
  unsupported type, bad config), limits of 30 nodes and 100 edges, 64 KB payload cap.
- **Immutable versioned definitions**: saving the same `name` again creates `v2`, `v3`, …
- **Concurrent execution**: independent ready tasks run in parallel, capped by `MAX_CONCURRENT_TASKS` (default 4).
- **Join barrier**: a task becomes `ready` only when **every** dependency has `succeeded`.
- **Retries with exponential backoff**: 2 s → 4 s → 8s …, capped at 60 s. A task waiting for a retry blocks its dependents.
- **Fail-fast**: after a terminal failure the run fails and every not-yet-started task is `skipped`.
  In-flight tasks may finish, but the outcome stays `failed`, and an in-flight task that then fails is not retried.
- **Cancellation**: pending and ready tasks become `cancelled`, and running tasks are interrupted. Cancel wins any race.
- **Restart recovery**: on boot, any task left `running` by a dead process is retried (or failed if out of attempts).
  The same sweep repeats every `ORPHAN_SWEEP_MS` for tasks whose worker could not save its outcome (e.g. a DB outage).
- **Every transition is a compare-and-set** in PostgreSQL. In-memory futures are never the source of truth.

## Architecture

```
                    ┌──────────────────────────── single JVM (Spring Boot, :8090) ────────────────────────────┐
  Browser           │                                                                                         │
 ┌──────────┐ poll  │  ┌───────────────┐   ┌──────────────┐     ┌──────────────────────┐   ┌───────────────┐   │
 │ React    │──1s──▶│  │ REST API      │──▶│ DagValidator │     │ WorkflowScheduler    │──▶│ TaskDispatcher│   │
 │ Flow UI  │◀──────│  │ /api/...      │   │ (Kahn sort)  │     │ @Scheduled tick 200ms│   │ ThreadPool(4) │   │
 └──────────┘       │  └──────┬────────┘   └──────────────┘     │ • evaluate runs      │   │ delay│math│   │   │
                    │         │ create run / cancel             │ • promote ready      │   │ http │transform│ │
                    │         ▼                                 │ • fail-fast / finish │   └──────┬────────┘   │
                    │  ┌──────────────────────────────────┐     └──────────┬───────────┘          │ outcome    │
                    │  │ RunStateService — one transaction │◀──────────────┘   claim (CAS)       │ (CAS)      │
                    │  │ per transition: CAS UPDATE + event │◀──────────────────────────────────────┘            │
                    │  └──────────────┬───────────────────┘                                                    │
                    └─────────────────┼────────────────────────────────────────────────────────────────────────┘
                                      ▼
                         ┌───────────────────────────────────────────────────────────────┐
                         │ PostgreSQL: workflow_definitions · workflow_runs · task_runs · │
                         │             task_events            (Flyway migrations)         │
                         └───────────────────────────────────────────────────────────────┘
```

**Run lifecycle:** definition → validation/topological check → immutable version → `workflow_run` + `task_run` rows →
the scheduler promotes ready tasks → the dispatcher claims them (`ready→running`, CAS) → the handler runs on the bounded pool
→ the outcome is persisted (`running→succeeded | ready (retry) | failed`, CAS) → the next tick re-evaluates dependents.

### Task state machine
```
 pending ──(all deps succeeded)──▶ ready ──(claim, attempts++)──▶ running ──▶ succeeded
    │                               ▲  │                            │
    │                               │  └──(cancel)──▶ cancelled     ├──(error, attempts < max)──▶ ready (next_run_at = now + backoff)
    └──(upstream failed)──▶ skipped │                               ├──(error, attempts = max)──▶ failed ──▶ run fails, others skipped
                                    └───────(restart recovery)──────┘  (cancel) ──▶ cancelled; late completion refused by CAS
```

### Key invariants and how they are enforced
| Invariant | Mechanism |
|---|---|
| A task is dispatched at most once at a time | `UPDATE … SET status='running' WHERE id=? AND status='ready' AND next_run_at<=now()` returns 1 for exactly one caller |
| Late or duplicate completions can't overwrite state | completion `UPDATE … WHERE id=? AND status='running'` — after a cancel or recovery it updates 0 rows and logs `late_completion_ignored` |
| Join waits for all branches | `ReadyTaskResolver`: `ready ⇔ ∀ dep: status = succeeded` |
| Retries are bounded | `attempts` increments on claim, so `max_attempts=3` means at most 3 executions |
| State survives restarts | All state is in Postgres. On boot, orphaned `running` rows → `ready` (with backoff) or `failed` |
| Database time is authoritative | `now()` in SQL for every timestamp and every backoff comparison |

## Quick start

```bash
docker compose up -d --build        # postgres (host :55433) + app (:8090), migrations + demo seeds run automatically
open http://localhost:8090          # dashboard
scripts/demo.sh                     # runs parallel-join, retry-demo, fail-fast and a rejected cycle (needs jq)
```

No manual DB setup is needed: Flyway creates the schema, and three demo workflows (`parallel-join`, `retry-demo`,
`fail-fast`) are seeded if they don't exist yet.

### Local development
```bash
docker compose up -d postgres                 # just the database on :55433
./mvnw spring-boot:run                        # API + scheduler on :8090 (JDK 21+)
cd frontend && npm install && npm run dev     # UI on :5173, proxies /api to :8090
```

### Tests
```bash
./mvnw test      # unit: DAG validator, topological sort, ready-task resolution, fail-fast, retry policy
./mvnw verify    # + Testcontainers integration: parallel dispatch, join barrier, retries, failure propagation,
                 #   restart recovery, cancellation, CAS idempotency, HTTP error semantics
```
On a Colima host, export the socket first:
`export DOCKER_HOST=unix://$HOME/.colima/default/docker.sock TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock`

## Configuration
| Variable | Default | Purpose |
|---|---|---|
| `DATABASE_URL` | `jdbc:postgresql://localhost:55433/orchestrator` | JDBC URL |
| `DATABASE_USER` / `DATABASE_PASSWORD` | `orchestrator` | DB credentials |
| `PORT` | `8090` | HTTP port |
| `MAX_CONCURRENT_TASKS` | `4` | executor threads |
| `RETRY_BASE_MS` / `RETRY_MAX_MS` | `2000` / `60000` | backoff = base·2^(attempt−1), capped |
| `SCHEDULER_TICK_MS` | `200` | scheduler loop interval |
| `ORPHAN_SWEEP_MS` | `15000` | how often tasks left `running` with no live worker (e.g. outcome lost during a DB outage) are recovered |
| `DB_POOL_SIZE` | `10` | JDBC connection pool size |
| `API_KEY` | *(empty)* | if set, all `POST /api/**` except the dry-run `/api/workflows/validate` need `X-API-Key` (the UI prompts for it) |

All settings are optional. `docker compose` reads overrides from a `.env` file (see `.env.example`); Spring Boot
does not, so export them for `./mvnw spring-boot:run`.
| `SEED_DEMO_DATA` | `true` | seed demo workflows on startup |

## API
| Method | Path | Notes |
|---|---|---|
| `POST` | `/api/workflows` | body = definition → `201 {id, name, version}` · `422 {errors:[{code,path,message}]}` · `400` malformed JSON · `413` too large |
| `POST` | `/api/workflows/validate` | dry run, no persistence → `200` or `422` |
| `GET` | `/api/workflows` · `/api/workflows/{id}` | list or detail (`404`) |
| `POST` | `/api/workflows/{id}/runs` | `202 {run_id, status:"pending"}` |
| `GET` | `/api/workflows/{id}/runs` · `/api/runs?workflowId=&limit=&offset=` | run history |
| `GET` | `/api/runs/{runId}` | run status plus every task's status, attempts, timing, result, and error |
| `GET` | `/api/runs/{runId}/events` | timeline |
| `POST` | `/api/runs/{runId}/cancel` | `202` · `409` if already terminal · `404` |
| `GET` | `/api/metrics` | runs and tasks by status, in-flight count, peak concurrency, outcome counters |
| `GET` | `/health/live` · `/health/ready` | readiness checks the DB and that the scheduler has started |

```bash
curl -X POST localhost:8090/api/workflows -H 'Content-Type: application/json' -d '{
  "name": "order-pipeline",
  "tasks": [
    {"id": "validate", "type": "delay", "config": {"duration_ms": 300}, "depends_on": []},
    {"id": "charge",   "type": "math",  "config": {"operation": "sum", "values": [2, 3]}, "depends_on": ["validate"]}
  ]}'
# {"id":"…","name":"order-pipeline","version":1}

curl -X POST localhost:8090/api/workflows/<id>/runs        # {"run_id":"…","status":"pending"}
curl localhost:8090/api/runs/<run_id> | jq '.status, [.tasks[] | {task_key, status, attempts}]'
```

Cycle rejection:
```json
HTTP/1.1 422
{"error":"workflow definition is invalid","errors":[
  {"code":"unsupported_task_type","path":"$.tasks[3].type","message":"unsupported task type 'shell'; supported: [delay, http, math, transform]"},
  {"code":"unknown_dependency","path":"$.tasks[3].depends_on","message":"task 'd' depends on unknown task 'ghost'"},
  {"code":"cycle_detected","path":"$.tasks","message":"dependency cycle detected among tasks [a, b, c]"}]}
```

### Task types (built-in only; no shell, no user code)
| type | config | behaviour |
|---|---|---|
| `delay` | `duration_ms` (0–10000) | sleeps (interruptible) |
| `math` | `operation` ∈ sum/product/min/max/fail, `values` (1–100 numbers) | deterministic result; `fail` always fails |
| `transform` | `operation` ∈ upper/lower/reverse/length, `input` (≤1000 chars) | deterministic string transform |
| `http` | `status` (100–599), `delay_ms` | calls the service's own `/internal/mock/{status}`. Arbitrary URLs are rejected (no SSRF) |

Any task can add `"fail_until_attempt": N` to its config to fail deterministically until attempt N, for retry demos.
Per-task `max_attempts` ranges from 1 to 5 (default 3).

## Demo scenarios (seeded)
| Workflow | What it shows |
|---|---|
| `parallel-join` | `enrich_users` and `enrich_orders` run **at the same time**. `join` starts only after both succeed |
| `retry-demo` | `flaky_charge` fails on attempts 1 and 2, waits 2 s then 4 s, and succeeds on attempt 3. `notify` waits for it |
| `fail-fast` | `always_fails` exhausts 2 attempts → run `failed`. `join` and `report` are `skipped`. `slow_branch` keeps running to completion |
| *cycle (UI template)* | `a → b → c → a` is rejected with `422`, and the offending nodes are highlighted on the canvas |

Failure/recovery demo: start `parallel-join`, then `docker compose kill -s KILL app && docker compose start app`
while the branches are running. On boot the log shows `recovered 2 orphaned running task(s)`, the branches re-run
(`attempts=2`), and the run still succeeds.

## Deployment (hosted)

### Free deploy: Render + Neon ($0)

`render.yaml` deploys the UI, API and scheduler as one free Render web service.
`DATABASE_URL` accepts a provider-style `postgresql://user:pass@host/db?sslmode=require`
URL (it's converted to JDBC on startup), and `JAVA_TOOL_OPTIONS` caps the heap so the
app fits in 512 MB (it uses ~190 MB).

1. Create a free Neon database and copy its **direct** (not pooled) connection string.
2. In Render: **New → Blueprint**, pick this repo, and paste the string as `DATABASE_URL`.
3. Free instances sleep after ~15 min idle, so the first request takes ~30–60s.
   Runs interrupted by sleep are recovered on wake by the restart-recovery path.

1. Provision managed PostgreSQL 14+ (Neon, Supabase, RDS, Cloud SQL, …).
2. Build and push the image: `docker build -t <registry>/workflow-orchestrator . && docker push …`
3. Run **one** instance (the scheduler is single-node) on Fly.io, Render, Railway, Cloud Run (min=max=1 instances), ECS, etc., with
   `DATABASE_URL=jdbc:postgresql://<host>:5432/<db>?sslmode=require`, `DATABASE_USER`, `DATABASE_PASSWORD`, and
   `API_KEY` (strongly recommended for public demos). Health checks: `/health/live` (liveness) and `/health/ready` (readiness).
4. Migrations run automatically on boot (Flyway).

## Observability
- Structured JSON logs (logstash encoder) with `request_id`, `run_id`, and `task_key` in the MDC. Credentials are never logged.
- `X-Request-Id` is accepted or generated and echoed on every response. Write requests get an access-log line.
- `/api/metrics`: outcome counters (`tasks_retried`, `runs_failed`, `late_completions_ignored`, …), plus in-flight and peak concurrency.
- `task_events` records the full execution history, shown in the UI timeline.

## Design trade-offs
- **Polling scheduler (200 ms) over events**: simple and restart-safe, at the cost of a little latency and steady DB queries.
- **`ready` + `next_run_at` instead of a separate "retrying" state**: one status, one dispatch query, and dependents are
  blocked automatically because the task isn't `succeeded`.
- **`attempts` increments on dispatch**, not on failure, so a crash mid-execution still counts toward the attempt limit.
- **Fail-fast skips all pending work**, not only descendants. This is simple and predictable for an MVP. A "continue independent
  branches" policy would be a per-workflow option.
- **Offset pagination** for run history (cursor pagination was deferred).

## Limitations
- **Single node.** Two instances against one database would not double-dispatch a task (CAS), but restart recovery would
  treat the other node's running tasks as orphans. Scale-out needs worker ownership plus leases/heartbeats with fencing
  tokens (as in the companion Go job queue), not just CAS.
- **At-least-once execution.** A crash after a handler's side effect but before the state write re-runs the task. Handlers
  must be idempotent. The built-in ones are deterministic and side-effect-free.
- **Cancellation can't undo side effects.** It interrupts threads cooperatively.
- No cron schedules, secrets management, multi-tenancy, or user-written code (by design).

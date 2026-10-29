# Implementation Status — Workflow Orchestration Engine (MVP v1.0)

Updated 2026-10-29.

## Milestones
- [x] **M1** Spring Boot 3.3.5 scaffold (Java 21 release), Flyway, PostgreSQL, DTOs, `/health/live` + `/health/ready`, Docker Compose
- [x] **M2** Definition schema, `DagValidator` (unique ids, known deps, no self-deps, Kahn cycle detection, ≤30 nodes, ≤100 edges, handler config, 64 KB cap), structured 422; create/read endpoints, immutable versioning
- [x] **M3** Persistent run/task state, transactional CAS scheduler, bounded executor (MAX_CONCURRENT_TASKS=4); parallel-branch + join tests
- [x] **M4** Retries (2s/4s/8s… capped 60s, `next_run_at`), fail-fast skip propagation, cancellation (+interrupt, CAS "cancel wins"), restart recovery; integration tests
- [x] **M5** React Flow dashboard, seeded demo workflows, `scripts/demo.sh`, CI workflow, README

## Verification log
| Command | Result |
|---|---|
| `./mvnw test` | 13 unit tests pass (DagValidatorTest 7, ReadyTaskResolverTest 6) |
| `./mvnw verify` | +8 Testcontainers integration tests pass (OrchestratorIT): parallel dispatch, join barrier, bounded retry, fail-fast propagation, restart recovery (retry + exhausted), cancel/409, CAS idempotency, 422/400/404 + versioning |
| `cd frontend && npm run build` | builds (331 KB JS / 107 KB gzip) |
| `docker compose up -d --build` | postgres + app healthy; UI and API on :8090 |
| `scripts/demo.sh` | parallel branches overlap; join starts after both; retry 2 s → 4 s then success; fail-fast skips `join`/`report` while `slow_branch` finishes; cycle → 422 with 3 structured errors |
| `docker compose kill -s KILL app` mid-run, then `start` | "recovered 2 orphaned running task(s)"; both branches retried (attempts=2), run succeeded |
| `API_KEY=s3cret docker compose up -d app` | POST without key → 401, with key → 202, GET → 200 |

## Deviations / notes
- Testcontainers bumped to **1.21.4** and `docker-java.properties` pins `api.version=1.44`. Docker Engine 29 rejects the older client API that Testcontainers 1.20 uses.
- On a Colima host, run tests with `DOCKER_HOST=unix://$HOME/.colima/default/docker.sock TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock`.
- Added endpoints beyond the spec: `GET /api/runs`, `GET /api/workflows/{id}/runs`, `POST /api/workflows/validate` (dry run), `GET /api/metrics`.
- Extra task type `transform` (deterministic string ops). The `http` type only calls the internal `/internal/mock/{status}` endpoint.
- History uses offset pagination (no cursor), per DESIGN_REVIEW.md.

## Not done
- Hosted deployment: instructions only (see README).
- No browser E2E test suite. The UI was verified manually and via API smoke tests.

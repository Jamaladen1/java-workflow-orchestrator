#!/usr/bin/env bash
# Exercises the main scenarios against a running orchestrator:
#   1) parallel branches + join barrier   2) bounded retries with backoff
#   3) fail-fast propagation              4) cycle rejected with 422
# Usage: scripts/demo.sh [base_url]     (requires curl + jq)
set -euo pipefail
BASE=${1:-http://localhost:8090}
H=(-H 'Content-Type: application/json')
[ -n "${API_KEY:-}" ] && H+=(-H "X-API-Key: $API_KEY")

wf_id() { curl -fsS "$BASE/api/workflows" | jq -r --arg n "$1" '[.[] | select(.name==$n)] | max_by(.version) | .id'; }

run_and_wait() {
  local name=$1 id run status
  id=$(wf_id "$name")
  run=$(curl -fsS -X POST "${H[@]}" "$BASE/api/workflows/$id/runs" | jq -r .run_id)
  echo "▶ $name  run=$run" >&2
  while :; do
    status=$(curl -fsS "$BASE/api/runs/$run" | jq -r .status)
    case $status in succeeded|failed|cancelled) break ;; esac
    sleep 0.5
  done
  # let in-flight tasks of a failed run finish so their timings are final
  while [ "$(curl -fsS "$BASE/api/runs/$run" | jq '[.tasks[] | select(.status=="running")] | length')" != 0 ]; do sleep 0.5; done
  echo "$run"
}

timeline() {
  curl -fsS "$BASE/api/runs/$1" | jq -r '
    (.started_at | sub("\\.[0-9]+";"") | sub("Z$";"+00:00")) as $s |
    "  run status: \(.status)   duration: \(.duration_ms) ms",
    (.tasks | sort_by(.started_at // "~")[] |
      "  \(.task_key | . + (" " * (14 - length)))  \(.status | . + (" " * (10 - length)))  attempts=\(.attempts)/\(.max_attempts)  start=\(.started_at // "-" | .[11:23])  end=\(.finished_at // "-" | .[11:23])\(if .last_error then "  err=\(.last_error)" else "" end)")'
}

echo "== 1. parallel-join =="
R=$(run_and_wait parallel-join); timeline "$R"
curl -fsS "$BASE/api/runs/$R" | jq -r '
  (.tasks | map({(.task_key): .}) | add) as $t |
  "  overlap(users,orders): \($t.enrich_users.started_at < $t.enrich_orders.finished_at and $t.enrich_orders.started_at < $t.enrich_users.finished_at)",
  "  join started after both finished: \($t.join.started_at >= $t.enrich_users.finished_at and $t.join.started_at >= $t.enrich_orders.finished_at)"'

echo; echo "== 2. retry-demo =="
R=$(run_and_wait retry-demo); timeline "$R"
curl -fsS "$BASE/api/runs/$R/events" | jq -r '.[] | select(.type=="task_retry_scheduled") | "  retry: \(.task_key) attempt \(.detail.attempt) failed -> backoff \(.detail.backoff_ms) ms"'

echo; echo "== 3. fail-fast =="
R=$(run_and_wait fail-fast); timeline "$R"

echo; echo "== 4. cycle rejected =="
curl -sS -o /tmp/cycle.json -w "  HTTP %{http_code}\n" -X POST "${H[@]}" "$BASE/api/workflows" -d '{
  "name":"cycle-invalid","tasks":[
   {"id":"a","type":"delay","config":{"duration_ms":10},"depends_on":["c"]},
   {"id":"b","type":"delay","config":{"duration_ms":10},"depends_on":["a"]},
   {"id":"c","type":"delay","config":{"duration_ms":10},"depends_on":["b"]},
   {"id":"d","type":"shell","config":{},"depends_on":["ghost"]}]}'
jq -r '.errors[] | "  \(.code)  \(.path)  \(.message)"' /tmp/cycle.json

echo; echo "== metrics =="
curl -fsS "$BASE/api/metrics" | jq -c '{peak_concurrency, runs_by_status, counters_since_start}'

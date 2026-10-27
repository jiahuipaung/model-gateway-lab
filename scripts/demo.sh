#!/bin/sh
set -eu

base_url="${BASE_URL:-http://127.0.0.1:8080}"

wait_for_gateway() {
  attempt=0
  until curl --fail --silent "${base_url}/healthz" >/dev/null; do
    attempt=$((attempt + 1))
    if [ "$attempt" -ge 60 ]; then
      echo "gateway did not become healthy" >&2
      exit 1
    fi
    sleep 1
  done
}

create_task() {
  key="$1"
  model="$2"
  curl --fail --silent \
    -X POST "${base_url}/v1/tasks" \
    -H 'Content-Type: application/json' \
    -H 'X-Tenant-ID: demo-team' \
    -H "Idempotency-Key: ${key}" \
    -d "{\"provider\":\"fake-a\",\"model\":\"${model}\",\"operation\":\"text-to-video\",\"prompt\":\"a paper boat crossing a quiet lake\"}"
}

wait_for_terminal() {
  task_id="$1"
  attempt=0
  while [ "$attempt" -lt 60 ]; do
    response="$(curl --fail --silent -H 'X-Tenant-ID: demo-team' "${base_url}/v1/tasks/${task_id}")"
    state="$(printf '%s' "$response" | jq -r '.task.execution_state')"
    if [ "$state" = "SUCCEEDED" ] || [ "$state" = "FAILED" ]; then
      printf '%s' "$response"
      return 0
    fi
    attempt=$((attempt + 1))
    sleep 0.2
  done
  echo "task ${task_id} did not reach a terminal state" >&2
  return 1
}

wait_for_gateway

run_id="$(date +%s)-$$"
normal_key="demo-normal-${run_id}"
unknown_key="demo-unknown-${run_id}"
normal_create="$(create_task "$normal_key" demo-success)"
normal_replay="$(create_task "$normal_key" demo-success)"
unknown_create="$(create_task "$unknown_key" demo-lost-response)"
normal_id="$(printf '%s' "$normal_create" | jq -r '.task.id')"
unknown_id="$(printf '%s' "$unknown_create" | jq -r '.task.id')"

test "$(printf '%s' "$normal_replay" | jq -r '.replayed')" = "true"
test "$(printf '%s' "$normal_replay" | jq -r '.task.id')" = "$normal_id"

normal_result="$(wait_for_terminal "$normal_id")"
unknown_result="$(wait_for_terminal "$unknown_id")"

printf '%s\n' "$normal_result" | jq '{scenario:"normal", task_id:.task.id, submission:.task.submission_state, execution:.task.execution_state, output:.task.output_ref}'
printf '%s\n' "$normal_replay" | jq '{scenario:"idempotent-replay", task_id:.task.id, replayed:.replayed}'
printf '%s\n' "$unknown_result" | jq '{scenario:"lost-response-reconciled", task_id:.task.id, submission:.task.submission_state, execution:.task.execution_state, output:.task.output_ref}'

test "$(printf '%s' "$normal_result" | jq -r '.task.execution_state')" = "SUCCEEDED"
test "$(printf '%s' "$unknown_result" | jq -r '.task.execution_state')" = "SUCCEEDED"

#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

BINARY="${REPO_ROOT}/bin/hookaido"
OUTPUT_ROOT="${REPO_ROOT}/.artifacts/adaptive-ab"
SCENARIO="mixed"
BACKEND="sqlite"
DURATION_SECONDS=120
INGRESS_WORKERS=16
MIXED_DRAIN_WORKERS=8
DEQUEUE_BATCH=15
PULL_TOKEN="adaptive-bench-token"
QUEUE_MAX_DEPTH=50000
ADAPTIVE_MIN_TOTAL=200
ADAPTIVE_QUEUED_PERCENT=80
ADAPTIVE_READY_LAG="30s"
ADAPTIVE_OLDEST_AGE="60s"
ADAPTIVE_SUSTAINED_GROWTH="on"

RUN_ID="$(date -u +%Y%m%d-%H%M%S)"
RUN_ROOT=""

ACTIVE_SERVER_PID=""
ACTIVE_MONITOR_PID=""
ACTIVE_WORKER_PIDS=""

usage() {
	cat <<'EOF'
Usage:
  scripts/adaptive-ab.sh [options]

Runs adaptive backpressure A/B scenarios with identical load profiles:
  - baseline: adaptive enabled=off
  - variant:  adaptive enabled=on

Scenarios:
  pull   = ingress-only saturation (reference profile)
  mixed  = ingress + pull dequeue/ack workers (default, #53/#54/#55 focus)
  all    = pull and mixed

Options:
  --binary <path>            Hookaido binary (default: ./bin/hookaido)
  --output-root <path>       Artifact root (default: ./.artifacts/adaptive-ab)
  --scenario <pull|mixed|all>
  --backend <sqlite|memory>  Queue backend (default: sqlite)
  --duration-seconds <n>     Load duration per run (default: 120)
  --ingress-workers <n>      Parallel ingress workers (default: 16)
  --mixed-drain-workers <n>  Parallel pull drain workers in mixed scenario (default: 8)
  --dequeue-batch <n>        Pull dequeue batch size (default: 15)
  --queue-max-depth <n>      Queue max_depth for test config (default: 50000)
  --run-id <id>              Override run id (default: UTC timestamp)
  --help                     Show this help

Outputs:
  <output-root>/<run-id>/<scenario>-<mode>/
    Hookaidofile
    run-meta.json
    server.log
    ingress-results.tsv
    monitor-output.log
    final-metrics.txt
    final-health.json
    summary.env
    summary.json

  <output-root>/<run-id>/comparison-<scenario>.md
  <output-root>/<run-id>/comparison.md
EOF
}

error() {
	printf 'error: %s\n' "$*" >&2
	exit 1
}

require_cmd() {
	local cmd="$1"
	command -v "${cmd}" >/dev/null 2>&1 || error "required command not found: ${cmd}"
}

parse_args() {
	while [ "$#" -gt 0 ]; do
		case "$1" in
		--binary)
			shift
			BINARY="${1:-}"
			;;
		--output-root)
			shift
			OUTPUT_ROOT="${1:-}"
			;;
		--scenario)
			shift
			SCENARIO="${1:-}"
			;;
		--backend)
			shift
			BACKEND="${1:-}"
			;;
		--duration-seconds)
			shift
			DURATION_SECONDS="${1:-}"
			;;
		--ingress-workers)
			shift
			INGRESS_WORKERS="${1:-}"
			;;
		--mixed-drain-workers)
			shift
			MIXED_DRAIN_WORKERS="${1:-}"
			;;
		--dequeue-batch)
			shift
			DEQUEUE_BATCH="${1:-}"
			;;
		--queue-max-depth)
			shift
			QUEUE_MAX_DEPTH="${1:-}"
			;;
		--run-id)
			shift
			RUN_ID="${1:-}"
			;;
		--help|-h)
			usage
			exit 0
			;;
		*)
			error "unknown argument: $1"
			;;
		esac
		shift || true
	done
}

validate_args() {
	case "${SCENARIO}" in
	pull|mixed|all) ;;
	*) error "--scenario must be pull|mixed|all (got: ${SCENARIO})" ;;
	esac
	case "${BACKEND}" in
	sqlite|memory) ;;
	*) error "--backend must be sqlite|memory (got: ${BACKEND})" ;;
	esac

	[[ "${DURATION_SECONDS}" =~ ^[0-9]+$ ]] || error "--duration-seconds must be an integer"
	[[ "${INGRESS_WORKERS}" =~ ^[0-9]+$ ]] || error "--ingress-workers must be an integer"
	[[ "${MIXED_DRAIN_WORKERS}" =~ ^[0-9]+$ ]] || error "--mixed-drain-workers must be an integer"
	[[ "${DEQUEUE_BATCH}" =~ ^[0-9]+$ ]] || error "--dequeue-batch must be an integer"
	[[ "${QUEUE_MAX_DEPTH}" =~ ^[0-9]+$ ]] || error "--queue-max-depth must be an integer"

	[ "${DURATION_SECONDS}" -gt 0 ] || error "--duration-seconds must be > 0"
	[ "${INGRESS_WORKERS}" -gt 0 ] || error "--ingress-workers must be > 0"
	[ "${MIXED_DRAIN_WORKERS}" -gt 0 ] || error "--mixed-drain-workers must be > 0"
	[ "${DEQUEUE_BATCH}" -gt 0 ] || error "--dequeue-batch must be > 0"
	[ "${QUEUE_MAX_DEPTH}" -gt 0 ] || error "--queue-max-depth must be > 0"

	[ -x "${BINARY}" ] || error "hookaido binary not executable: ${BINARY}"
}

fetch_metric_value() {
	local file="$1"
	local metric="$2"
	local labels="$3"
	local value
	if [ -z "${labels}" ]; then
		value="$(
			awk -v metric="${metric}" '
				$1 == metric { print $2; found = 1; exit }
				END { if (!found) print "0" }
			' "${file}"
		)"
	else
		value="$(
			awk -v metric="${metric}" -v labels="${labels}" '
				index($1, metric"{") == 1 && index($1, labels) > 0 { print $2; found = 1; exit }
				END { if (!found) print "0" }
			' "${file}"
		)"
	fi
	printf '%s' "${value}"
}

sum_metric_value() {
	local file="$1"
	local metric="$2"
	awk -v metric="${metric}" '
		$1 == metric || index($1, metric"{") == 1 { sum += ($2 + 0); found = 1 }
		END {
			if (!found) {
				print "0"
				exit
			}
			printf "%.0f", sum
		}
	' "${file}"
}

summary_get() {
	local file="$1"
	local key="$2"
	sed -n "s/^${key}=//p" "${file}" | head -n1
}

compute_latency_percentile_ms() {
	local ingress_results="$1"
	local percentile="$2"
	local tmp
	local total
	local idx
	tmp="$(mktemp)"
	awk -F '\t' '$2 == "202" { printf "%.6f\n", ($3 + 0) * 1000 }' "${ingress_results}" | sort -n >"${tmp}"
	total="$(wc -l <"${tmp}" | tr -d '[:space:]')"
	if [ -z "${total}" ] || [ "${total}" -eq 0 ]; then
		rm -f "${tmp}"
		printf '0'
		return
	fi
	idx=$(( (percentile * total + 99) / 100 ))
	if [ "${idx}" -lt 1 ]; then
		idx=1
	fi
	if [ "${idx}" -gt "${total}" ]; then
		idx="${total}"
	fi
	sed -n "${idx}p" "${tmp}"
	rm -f "${tmp}"
}

render_comparison() {
	local scenario="$1"
	local off_summary="$2"
	local on_summary="$3"
	local out_file="$4"

	local off_adaptive
	local on_adaptive
	local off_adaptive_reject
	local on_adaptive_reject
	local off_queue_full
	local on_queue_full
	local off_ready_lag
	local on_ready_lag
	local off_oldest
	local on_oldest
	local off_p95
	local on_p95
	local off_p99
	local on_p99
	local off_rate
	local on_rate
	local off_pull_acked
	local on_pull_acked
	local off_pull_ack_conflict
	local on_pull_ack_conflict
	local off_pull_nack_conflict
	local on_pull_nack_conflict
	local off_pull_ack_conflict_ratio
	local on_pull_ack_conflict_ratio

	off_adaptive="$(summary_get "${off_summary}" "adaptive_applied_total")"
	on_adaptive="$(summary_get "${on_summary}" "adaptive_applied_total")"
	off_adaptive_reject="$(summary_get "${off_summary}" "adaptive_reject_503_total")"
	on_adaptive_reject="$(summary_get "${on_summary}" "adaptive_reject_503_total")"
	off_queue_full="$(summary_get "${off_summary}" "queue_full_reject_503_total")"
	on_queue_full="$(summary_get "${on_summary}" "queue_full_reject_503_total")"
	off_ready_lag="$(summary_get "${off_summary}" "queue_ready_lag_seconds")"
	on_ready_lag="$(summary_get "${on_summary}" "queue_ready_lag_seconds")"
	off_oldest="$(summary_get "${off_summary}" "queue_oldest_queued_age_seconds")"
	on_oldest="$(summary_get "${on_summary}" "queue_oldest_queued_age_seconds")"
	off_p95="$(summary_get "${off_summary}" "latency_p95_ms")"
	on_p95="$(summary_get "${on_summary}" "latency_p95_ms")"
	off_p99="$(summary_get "${off_summary}" "latency_p99_ms")"
	on_p99="$(summary_get "${on_summary}" "latency_p99_ms")"
	off_rate="$(summary_get "${off_summary}" "accepted_rate_per_second")"
	on_rate="$(summary_get "${on_summary}" "accepted_rate_per_second")"
	off_pull_acked="$(summary_get "${off_summary}" "pull_acked_total")"
	on_pull_acked="$(summary_get "${on_summary}" "pull_acked_total")"
	off_pull_ack_conflict="$(summary_get "${off_summary}" "pull_ack_conflict_total")"
	on_pull_ack_conflict="$(summary_get "${on_summary}" "pull_ack_conflict_total")"
	off_pull_nack_conflict="$(summary_get "${off_summary}" "pull_nack_conflict_total")"
	on_pull_nack_conflict="$(summary_get "${on_summary}" "pull_nack_conflict_total")"
	off_pull_ack_conflict_ratio="$(summary_get "${off_summary}" "pull_ack_conflict_ratio_percent")"
	on_pull_ack_conflict_ratio="$(summary_get "${on_summary}" "pull_ack_conflict_ratio_percent")"

	cat >"${out_file}" <<EOF
# Adaptive Backpressure A/B Comparison (${scenario})

| Metric | Adaptive Off | Adaptive On | Delta (On-Off) |
| --- | ---: | ---: | ---: |
| hookaido_ingress_adaptive_backpressure_applied_total | ${off_adaptive} | ${on_adaptive} | $(awk -v on="${on_adaptive}" -v off="${off_adaptive}" 'BEGIN { printf "%.3f", (on+0)-(off+0) }') |
| hookaido_ingress_rejected_by_reason_total{reason="adaptive_backpressure",status="503"} | ${off_adaptive_reject} | ${on_adaptive_reject} | $(awk -v on="${on_adaptive_reject}" -v off="${off_adaptive_reject}" 'BEGIN { printf "%.3f", (on+0)-(off+0) }') |
| hookaido_ingress_rejected_by_reason_total{reason="queue_full",status="503"} | ${off_queue_full} | ${on_queue_full} | $(awk -v on="${on_queue_full}" -v off="${off_queue_full}" 'BEGIN { printf "%.3f", (on+0)-(off+0) }') |
| hookaido_queue_ready_lag_seconds | ${off_ready_lag} | ${on_ready_lag} | $(awk -v on="${on_ready_lag}" -v off="${off_ready_lag}" 'BEGIN { printf "%.3f", (on+0)-(off+0) }') |
| hookaido_queue_oldest_queued_age_seconds | ${off_oldest} | ${on_oldest} | $(awk -v on="${on_oldest}" -v off="${off_oldest}" 'BEGIN { printf "%.3f", (on+0)-(off+0) }') |
| ingress_latency_p95_ms | ${off_p95} | ${on_p95} | $(awk -v on="${on_p95}" -v off="${off_p95}" 'BEGIN { printf "%.3f", (on+0)-(off+0) }') |
| ingress_latency_p99_ms | ${off_p99} | ${on_p99} | $(awk -v on="${on_p99}" -v off="${off_p99}" 'BEGIN { printf "%.3f", (on+0)-(off+0) }') |
| accepted_request_rate_per_second | ${off_rate} | ${on_rate} | $(awk -v on="${on_rate}" -v off="${off_rate}" 'BEGIN { printf "%.3f", (on+0)-(off+0) }') |
| hookaido_pull_acked_total (sum routes) | ${off_pull_acked} | ${on_pull_acked} | $(awk -v on="${on_pull_acked}" -v off="${off_pull_acked}" 'BEGIN { printf "%.3f", (on+0)-(off+0) }') |
| hookaido_pull_ack_conflict_total (sum routes) | ${off_pull_ack_conflict} | ${on_pull_ack_conflict} | $(awk -v on="${on_pull_ack_conflict}" -v off="${off_pull_ack_conflict}" 'BEGIN { printf "%.3f", (on+0)-(off+0) }') |
| hookaido_pull_nack_conflict_total (sum routes) | ${off_pull_nack_conflict} | ${on_pull_nack_conflict} | $(awk -v on="${on_pull_nack_conflict}" -v off="${off_pull_nack_conflict}" 'BEGIN { printf "%.3f", (on+0)-(off+0) }') |
| pull_ack_conflict_ratio_percent | ${off_pull_ack_conflict_ratio} | ${on_pull_ack_conflict_ratio} | $(awk -v on="${on_pull_ack_conflict_ratio}" -v off="${off_pull_ack_conflict_ratio}" 'BEGIN { printf "%.3f", (on+0)-(off+0) }') |
EOF
}

create_run_meta() {
	local run_dir="$1"
	local scenario="$2"
	local mode="$3"
	local ingress_port="$4"
	local pull_port="$5"
	local admin_port="$6"
	local metrics_port="$7"
	local config_file="$8"
	local binary_sha
	local git_rev
	local version_json

	binary_sha="$(sha256sum "${BINARY}" | awk '{print $1}')"
	git_rev="$(git -C "${REPO_ROOT}" rev-parse HEAD 2>/dev/null || printf 'unknown')"
	version_json="$("${BINARY}" version --json 2>/dev/null || printf '{"version":"unknown","commit":"unknown","build_date":"unknown"}')"

	cat >"${run_dir}/run-meta.json" <<EOF
{
  "run_id": "${RUN_ID}",
  "scenario": "${scenario}",
  "mode": "${mode}",
  "timestamp_utc": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "binary_path": "${BINARY}",
  "binary_sha256": "${binary_sha}",
  "git_revision": "${git_rev}",
  "backend": "${BACKEND}",
  "duration_seconds": ${DURATION_SECONDS},
  "ingress_workers": ${INGRESS_WORKERS},
  "mixed_drain_workers": ${MIXED_DRAIN_WORKERS},
  "dequeue_batch": ${DEQUEUE_BATCH},
  "queue_max_depth": ${QUEUE_MAX_DEPTH},
  "ports": {
    "ingress": ${ingress_port},
    "pull_api": ${pull_port},
    "admin_api": ${admin_port},
    "metrics": ${metrics_port}
  },
  "config_file": "${config_file}",
  "version": ${version_json}
}
EOF
}

write_config() {
	local config_file="$1"
	local ingress_port="$2"
	local pull_port="$3"
	local admin_port="$4"
	local metrics_port="$5"
	local adaptive_enabled="$6"

	cat >"${config_file}" <<EOF
ingress {
  listen 127.0.0.1:${ingress_port}
}

pull_api {
  listen 127.0.0.1:${pull_port}
  prefix /pull
  auth token raw:${PULL_TOKEN}
}

admin_api {
  listen 127.0.0.1:${admin_port}
}

observability {
  metrics {
    listen 127.0.0.1:${metrics_port}
    prefix /metrics
  }
}

defaults {
  adaptive_backpressure {
    enabled ${adaptive_enabled}
    min_total ${ADAPTIVE_MIN_TOTAL}
    queued_percent ${ADAPTIVE_QUEUED_PERCENT}
    ready_lag ${ADAPTIVE_READY_LAG}
    oldest_queued_age ${ADAPTIVE_OLDEST_AGE}
    sustained_growth ${ADAPTIVE_SUSTAINED_GROWTH}
  }
}

queue_limits {
  max_depth ${QUEUE_MAX_DEPTH}
  drop_policy reject
}

/webhooks/github {
  queue { backend ${BACKEND} }
  pull {
    path /github
  }
}
EOF
}

wait_for_ready() {
	local admin_port="$1"
	local ready_url="http://127.0.0.1:${admin_port}/healthz"
	local n
	for n in $(seq 1 120); do
		if curl -fsS "${ready_url}" >/dev/null 2>&1; then
			return 0
		fi
		sleep 0.25
	done
	return 1
}

start_server() {
	local config_file="$1"
	local db_file="$2"
	local server_log="$3"

	"${BINARY}" run --config "${config_file}" --db "${db_file}" >"${server_log}" 2>&1 &
	ACTIVE_SERVER_PID="$!"
}

stop_server() {
	if [ -n "${ACTIVE_SERVER_PID}" ] && kill -0 "${ACTIVE_SERVER_PID}" >/dev/null 2>&1; then
		kill "${ACTIVE_SERVER_PID}" >/dev/null 2>&1 || true
		wait "${ACTIVE_SERVER_PID}" >/dev/null 2>&1 || true
	fi
	ACTIVE_SERVER_PID=""
}

stop_monitor() {
	if [ -n "${ACTIVE_MONITOR_PID}" ] && kill -0 "${ACTIVE_MONITOR_PID}" >/dev/null 2>&1; then
		kill "${ACTIVE_MONITOR_PID}" >/dev/null 2>&1 || true
		wait "${ACTIVE_MONITOR_PID}" >/dev/null 2>&1 || true
	fi
	ACTIVE_MONITOR_PID=""
}

stop_workers() {
	local pid
	for pid in ${ACTIVE_WORKER_PIDS}; do
		if kill -0 "${pid}" >/dev/null 2>&1; then
			kill "${pid}" >/dev/null 2>&1 || true
			wait "${pid}" >/dev/null 2>&1 || true
		fi
	done
	ACTIVE_WORKER_PIDS=""
}

cleanup() {
	stop_workers
	stop_monitor
	stop_server
}

start_monitor() {
	local run_dir="$1"
	local metrics_port="$2"
	local monitor_log="${run_dir}/monitor-output.log"
	local metrics_file="${run_dir}/.monitor-metrics.tmp"
	local adaptive
	local queue_full
	local ready_lag
	local oldest_age
	local pull_acked
	local pull_ack_conflict
	local pull_nack_conflict
	local pull_ack_conflict_ratio

	(
		while true; do
			if ! curl -fsS "http://127.0.0.1:${metrics_port}/metrics" >"${metrics_file}" 2>/dev/null; then
				sleep 1
				continue
			fi
			adaptive="$(fetch_metric_value "${metrics_file}" "hookaido_ingress_adaptive_backpressure_applied_total" "")"
			queue_full="$(fetch_metric_value "${metrics_file}" "hookaido_ingress_rejected_by_reason_total" "{reason=\"queue_full\",status=\"503\"}")"
			ready_lag="$(fetch_metric_value "${metrics_file}" "hookaido_queue_ready_lag_seconds" "")"
			oldest_age="$(fetch_metric_value "${metrics_file}" "hookaido_queue_oldest_queued_age_seconds" "")"
			pull_acked="$(sum_metric_value "${metrics_file}" "hookaido_pull_acked_total")"
			pull_ack_conflict="$(sum_metric_value "${metrics_file}" "hookaido_pull_ack_conflict_total")"
			pull_nack_conflict="$(sum_metric_value "${metrics_file}" "hookaido_pull_nack_conflict_total")"
			pull_ack_conflict_ratio="$(awk -v conflict="${pull_ack_conflict}" -v acked="${pull_acked}" 'BEGIN { if ((acked+0) <= 0) { print "0.000"; exit } printf "%.3f", ((conflict+0) * 100.0) / (acked+0) }')"
			printf '%s adaptive_applied=%s queue_full_503=%s ready_lag_s=%s oldest_queued_age_s=%s pull_acked=%s ack_conflict=%s nack_conflict=%s ack_conflict_ratio_pct=%s\n' \
				"$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
				"${adaptive}" \
				"${queue_full}" \
				"${ready_lag}" \
				"${oldest_age}" \
				"${pull_acked}" \
				"${pull_ack_conflict}" \
				"${pull_nack_conflict}" \
				"${pull_ack_conflict_ratio}" >>"${monitor_log}"
			sleep 1
		done
	) &
	ACTIVE_MONITOR_PID="$!"
}

ingress_worker() {
	local ingress_port="$1"
	local end_epoch="$2"
	local output_file="$3"
	local req_url="http://127.0.0.1:${ingress_port}/webhooks/github"
	local ts
	local result
	local status
	local latency

	while [ "$(date +%s)" -lt "${end_epoch}" ]; do
		ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
		result="$(
			curl -sS -o /dev/null -w '%{http_code}\t%{time_total}' \
				-X POST "${req_url}" \
				-H "Content-Type: application/json" \
				--data-binary '{"event":"adaptive-ab","source":"load"}' \
				--max-time 5 2>/dev/null || printf '000\t0'
		)"
		status="${result%%$'\t'*}"
		latency="${result#*$'\t'}"
		printf '%s\t%s\t%s\n' "${ts}" "${status}" "${latency}" >>"${output_file}"
	done
}

mixed_drain_worker() {
	local pull_port="$1"
	local end_epoch="$2"
	local route_base="http://127.0.0.1:${pull_port}/pull/github"
	local dequeue_body
	local dequeue_resp
	local lease_ids
	local ack_payload

	dequeue_body="$(printf '{"batch":%s,"lease_ttl":"30s","max_wait":"200ms"}' "${DEQUEUE_BATCH}")"
	while [ "$(date +%s)" -lt "${end_epoch}" ]; do
		dequeue_resp="$(
			curl -sS \
				-X POST "${route_base}/dequeue" \
				-H "Authorization: Bearer ${PULL_TOKEN}" \
				-H "Content-Type: application/json" \
				--data-binary "${dequeue_body}" \
				--max-time 5 2>/dev/null || printf ''
		)"
		lease_ids="$(
			printf '%s' "${dequeue_resp}" \
				| grep -o '"lease_id"[[:space:]]*:[[:space:]]*"[^"]*"' \
				| sed -E 's/.*:[[:space:]]*"([^"]*)"/"\1"/' \
				| paste -sd, - || true
		)"
		if [ -z "${lease_ids}" ]; then
			continue
		fi
		ack_payload="$(printf '{"lease_ids":[%s]}' "${lease_ids}")"
		curl -sS -o /dev/null \
			-X POST "${route_base}/ack" \
			-H "Authorization: Bearer ${PULL_TOKEN}" \
			-H "Content-Type: application/json" \
			--data-binary "${ack_payload}" \
			--max-time 5 >/dev/null 2>&1 || true
	done
}

run_load() {
	local scenario="$1"
	local ingress_port="$2"
	local pull_port="$3"
	local ingress_results="$4"
	local end_epoch="$5"
	local i

	: >"${ingress_results}"
	for i in $(seq 1 "${INGRESS_WORKERS}"); do
		ingress_worker "${ingress_port}" "${end_epoch}" "${ingress_results}" &
		ACTIVE_WORKER_PIDS="${ACTIVE_WORKER_PIDS} $!"
	done

	if [ "${scenario}" = "mixed" ]; then
		for i in $(seq 1 "${MIXED_DRAIN_WORKERS}"); do
			mixed_drain_worker "${pull_port}" "${end_epoch}" &
			ACTIVE_WORKER_PIDS="${ACTIVE_WORKER_PIDS} $!"
		done
	fi

	local pid
	for pid in ${ACTIVE_WORKER_PIDS}; do
		wait "${pid}" >/dev/null 2>&1 || true
	done
	ACTIVE_WORKER_PIDS=""
}

write_run_summary() {
	local run_dir="$1"
	local scenario="$2"
	local mode="$3"
	local started_epoch="$4"
	local ended_epoch="$5"
	local metrics_file="${run_dir}/final-metrics.txt"
	local ingress_results="${run_dir}/ingress-results.tsv"

	local adaptive_applied
	local adaptive_reject
	local queue_full_reject
	local ready_lag
	local oldest_age
	local accepted_total
	local accepted_rate
	local latency_p95
	local latency_p99
	local total_requests
	local elapsed_seconds
	local pull_acked_total
	local pull_ack_conflict_total
	local pull_nack_conflict_total
	local pull_ack_conflict_ratio_percent

	adaptive_applied="$(fetch_metric_value "${metrics_file}" "hookaido_ingress_adaptive_backpressure_applied_total" "")"
	adaptive_reject="$(fetch_metric_value "${metrics_file}" "hookaido_ingress_rejected_by_reason_total" "{reason=\"adaptive_backpressure\",status=\"503\"}")"
	queue_full_reject="$(fetch_metric_value "${metrics_file}" "hookaido_ingress_rejected_by_reason_total" "{reason=\"queue_full\",status=\"503\"}")"
	ready_lag="$(fetch_metric_value "${metrics_file}" "hookaido_queue_ready_lag_seconds" "")"
	oldest_age="$(fetch_metric_value "${metrics_file}" "hookaido_queue_oldest_queued_age_seconds" "")"
	accepted_total="$(awk -F '\t' '$2 == "202" { c++ } END { print c + 0 }' "${ingress_results}")"
	total_requests="$(awk 'END { print NR + 0 }' "${ingress_results}")"
	elapsed_seconds=$(( ended_epoch - started_epoch ))
	if [ "${elapsed_seconds}" -le 0 ]; then
		elapsed_seconds=1
	fi
	accepted_rate="$(awk -v accepted="${accepted_total}" -v elapsed="${elapsed_seconds}" 'BEGIN { printf "%.3f", accepted / elapsed }')"
	latency_p95="$(compute_latency_percentile_ms "${ingress_results}" 95)"
	latency_p99="$(compute_latency_percentile_ms "${ingress_results}" 99)"
	pull_acked_total="$(sum_metric_value "${metrics_file}" "hookaido_pull_acked_total")"
	pull_ack_conflict_total="$(sum_metric_value "${metrics_file}" "hookaido_pull_ack_conflict_total")"
	pull_nack_conflict_total="$(sum_metric_value "${metrics_file}" "hookaido_pull_nack_conflict_total")"
	pull_ack_conflict_ratio_percent="$(awk -v conflict="${pull_ack_conflict_total}" -v acked="${pull_acked_total}" 'BEGIN { if ((acked+0) <= 0) { print "0.000"; exit } printf "%.3f", ((conflict+0) * 100.0) / (acked+0) }')"

	cat >"${run_dir}/summary.env" <<EOF
scenario=${scenario}
mode=${mode}
duration_seconds=${elapsed_seconds}
total_requests=${total_requests}
accepted_total=${accepted_total}
accepted_rate_per_second=${accepted_rate}
latency_p95_ms=${latency_p95}
latency_p99_ms=${latency_p99}
adaptive_applied_total=${adaptive_applied}
adaptive_reject_503_total=${adaptive_reject}
queue_full_reject_503_total=${queue_full_reject}
queue_ready_lag_seconds=${ready_lag}
queue_oldest_queued_age_seconds=${oldest_age}
pull_acked_total=${pull_acked_total}
pull_ack_conflict_total=${pull_ack_conflict_total}
pull_nack_conflict_total=${pull_nack_conflict_total}
pull_ack_conflict_ratio_percent=${pull_ack_conflict_ratio_percent}
EOF

	cat >"${run_dir}/summary.json" <<EOF
{
  "scenario": "${scenario}",
  "mode": "${mode}",
  "duration_seconds": ${elapsed_seconds},
  "total_requests": ${total_requests},
  "accepted_total": ${accepted_total},
  "accepted_rate_per_second": ${accepted_rate},
  "latency_p95_ms": ${latency_p95},
  "latency_p99_ms": ${latency_p99},
  "metrics": {
    "hookaido_ingress_adaptive_backpressure_applied_total": ${adaptive_applied},
    "hookaido_ingress_rejected_by_reason_total{reason=\"adaptive_backpressure\",status=\"503\"}": ${adaptive_reject},
    "hookaido_ingress_rejected_by_reason_total{reason=\"queue_full\",status=\"503\"}": ${queue_full_reject},
    "hookaido_queue_ready_lag_seconds": ${ready_lag},
    "hookaido_queue_oldest_queued_age_seconds": ${oldest_age},
    "hookaido_pull_acked_total_sum_routes": ${pull_acked_total},
    "hookaido_pull_ack_conflict_total_sum_routes": ${pull_ack_conflict_total},
    "hookaido_pull_nack_conflict_total_sum_routes": ${pull_nack_conflict_total},
    "pull_ack_conflict_ratio_percent": ${pull_ack_conflict_ratio_percent}
  }
}
EOF
}

run_one_mode() {
	local scenario="$1"
	local mode="$2"
	local slot="$3"

	local ingress_port="$((18080 + slot * 10))"
	local pull_port="$((19443 + slot * 10))"
	local admin_port="$((12019 + slot * 10))"
	local metrics_port="$((19900 + slot * 10))"
	local run_dir="${RUN_ROOT}/${scenario}-${mode}"
	local config_file="${run_dir}/Hookaidofile"
	local db_file="${run_dir}/hookaido.db"
	local server_log="${run_dir}/server.log"
	local started_epoch
	local ended_epoch
	local end_epoch
	local adaptive_enabled

	mkdir -p "${run_dir}"
	adaptive_enabled="off"
	if [ "${mode}" = "on" ]; then
		adaptive_enabled="on"
	fi
	write_config "${config_file}" "${ingress_port}" "${pull_port}" "${admin_port}" "${metrics_port}" "${adaptive_enabled}"
	create_run_meta "${run_dir}" "${scenario}" "${mode}" "${ingress_port}" "${pull_port}" "${admin_port}" "${metrics_port}" "${config_file}"

	start_server "${config_file}" "${db_file}" "${server_log}"
	if ! wait_for_ready "${admin_port}"; then
		stop_server
		error "hookaido failed to start for ${scenario}-${mode}; inspect ${server_log}"
	fi

	start_monitor "${run_dir}" "${metrics_port}"
	started_epoch="$(date +%s)"
	end_epoch=$(( started_epoch + DURATION_SECONDS ))
	run_load "${scenario}" "${ingress_port}" "${pull_port}" "${run_dir}/ingress-results.tsv" "${end_epoch}"
	ended_epoch="$(date +%s)"

	curl -fsS "http://127.0.0.1:${metrics_port}/metrics" >"${run_dir}/final-metrics.txt"
	curl -fsS "http://127.0.0.1:${admin_port}/healthz?details=1" >"${run_dir}/final-health.json"

	stop_monitor
	stop_server
	write_run_summary "${run_dir}" "${scenario}" "${mode}" "${started_epoch}" "${ended_epoch}"
}

write_run_index() {
	local out_file="${RUN_ROOT}/run-index.md"
	cat >"${out_file}" <<EOF
# Adaptive Backpressure A/B Run Index

- run id: \`${RUN_ID}\`
- timestamp (UTC): \`$(date -u +%Y-%m-%dT%H:%M:%SZ)\`
- binary: \`${BINARY}\`
- backend: \`${BACKEND}\`
- duration per run: \`${DURATION_SECONDS}s\`
- ingress workers: \`${INGRESS_WORKERS}\`
- mixed drain workers: \`${MIXED_DRAIN_WORKERS}\`
- dequeue batch: \`${DEQUEUE_BATCH}\`
- queue max depth: \`${QUEUE_MAX_DEPTH}\`

Comparisons:
EOF
	if [ "${SCENARIO}" = "pull" ] || [ "${SCENARIO}" = "all" ]; then
		printf -- '- [pull](comparison-pull.md)\n' >>"${out_file}"
	fi
	if [ "${SCENARIO}" = "mixed" ] || [ "${SCENARIO}" = "all" ]; then
		printf -- '- [mixed](comparison-mixed.md)\n' >>"${out_file}"
	fi
}

write_aggregate_comparison() {
	local out_file="${RUN_ROOT}/comparison.md"
	: >"${out_file}"
	if [ -f "${RUN_ROOT}/comparison-pull.md" ]; then
		cat "${RUN_ROOT}/comparison-pull.md" >>"${out_file}"
		printf '\n\n' >>"${out_file}"
	fi
	if [ -f "${RUN_ROOT}/comparison-mixed.md" ]; then
		cat "${RUN_ROOT}/comparison-mixed.md" >>"${out_file}"
		printf '\n' >>"${out_file}"
	fi
}

main() {
	require_cmd awk
	require_cmd curl
	require_cmd date
	require_cmd grep
	require_cmd mktemp
	require_cmd paste
	require_cmd seq
	require_cmd sed
	require_cmd sha256sum
	require_cmd sort
	require_cmd wc

	parse_args "$@"
	validate_args

	RUN_ROOT="${OUTPUT_ROOT}/${RUN_ID}"
	mkdir -p "${RUN_ROOT}"

	trap cleanup EXIT INT TERM

	write_run_index

	local slot=0
	local off_summary
	local on_summary

	if [ "${SCENARIO}" = "pull" ] || [ "${SCENARIO}" = "all" ]; then
		run_one_mode "pull" "off" "${slot}"
		slot=$((slot + 1))
		run_one_mode "pull" "on" "${slot}"
		slot=$((slot + 1))
		off_summary="${RUN_ROOT}/pull-off/summary.env"
		on_summary="${RUN_ROOT}/pull-on/summary.env"
		render_comparison "pull" "${off_summary}" "${on_summary}" "${RUN_ROOT}/comparison-pull.md"
	fi

	if [ "${SCENARIO}" = "mixed" ] || [ "${SCENARIO}" = "all" ]; then
		run_one_mode "mixed" "off" "${slot}"
		slot=$((slot + 1))
		run_one_mode "mixed" "on" "${slot}"
		slot=$((slot + 1))
		off_summary="${RUN_ROOT}/mixed-off/summary.env"
		on_summary="${RUN_ROOT}/mixed-on/summary.env"
		render_comparison "mixed" "${off_summary}" "${on_summary}" "${RUN_ROOT}/comparison-mixed.md"
	fi

	write_aggregate_comparison
	printf 'adaptive A/B artifacts written to %s\n' "${RUN_ROOT}"
}

main "$@"

#!/usr/bin/env bash
set -euo pipefail

RUN_ROOT=""
SCENARIO="mixed"
MAX_READY_LAG_SECONDS="30"
MAX_OLDEST_QUEUED_AGE_SECONDS="30"
MAX_READY_LAG_DELTA_SECONDS="10"
MAX_OLDEST_QUEUED_AGE_DELTA_SECONDS="10"
MIN_ACCEPTED_TOTAL="100"

usage() {
	cat <<'EOF'
Usage:
  scripts/adaptive-lag-guardrail.sh --run-root <path> [options]

Validates queue lag/age guardrails from scripts/adaptive-ab.sh summary artifacts.

Required:
  --run-root <path>                           Path like ./.artifacts/adaptive-ab/<run-id>

Options:
  --scenario <mixed|pull>                     Scenario to evaluate (default: mixed)
  --max-ready-lag-seconds <n>                 Maximum allowed queue_ready_lag_seconds per mode (default: 30)
  --max-oldest-queued-age-seconds <n>         Maximum allowed queue_oldest_queued_age_seconds per mode (default: 30)
  --max-ready-lag-delta-seconds <n>           Maximum allowed delta (on-off) for queue_ready_lag_seconds (default: 10)
  --max-oldest-queued-age-delta-seconds <n>   Maximum allowed delta (on-off) for queue_oldest_queued_age_seconds (default: 10)
  --min-accepted-total <n>                    Minimum accepted_total required per mode (default: 100)
  --help                                      Show this help

Exit codes:
  0  Guardrail passed for both off/on modes
  1  Guardrail failed
  2  Invalid arguments or missing artifacts
EOF
}

fail() {
	printf 'guardrail fail: %s\n' "$*" >&2
	exit 1
}

arg_error() {
	printf 'guardrail error: %s\n' "$*" >&2
	exit 2
}

parse_args() {
	while [ "$#" -gt 0 ]; do
		case "$1" in
		--run-root)
			shift
			RUN_ROOT="${1:-}"
			;;
		--scenario)
			shift
			SCENARIO="${1:-}"
			;;
		--max-ready-lag-seconds)
			shift
			MAX_READY_LAG_SECONDS="${1:-}"
			;;
		--max-oldest-queued-age-seconds)
			shift
			MAX_OLDEST_QUEUED_AGE_SECONDS="${1:-}"
			;;
		--max-ready-lag-delta-seconds)
			shift
			MAX_READY_LAG_DELTA_SECONDS="${1:-}"
			;;
		--max-oldest-queued-age-delta-seconds)
			shift
			MAX_OLDEST_QUEUED_AGE_DELTA_SECONDS="${1:-}"
			;;
		--min-accepted-total)
			shift
			MIN_ACCEPTED_TOTAL="${1:-}"
			;;
		--help|-h)
			usage
			exit 0
			;;
		*)
			arg_error "unknown argument: $1"
			;;
		esac
		shift || true
	done
}

validate_args() {
	[ -n "${RUN_ROOT}" ] || arg_error "--run-root is required"
	[ -d "${RUN_ROOT}" ] || arg_error "run root does not exist: ${RUN_ROOT}"
	case "${SCENARIO}" in
	mixed|pull) ;;
	*) arg_error "--scenario must be mixed|pull (got: ${SCENARIO})" ;;
	esac
	[[ "${MAX_READY_LAG_SECONDS}" =~ ^[0-9]+([.][0-9]+)?$ ]] || arg_error "--max-ready-lag-seconds must be numeric"
	[[ "${MAX_OLDEST_QUEUED_AGE_SECONDS}" =~ ^[0-9]+([.][0-9]+)?$ ]] || arg_error "--max-oldest-queued-age-seconds must be numeric"
	[[ "${MAX_READY_LAG_DELTA_SECONDS}" =~ ^[0-9]+([.][0-9]+)?$ ]] || arg_error "--max-ready-lag-delta-seconds must be numeric"
	[[ "${MAX_OLDEST_QUEUED_AGE_DELTA_SECONDS}" =~ ^[0-9]+([.][0-9]+)?$ ]] || arg_error "--max-oldest-queued-age-delta-seconds must be numeric"
	[[ "${MIN_ACCEPTED_TOTAL}" =~ ^[0-9]+$ ]] || arg_error "--min-accepted-total must be an integer"
}

summary_get() {
	local summary_file="$1"
	local key="$2"
	sed -n "s/^${key}=//p" "${summary_file}" | head -n1
}

validate_mode() {
	local mode="$1"
	local summary_file="${RUN_ROOT}/${SCENARIO}-${mode}/summary.env"
	local accepted_total
	local ready_lag
	local oldest_age
	local ready_ok
	local oldest_ok

	[ -f "${summary_file}" ] || arg_error "missing summary file: ${summary_file}"

	accepted_total="$(summary_get "${summary_file}" "accepted_total")"
	ready_lag="$(summary_get "${summary_file}" "queue_ready_lag_seconds")"
	oldest_age="$(summary_get "${summary_file}" "queue_oldest_queued_age_seconds")"

	[[ "${accepted_total}" =~ ^[0-9]+$ ]] || arg_error "invalid accepted_total in ${summary_file}: ${accepted_total}"
	[[ "${ready_lag}" =~ ^[0-9]+([.][0-9]+)?$ ]] || arg_error "invalid queue_ready_lag_seconds in ${summary_file}: ${ready_lag}"
	[[ "${oldest_age}" =~ ^[0-9]+([.][0-9]+)?$ ]] || arg_error "invalid queue_oldest_queued_age_seconds in ${summary_file}: ${oldest_age}"

	if [ "${accepted_total}" -lt "${MIN_ACCEPTED_TOTAL}" ]; then
		fail "${SCENARIO}-${mode}: insufficient accepted_total (${accepted_total} < ${MIN_ACCEPTED_TOTAL})"
	fi

	ready_ok="$(
		awk -v v="${ready_lag}" -v max="${MAX_READY_LAG_SECONDS}" 'BEGIN { if ((v+0) <= (max+0)) print "yes"; else print "no" }'
	)"
	if [ "${ready_ok}" != "yes" ]; then
		fail "${SCENARIO}-${mode}: queue_ready_lag_seconds ${ready_lag} exceeds threshold ${MAX_READY_LAG_SECONDS}"
	fi

	oldest_ok="$(
		awk -v v="${oldest_age}" -v max="${MAX_OLDEST_QUEUED_AGE_SECONDS}" 'BEGIN { if ((v+0) <= (max+0)) print "yes"; else print "no" }'
	)"
	if [ "${oldest_ok}" != "yes" ]; then
		fail "${SCENARIO}-${mode}: queue_oldest_queued_age_seconds ${oldest_age} exceeds threshold ${MAX_OLDEST_QUEUED_AGE_SECONDS}"
	fi

	printf '%s\t%s\t%s\t%s\n' "${SCENARIO}-${mode}" "${accepted_total}" "${ready_lag}" "${oldest_age}"
}

main() {
	parse_args "$@"
	validate_args

	local report_file="${RUN_ROOT}/guardrail-lag-${SCENARIO}.md"
	local row_off
	local row_on
	local ready_off
	local ready_on
	local oldest_off
	local oldest_on
	local ready_delta
	local oldest_delta
	local ready_delta_ok
	local oldest_delta_ok

	row_off="$(validate_mode "off")"
	row_on="$(validate_mode "on")"

	ready_off="$(printf '%s' "${row_off}" | awk -F '\t' '{print $3}')"
	ready_on="$(printf '%s' "${row_on}" | awk -F '\t' '{print $3}')"
	oldest_off="$(printf '%s' "${row_off}" | awk -F '\t' '{print $4}')"
	oldest_on="$(printf '%s' "${row_on}" | awk -F '\t' '{print $4}')"

	ready_delta="$(awk -v on="${ready_on}" -v off="${ready_off}" 'BEGIN { printf "%.3f", (on+0)-(off+0) }')"
	oldest_delta="$(awk -v on="${oldest_on}" -v off="${oldest_off}" 'BEGIN { printf "%.3f", (on+0)-(off+0) }')"

	ready_delta_ok="$(
		awk -v delta="${ready_delta}" -v max="${MAX_READY_LAG_DELTA_SECONDS}" 'BEGIN { if ((delta+0) <= (max+0)) print "yes"; else print "no" }'
	)"
	if [ "${ready_delta_ok}" != "yes" ]; then
		fail "${SCENARIO}: queue_ready_lag_seconds delta(on-off) ${ready_delta} exceeds threshold ${MAX_READY_LAG_DELTA_SECONDS}"
	fi

	oldest_delta_ok="$(
		awk -v delta="${oldest_delta}" -v max="${MAX_OLDEST_QUEUED_AGE_DELTA_SECONDS}" 'BEGIN { if ((delta+0) <= (max+0)) print "yes"; else print "no" }'
	)"
	if [ "${oldest_delta_ok}" != "yes" ]; then
		fail "${SCENARIO}: queue_oldest_queued_age_seconds delta(on-off) ${oldest_delta} exceeds threshold ${MAX_OLDEST_QUEUED_AGE_DELTA_SECONDS}"
	fi

	cat >"${report_file}" <<EOF
# Queue Lag/Age Guardrail (${SCENARIO})

- run root: \`${RUN_ROOT}\`
- max queue_ready_lag_seconds per mode: \`${MAX_READY_LAG_SECONDS}\`
- max queue_oldest_queued_age_seconds per mode: \`${MAX_OLDEST_QUEUED_AGE_SECONDS}\`
- max queue_ready_lag_seconds delta (on-off): \`${MAX_READY_LAG_DELTA_SECONDS}\`
- max queue_oldest_queued_age_seconds delta (on-off): \`${MAX_OLDEST_QUEUED_AGE_DELTA_SECONDS}\`
- minimum accepted_total per mode: \`${MIN_ACCEPTED_TOTAL}\`

| Mode | accepted_total | queue_ready_lag_seconds | queue_oldest_queued_age_seconds |
| --- | ---: | ---: | ---: |
| $(printf '%s' "${row_off}" | awk -F '\t' '{print $1}') | $(printf '%s' "${row_off}" | awk -F '\t' '{print $2}') | $(printf '%s' "${row_off}" | awk -F '\t' '{print $3}') | $(printf '%s' "${row_off}" | awk -F '\t' '{print $4}') |
| $(printf '%s' "${row_on}" | awk -F '\t' '{print $1}') | $(printf '%s' "${row_on}" | awk -F '\t' '{print $2}') | $(printf '%s' "${row_on}" | awk -F '\t' '{print $3}') | $(printf '%s' "${row_on}" | awk -F '\t' '{print $4}') |

| Delta | queue_ready_lag_seconds (on-off) | queue_oldest_queued_age_seconds (on-off) |
| --- | ---: | ---: |
| \`${SCENARIO}\` | ${ready_delta} | ${oldest_delta} |
EOF

	printf 'guardrail pass: %s lag-age (report: %s)\n' "${SCENARIO}" "${report_file}"
}

main "$@"

#!/usr/bin/env bash
set -euo pipefail

RUN_ROOT=""
SCENARIO="mixed"
MAX_ACK_CONFLICT_RATIO_PERCENT="5.0"
MIN_ACKED_TOTAL="100"
MAX_ROUTE_ACK_CONFLICT_RATIO_PERCENT="5.0"
MIN_ROUTE_ACKED_TOTAL="50"

usage() {
	cat <<'EOF'
Usage:
  scripts/adaptive-guardrail.sh --run-root <path> [options]

Validates mixed/pull A/B guardrail metrics produced by scripts/adaptive-ab.sh.

Required:
  --run-root <path>                     Path like ./.artifacts/adaptive-ab/<run-id>

Options:
  --scenario <mixed|pull>               Scenario to evaluate (default: mixed)
  --max-ack-conflict-ratio <percent>    Maximum allowed pull_ack_conflict_ratio_percent (default: 5.0)
  --min-acked-total <n>                 Minimum pull_acked_total required per mode (default: 100)
  --max-route-ack-conflict-ratio <pct>  Maximum allowed per-route pull_ack_conflict_ratio_percent (default: 5.0)
  --min-route-acked-total <n>           Minimum pull_acked_total required per route for threshold checks (default: 50)
  --help                                Show this help

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
		--max-ack-conflict-ratio)
			shift
			MAX_ACK_CONFLICT_RATIO_PERCENT="${1:-}"
			;;
		--min-acked-total)
			shift
			MIN_ACKED_TOTAL="${1:-}"
			;;
		--max-route-ack-conflict-ratio)
			shift
			MAX_ROUTE_ACK_CONFLICT_RATIO_PERCENT="${1:-}"
			;;
		--min-route-acked-total)
			shift
			MIN_ROUTE_ACKED_TOTAL="${1:-}"
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
	[[ "${MAX_ACK_CONFLICT_RATIO_PERCENT}" =~ ^[0-9]+([.][0-9]+)?$ ]] || arg_error "--max-ack-conflict-ratio must be numeric"
	[[ "${MIN_ACKED_TOTAL}" =~ ^[0-9]+$ ]] || arg_error "--min-acked-total must be an integer"
	[[ "${MAX_ROUTE_ACK_CONFLICT_RATIO_PERCENT}" =~ ^[0-9]+([.][0-9]+)?$ ]] || arg_error "--max-route-ack-conflict-ratio must be numeric"
	[[ "${MIN_ROUTE_ACKED_TOTAL}" =~ ^[0-9]+$ ]] || arg_error "--min-route-acked-total must be an integer"
}

summary_get() {
	local summary_file="$1"
	local key="$2"
	sed -n "s/^${key}=//p" "${summary_file}" | head -n1
}

extract_route_rows() {
	local metrics_file="$1"
	local rows_file="$2"
	awk '
		function route_from_sample(sample, out) {
			if (match(sample, /route="[^"]+"/) == 0) {
				return ""
			}
			out = substr(sample, RSTART + 7, RLENGTH - 8)
			return out
		}
		$1 ~ /^hookaido_pull_acked_total\{/ {
			route = route_from_sample($1)
			if (route != "") {
				acked[route] = $2 + 0
				routes[route] = 1
			}
		}
		$1 ~ /^hookaido_pull_ack_conflict_total\{/ {
			route = route_from_sample($1)
			if (route != "") {
				conflicts[route] = $2 + 0
				routes[route] = 1
			}
		}
		END {
			for (route in routes) {
				acked_total = acked[route] + 0
				conflict_total = conflicts[route] + 0
				ratio = 0
				if (acked_total > 0) {
					ratio = (conflict_total * 100.0) / acked_total
				}
				printf "%s\t%.0f\t%.0f\t%.3f\n", route, acked_total, conflict_total, ratio
			}
		}
	' "${metrics_file}" | sort >"${rows_file}"
}

validate_route_rows() {
	local mode="$1"
	local rows_file="$2"
	local route
	local acked
	local conflicts
	local ratio
	local ratio_ok

	while IFS=$'\t' read -r route acked conflicts ratio; do
		[ -n "${route}" ] || continue
		[[ "${acked}" =~ ^[0-9]+$ ]] || arg_error "invalid per-route pull_acked_total for ${SCENARIO}-${mode} route ${route}: ${acked}"
		[[ "${conflicts}" =~ ^[0-9]+$ ]] || arg_error "invalid per-route pull_ack_conflict_total for ${SCENARIO}-${mode} route ${route}: ${conflicts}"
		[[ "${ratio}" =~ ^[0-9]+([.][0-9]+)?$ ]] || arg_error "invalid per-route ratio for ${SCENARIO}-${mode} route ${route}: ${ratio}"
		if [ "${acked}" -lt "${MIN_ROUTE_ACKED_TOTAL}" ]; then
			continue
		fi
		ratio_ok="$(
			awk -v ratio="${ratio}" -v max="${MAX_ROUTE_ACK_CONFLICT_RATIO_PERCENT}" 'BEGIN { if ((ratio+0) <= (max+0)) print "yes"; else print "no" }'
		)"
		if [ "${ratio_ok}" != "yes" ]; then
			fail "${SCENARIO}-${mode}: route ${route} pull_ack_conflict_ratio_percent ${ratio} exceeds per-route threshold ${MAX_ROUTE_ACK_CONFLICT_RATIO_PERCENT}"
		fi
	done <"${rows_file}"
}

render_route_rows_markdown() {
	local rows_file="$1"
	local route
	local acked
	local conflicts
	local ratio

	if [ ! -s "${rows_file}" ]; then
		printf '| (none) | 0 | 0 | 0.000 |\n'
		return
	fi

	while IFS=$'\t' read -r route acked conflicts ratio; do
		[ -n "${route}" ] || continue
		printf '| `%s` | %s | %s | %s |\n' "${route}" "${acked}" "${conflicts}" "${ratio}"
	done <"${rows_file}"
}

validate_mode() {
	local mode="$1"
	local route_rows_file="$2"
	local summary_file="${RUN_ROOT}/${SCENARIO}-${mode}/summary.env"
	local metrics_file="${RUN_ROOT}/${SCENARIO}-${mode}/final-metrics.txt"
	local acked
	local conflicts
	local ratio
	local ratio_ok

	[ -f "${summary_file}" ] || arg_error "missing summary file: ${summary_file}"
	[ -f "${metrics_file}" ] || arg_error "missing metrics file: ${metrics_file}"

	acked="$(summary_get "${summary_file}" "pull_acked_total")"
	conflicts="$(summary_get "${summary_file}" "pull_ack_conflict_total")"
	ratio="$(summary_get "${summary_file}" "pull_ack_conflict_ratio_percent")"

	[[ "${acked}" =~ ^[0-9]+$ ]] || arg_error "invalid pull_acked_total in ${summary_file}: ${acked}"
	[[ "${conflicts}" =~ ^[0-9]+$ ]] || arg_error "invalid pull_ack_conflict_total in ${summary_file}: ${conflicts}"
	[[ "${ratio}" =~ ^[0-9]+([.][0-9]+)?$ ]] || arg_error "invalid pull_ack_conflict_ratio_percent in ${summary_file}: ${ratio}"

	if [ "${acked}" -lt "${MIN_ACKED_TOTAL}" ]; then
		fail "${SCENARIO}-${mode}: insufficient pull_acked_total (${acked} < ${MIN_ACKED_TOTAL})"
	fi

	ratio_ok="$(
		awk -v ratio="${ratio}" -v max="${MAX_ACK_CONFLICT_RATIO_PERCENT}" 'BEGIN { if ((ratio+0) <= (max+0)) print "yes"; else print "no" }'
	)"
	if [ "${ratio_ok}" != "yes" ]; then
		fail "${SCENARIO}-${mode}: pull_ack_conflict_ratio_percent ${ratio} exceeds threshold ${MAX_ACK_CONFLICT_RATIO_PERCENT}"
	fi

	extract_route_rows "${metrics_file}" "${route_rows_file}"
	validate_route_rows "${mode}" "${route_rows_file}"

	printf '%s\t%s\t%s\t%s\n' "${SCENARIO}-${mode}" "${acked}" "${conflicts}" "${ratio}"
}

main() {
	parse_args "$@"
	validate_args

	local report_file="${RUN_ROOT}/guardrail-${SCENARIO}.md"
	local route_rows_off_file
	local route_rows_on_file
	local route_rows_off
	local route_rows_on
	local row_off
	local row_on

	route_rows_off_file="$(mktemp)"
	route_rows_on_file="$(mktemp)"
	trap 'rm -f "${route_rows_off_file:-}" "${route_rows_on_file:-}"' EXIT

	row_off="$(validate_mode "off" "${route_rows_off_file}")"
	row_on="$(validate_mode "on" "${route_rows_on_file}")"
	route_rows_off="$(render_route_rows_markdown "${route_rows_off_file}")"
	route_rows_on="$(render_route_rows_markdown "${route_rows_on_file}")"

	cat >"${report_file}" <<EOF
# Pull ACK Conflict Guardrail (${SCENARIO})

- run root: \`${RUN_ROOT}\`
- max ratio threshold: \`${MAX_ACK_CONFLICT_RATIO_PERCENT}%\`
- minimum acked per mode: \`${MIN_ACKED_TOTAL}\`
- max per-route ratio threshold: \`${MAX_ROUTE_ACK_CONFLICT_RATIO_PERCENT}%\`
- minimum acked per route for threshold checks: \`${MIN_ROUTE_ACKED_TOTAL}\`

| Mode | pull_acked_total | pull_ack_conflict_total | pull_ack_conflict_ratio_percent |
| --- | ---: | ---: | ---: |
| $(printf '%s' "${row_off}" | awk -F '\t' '{print $1}') | $(printf '%s' "${row_off}" | awk -F '\t' '{print $2}') | $(printf '%s' "${row_off}" | awk -F '\t' '{print $3}') | $(printf '%s' "${row_off}" | awk -F '\t' '{print $4}') |
| $(printf '%s' "${row_on}" | awk -F '\t' '{print $1}') | $(printf '%s' "${row_on}" | awk -F '\t' '{print $2}') | $(printf '%s' "${row_on}" | awk -F '\t' '{print $3}') | $(printf '%s' "${row_on}" | awk -F '\t' '{print $4}') |

## Route Drill-down (${SCENARIO}-off)

| Route | pull_acked_total | pull_ack_conflict_total | pull_ack_conflict_ratio_percent |
| --- | ---: | ---: | ---: |
${route_rows_off}

## Route Drill-down (${SCENARIO}-on)

| Route | pull_acked_total | pull_ack_conflict_total | pull_ack_conflict_ratio_percent |
| --- | ---: | ---: | ---: |
${route_rows_on}
EOF

	printf 'guardrail pass: %s (report: %s)\n' "${SCENARIO}" "${report_file}"
}

main "$@"

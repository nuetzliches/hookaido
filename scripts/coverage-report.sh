#!/usr/bin/env bash
# Summarize a `go test -coverprofile` profile per package, most uncovered
# statements first, plus a repo total measured against the BACKLOG.md target.
#
# Uncovered-statement count is the number that matters for the repo total: a
# package at 62% with 1200 uncovered statements moves it far more than one at
# 62% with 40, and a percentage column alone hides that.
#
# Usage: scripts/coverage-report.sh [profile] [target-percent]
set -euo pipefail

PROFILE="${1:-coverage.out}"
TARGET="${2:-80.0}"

# Generated code is never hand-tested, so counting it only makes the number lie
# about the code we actually write. Extend this ERE when a new generated package
# appears; it is matched against the module-relative package path.
EXCLUDE_RE="${COVER_EXCLUDE:-^modules/grpcworker/proto$}"
MODULE="${COVER_MODULE:-github.com/nuetzliches/hookaido/v2}"

if [[ ! -f "$PROFILE" ]]; then
	echo "coverage profile not found: $PROFILE" >&2
	echo "generate one with: make cover" >&2
	exit 2
fi

awk -F: -v module="$MODULE" -v exclude="$EXCLUDE_RE" -v target="$TARGET" '
	# Profile data lines look like:
	#   <import/path>/<file>.go:<startline>.<col>,<endline>.<col> <numstmt> <count>
	# The leading mode: line is the only non-data line; skip it by name rather
	# than by line number so a reordered profile still parses.
	$0 ~ /^mode:/ { next }
	{
		split($0, fields, " ")
		numstmt = fields[2]
		count = fields[3]

		pkg = $1
		sub(/\/[^\/]*$/, "", pkg)
		sub("^" module "/", "", pkg)
		if (pkg ~ exclude) { excluded += numstmt; next }

		total[pkg] += numstmt
		grand_total += numstmt
		if (count + 0 > 0) {
			covered[pkg] += numstmt
			grand_covered += numstmt
		}
	}
	END {
		if (grand_total == 0) {
			print "no statements found in profile" > "/dev/stderr"
			exit 1
		}
		# Pipe the per-package rows through sort so the summary below stays
		# pinned to the bottom instead of being ranked in among them.
		sorter = "sort -k2 -rn"
		for (pkg in total) {
			printf "%-34s %5d uncovered  %5d total  %5.1f%%\n", \
				pkg, total[pkg] - covered[pkg], total[pkg], \
				100 * covered[pkg] / total[pkg] | sorter
		}
		close(sorter)
		printf "\n%-34s %5d uncovered  %5d total  %5.1f%%  (target %s%%)\n", \
			"TOTAL", grand_total - grand_covered, grand_total, \
			100 * grand_covered / grand_total, target
		if (excluded > 0) {
			printf "%-34s %5d statements in generated packages, not counted (%s)\n", \
				"excluded", excluded, exclude
		}
	}
' "$PROFILE"

package config

import (
	"fmt"
	"strings"
)

// DiffOp represents a single line-level diff operation.
type DiffOp struct {
	Kind  byte // '=', '-', '+'
	Text  string
	OldNo int
	NewNo int
}

// NormalizedDiffLines splits text into lines, normalizing CRLF to LF and
// stripping a final empty element caused by a trailing newline.
func NormalizedDiffLines(s string) []string {
	s = strings.ReplaceAll(s, "\r\n", "\n")
	lines := strings.Split(s, "\n")
	if len(lines) > 0 && lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	return lines
}

// LineDiffOps computes a line-level diff using the LCS algorithm and returns
// a sequence of DiffOp values annotated with 1-based line numbers.
func LineDiffOps(oldLines, newLines []string) []DiffOp {
	n := len(oldLines)
	m := len(newLines)
	dp := make([][]int, n+1)
	for i := range dp {
		dp[i] = make([]int, m+1)
	}
	for i := n - 1; i >= 0; i-- {
		for j := m - 1; j >= 0; j-- {
			if oldLines[i] == newLines[j] {
				dp[i][j] = dp[i+1][j+1] + 1
				continue
			}
			if dp[i+1][j] >= dp[i][j+1] {
				dp[i][j] = dp[i+1][j]
			} else {
				dp[i][j] = dp[i][j+1]
			}
		}
	}

	i, j := 0, 0
	oldNo, newNo := 1, 1
	ops := make([]DiffOp, 0, n+m)
	for i < n && j < m {
		if oldLines[i] == newLines[j] {
			ops = append(ops, DiffOp{Kind: '=', Text: oldLines[i], OldNo: oldNo, NewNo: newNo})
			i++
			j++
			oldNo++
			newNo++
			continue
		}
		if dp[i+1][j] >= dp[i][j+1] {
			ops = append(ops, DiffOp{Kind: '-', Text: oldLines[i], OldNo: oldNo, NewNo: newNo})
			i++
			oldNo++
		} else {
			ops = append(ops, DiffOp{Kind: '+', Text: newLines[j], OldNo: oldNo, NewNo: newNo})
			j++
			newNo++
		}
	}
	for i < n {
		ops = append(ops, DiffOp{Kind: '-', Text: oldLines[i], OldNo: oldNo, NewNo: newNo})
		i++
		oldNo++
	}
	for j < m {
		ops = append(ops, DiffOp{Kind: '+', Text: newLines[j], OldNo: oldNo, NewNo: newNo})
		j++
		newNo++
	}
	return ops
}

// UnifiedDiff formats a sequence of DiffOp values as a unified diff string
// with the given number of context lines. Returns "" when there are no changes.
func UnifiedDiff(ops []DiffOp, contextLines int, oldName, newName string) string {
	hasChanges := false
	for _, op := range ops {
		if op.Kind != '=' {
			hasChanges = true
			break
		}
	}
	if !hasChanges {
		return ""
	}

	var b strings.Builder
	_, _ = fmt.Fprintf(&b, "--- %s\n+++ %s\n", oldName, newName)

	i := 0
	for i < len(ops) {
		change := -1
		for j := i; j < len(ops); j++ {
			if ops[j].Kind != '=' {
				change = j
				break
			}
		}
		if change < 0 {
			break
		}

		start := change - contextLines
		if start < 0 {
			start = 0
		}
		end := change + contextLines + 1
		if end > len(ops) {
			end = len(ops)
		}

		for {
			next := -1
			for j := end; j < len(ops); j++ {
				if ops[j].Kind != '=' {
					next = j
					break
				}
			}
			if next < 0 || next > end+contextLines {
				break
			}
			end = next + contextLines + 1
			if end > len(ops) {
				end = len(ops)
			}
		}

		oldStart := ops[start].OldNo
		newStart := ops[start].NewNo
		oldCount := 0
		newCount := 0
		for k := start; k < end; k++ {
			if ops[k].Kind != '+' {
				oldCount++
			}
			if ops[k].Kind != '-' {
				newCount++
			}
		}
		_, _ = fmt.Fprintf(&b, "@@ -%d,%d +%d,%d @@\n", oldStart, oldCount, newStart, newCount)
		for k := start; k < end; k++ {
			prefix := byte(' ')
			switch ops[k].Kind {
			case '-':
				prefix = '-'
			case '+':
				prefix = '+'
			}
			b.WriteByte(prefix)
			b.WriteString(ops[k].Text)
			b.WriteByte('\n')
		}

		i = end
	}

	return strings.TrimSuffix(b.String(), "\n")
}

// FormatDiff parses two config byte slices, formats them canonically, and
// returns a unified diff. contextLines defaults to 3 if <= 0.
func FormatDiff(oldData, newData []byte, contextLines int, oldName, newName string) (string, error) {
	if contextLines <= 0 {
		contextLines = 3
	}

	oldCfg, err := Parse(oldData)
	if err != nil {
		return "", fmt.Errorf("parsing %s: %w", oldName, err)
	}
	newCfg, err := Parse(newData)
	if err != nil {
		return "", fmt.Errorf("parsing %s: %w", newName, err)
	}

	oldFormatted, err := Format(oldCfg)
	if err != nil {
		return "", fmt.Errorf("formatting %s: %w", oldName, err)
	}
	newFormatted, err := Format(newCfg)
	if err != nil {
		return "", fmt.Errorf("formatting %s: %w", newName, err)
	}

	oldLines := NormalizedDiffLines(string(oldFormatted))
	newLines := NormalizedDiffLines(string(newFormatted))
	ops := LineDiffOps(oldLines, newLines)
	return UnifiedDiff(ops, contextLines, oldName, newName), nil
}

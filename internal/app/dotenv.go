package app

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

func loadDotenv(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	lineNo := 0
	for sc.Scan() {
		lineNo++
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "export ") {
			line = strings.TrimSpace(strings.TrimPrefix(line, "export "))
		}

		key, val, ok := strings.Cut(line, "=")
		if !ok {
			return fmt.Errorf(".env line %d: missing '='", lineNo)
		}
		key = strings.TrimSpace(key)
		if key == "" {
			return fmt.Errorf(".env line %d: empty key", lineNo)
		}
		val = strings.TrimSpace(val)
		if len(val) >= 2 {
			if val[0] == '"' && val[len(val)-1] == '"' {
				u, err := strconv.Unquote(val)
				if err != nil {
					return fmt.Errorf(".env line %d: %w", lineNo, err)
				}
				val = u
			} else if val[0] == '\'' && val[len(val)-1] == '\'' {
				val = val[1 : len(val)-1]
			}
		}

		if cur, ok := os.LookupEnv(key); ok && cur != "" {
			continue
		}
		if err := os.Setenv(key, val); err != nil {
			return fmt.Errorf(".env line %d: %w", lineNo, err)
		}
	}
	if err := sc.Err(); err != nil {
		return err
	}
	return nil
}

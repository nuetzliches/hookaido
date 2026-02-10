package config

import (
	"fmt"
	"os"
	"strings"
)

func resolvePlaceholders(in string) (string, []string, []string) {
	var errs []string
	var warns []string

	var out strings.Builder
	out.Grow(len(in))

	for i := 0; i < len(in); {
		if strings.HasPrefix(in[i:], "{$") {
			end := strings.IndexByte(in[i+2:], '}')
			if end == -1 {
				errs = append(errs, "unterminated {$...} placeholder")
				out.WriteString(in[i:])
				break
			}
			body := in[i+2 : i+2+end]
			name, def, hasDef := strings.Cut(body, ":")
			if name == "" {
				errs = append(errs, "empty env var in {$...} placeholder")
			}
			val, ok := os.LookupEnv(name)
			if !ok {
				if hasDef {
					val = def
				} else {
					val = ""
					if name != "" {
						warns = append(warns, fmt.Sprintf("env var %q not set; replaced with empty string", name))
					}
				}
			}
			out.WriteString(val)
			i += 2 + end + 1
			continue
		}

		if strings.HasPrefix(in[i:], "{env.") {
			end := strings.IndexByte(in[i+5:], '}')
			if end == -1 {
				errs = append(errs, "unterminated {env.*} placeholder")
				out.WriteString(in[i:])
				break
			}
			name := in[i+5 : i+5+end]
			if name == "" {
				errs = append(errs, "empty env var in {env.*} placeholder")
			}
			val, ok := os.LookupEnv(name)
			if !ok {
				val = ""
				if name != "" {
					warns = append(warns, fmt.Sprintf("env var %q not set; replaced with empty string", name))
				}
			}
			out.WriteString(val)
			i += 5 + end + 1
			continue
		}

		if strings.HasPrefix(in[i:], "{file.") {
			end := strings.IndexByte(in[i+6:], '}')
			if end == -1 {
				errs = append(errs, "unterminated {file.*} placeholder")
				out.WriteString(in[i:])
				break
			}
			path := in[i+6 : i+6+end]
			if path == "" {
				errs = append(errs, "empty path in {file.*} placeholder")
				i += 6 + end + 1
				continue
			}
			b, err := os.ReadFile(path)
			if err != nil {
				errs = append(errs, fmt.Sprintf("file placeholder %q: %v", path, err))
				i += 6 + end + 1
				continue
			}
			out.Write(b)
			i += 6 + end + 1
			continue
		}

		out.WriteByte(in[i])
		i++
	}

	return out.String(), errs, warns
}

func resolveValue(in, field string, res *ValidationResult) string {
	val, errs, warns := resolvePlaceholders(in)
	for _, err := range errs {
		res.Errors = append(res.Errors, fmt.Sprintf("%s: %s", field, err))
	}
	for _, warn := range warns {
		res.Warnings = append(res.Warnings, fmt.Sprintf("%s: %s", field, warn))
	}
	return val
}

package config

// normalizeInput prepares raw file bytes for parsing:
// - strips UTF-8 BOM
// - normalizes CRLF/CR to LF
//
// Unlike canonicalize(), it does not trim trailing whitespace.
func normalizeInput(in []byte) []byte {
	if len(in) >= 3 && in[0] == 0xEF && in[1] == 0xBB && in[2] == 0xBF {
		in = in[3:]
	}

	out := make([]byte, 0, len(in))
	for i := 0; i < len(in); i++ {
		b := in[i]
		if b == '\r' {
			if i+1 < len(in) && in[i+1] == '\n' {
				i++
			}
			out = append(out, '\n')
			continue
		}
		out = append(out, b)
	}
	return out
}

func canonicalize(in []byte) []byte {
	// Deterministic output:
	// - normalize line endings to LF
	// - strip UTF-8 BOM
	// - ensure exactly one trailing newline
	if len(in) >= 3 && in[0] == 0xEF && in[1] == 0xBB && in[2] == 0xBF {
		in = in[3:]
	}

	out := make([]byte, 0, len(in))
	for i := 0; i < len(in); i++ {
		b := in[i]
		if b == '\r' {
			if i+1 < len(in) && in[i+1] == '\n' {
				i++
			}
			out = append(out, '\n')
			continue
		}
		out = append(out, b)
	}

	// Trim trailing whitespace/newlines and add a single newline.
	for len(out) > 0 {
		last := out[len(out)-1]
		if last == '\n' || last == ' ' || last == '\t' {
			out = out[:len(out)-1]
			continue
		}
		break
	}
	out = append(out, '\n')
	return out
}

package httpheader

import (
	"fmt"
	"strings"
)

// ValidateMap validates user-supplied HTTP header key/value pairs before
// enqueue so invalid headers fail fast instead of failing later at delivery.
func ValidateMap(headers map[string]string) error {
	for rawName, rawValue := range headers {
		name := strings.TrimSpace(rawName)
		if name == "" {
			return fmt.Errorf("header name must not be empty")
		}
		if rawName != name {
			return fmt.Errorf("header %q has leading or trailing whitespace", rawName)
		}
		if !validHeaderFieldName(name) {
			return fmt.Errorf("header %q has invalid field name", name)
		}
		if !validHeaderFieldValue(rawValue) {
			return fmt.Errorf("header %q has invalid field value", name)
		}
	}
	return nil
}

func validHeaderFieldName(name string) bool {
	if name == "" {
		return false
	}
	for i := 0; i < len(name); i++ {
		if !isTokenByte(name[i]) {
			return false
		}
	}
	return true
}

func isTokenByte(b byte) bool {
	if b >= '0' && b <= '9' {
		return true
	}
	if b >= 'A' && b <= 'Z' {
		return true
	}
	if b >= 'a' && b <= 'z' {
		return true
	}
	switch b {
	case '!', '#', '$', '%', '&', '\'', '*', '+', '-', '.', '^', '_', '`', '|', '~':
		return true
	default:
		return false
	}
}

func validHeaderFieldValue(value string) bool {
	for i := 0; i < len(value); i++ {
		b := value[i]
		if b == '\r' || b == '\n' || b == 0x7f {
			return false
		}
		if b < 0x20 && b != '\t' {
			return false
		}
	}
	return true
}

package admin

import (
	"errors"
	"strings"
)

const (
	ManagementMutationCodeRouteAlreadyMapped   = "management_route_already_mapped"
	ManagementMutationCodeRoutePublishDisabled = "management_route_publish_disabled"
	ManagementMutationCodeRouteTargetMismatch  = "management_route_target_mismatch"
	ManagementMutationCodeRouteBacklogActive   = "management_route_backlog_active"
)

// ManagementMutationError carries explicit conflict classification metadata while
// preserving sentinel compatibility via error wrapping.
type ManagementMutationError struct {
	base   error
	code   string
	detail string
}

func (e *ManagementMutationError) Error() string {
	if e == nil {
		return ""
	}
	base := strings.TrimSpace(e.base.Error())
	detail := strings.TrimSpace(e.detail)
	if base == "" {
		return detail
	}
	if detail == "" {
		return base
	}
	return base + ": " + detail
}

func (e *ManagementMutationError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.base
}

func NewManagementMutationError(base error, code, detail string) error {
	if base == nil {
		base = errors.New("management mutation failed")
	}
	return &ManagementMutationError{
		base:   base,
		code:   strings.TrimSpace(code),
		detail: strings.TrimSpace(detail),
	}
}

func ExtractManagementMutationError(err error) (code, detail string, ok bool) {
	var typed *ManagementMutationError
	if !errors.As(err, &typed) || typed == nil {
		return "", "", false
	}
	return strings.TrimSpace(typed.code), strings.TrimSpace(typed.detail), true
}

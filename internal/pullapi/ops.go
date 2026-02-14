package pullapi

import (
	"errors"
	"strings"
	"time"

	"github.com/nuetzliches/hookaido/internal/queue"
)

// OpError is a transport-neutral operation error for pull worker flows.
type OpError struct {
	StatusCode int
	Code       string
	Detail     string
}

func (e *OpError) Error() string {
	if e == nil {
		return ""
	}
	return e.Detail
}

// DequeueParams captures normalized dequeue input for route-bound operations.
type DequeueParams struct {
	Batch       int
	MaxWait     time.Duration
	HasMaxWait  bool
	LeaseTTL    time.Duration
	HasLeaseTTL bool
}

// DequeueResult is the route-bound dequeue output.
type DequeueResult struct {
	Items []queue.Envelope
}

// LeaseBatchResult captures aggregate lease mutation output.
type LeaseBatchResult struct {
	Succeeded int
	Conflicts []queue.LeaseBatchConflict
}

func (s *Server) Dequeue(route string, params DequeueParams) (DequeueResult, *OpError) {
	batch := params.Batch
	if batch <= 0 {
		batch = 1
	}
	if s.MaxBatch > 0 && batch > s.MaxBatch {
		batch = s.MaxBatch
	}

	maxWait := time.Duration(0)
	if params.HasMaxWait {
		maxWait = params.MaxWait
	} else if s.DefaultMaxWait > 0 {
		maxWait = s.DefaultMaxWait
	}
	if s.MaxWait > 0 && maxWait > s.MaxWait {
		maxWait = s.MaxWait
	}

	leaseTTL := s.DefaultLeaseTTL
	if params.HasLeaseTTL {
		leaseTTL = params.LeaseTTL
	}
	if s.MaxLeaseTTL > 0 && leaseTTL > s.MaxLeaseTTL {
		leaseTTL = s.MaxLeaseTTL
	}

	resp, err := s.Store.Dequeue(queue.DequeueRequest{
		Route:    route,
		Target:   s.Target,
		Batch:    batch,
		MaxWait:  maxWait,
		LeaseTTL: leaseTTL,
	})
	if err != nil {
		s.observeDequeue(route, 503, nil)
		return DequeueResult{}, &OpError{
			StatusCode: 503,
			Code:       pullErrStoreUnavailable,
			Detail:     "dequeue is temporarily unavailable",
		}
	}

	s.observeDequeue(route, 200, resp.Items)
	return DequeueResult{Items: resp.Items}, nil
}

func (s *Server) AckSingle(route string, leaseID string) *OpError {
	leaseID = strings.TrimSpace(leaseID)
	if leaseID == "" {
		s.observeAck(route, 400, "", false)
		return &OpError{
			StatusCode: 400,
			Code:       pullErrInvalidBody,
			Detail:     "lease_id is required",
		}
	}

	if s.isRecentlyCompletedLease(leaseID, recentLeaseOpAck) {
		s.observeAck(route, 204, leaseID, false)
		return nil
	}

	if err := s.Store.Ack(leaseID); err != nil {
		if errors.Is(err, queue.ErrLeaseNotFound) || errors.Is(err, queue.ErrLeaseExpired) {
			s.observeAck(route, 409, leaseID, errors.Is(err, queue.ErrLeaseExpired))
			return &OpError{
				StatusCode: 409,
				Code:       pullErrLeaseConflict,
				Detail:     "lease is invalid or expired",
			}
		}
		s.observeAck(route, 500, leaseID, false)
		return &OpError{
			StatusCode: 500,
			Code:       pullErrInternal,
			Detail:     "ack failed",
		}
	}

	s.observeAck(route, 204, leaseID, false)
	s.rememberCompletedLease(leaseID, recentLeaseOpAck)
	return nil
}

func (s *Server) AckBatch(route string, leaseIDs []string) (LeaseBatchResult, *OpError) {
	pendingLeaseIDs, completedLeaseIDs := s.partitionRecentlyCompletedLeases(leaseIDs, recentLeaseOpAck)
	for _, leaseID := range completedLeaseIDs {
		s.observeAck(route, 204, leaseID, false)
	}

	ackedFromCompleted := len(completedLeaseIDs)
	if len(pendingLeaseIDs) == 0 {
		return LeaseBatchResult{Succeeded: ackedFromCompleted}, nil
	}

	if batchStore, ok := s.Store.(queue.LeaseBatchStore); ok {
		res, err := batchStore.AckBatch(pendingLeaseIDs)
		if err != nil {
			s.observeAck(route, 500, "", false)
			return LeaseBatchResult{}, &OpError{
				StatusCode: 500,
				Code:       pullErrInternal,
				Detail:     "ack failed",
			}
		}

		observeBatchAck(s, route, pendingLeaseIDs, res)
		for _, leaseID := range s.successfulLeaseIDs(pendingLeaseIDs, res.Conflicts) {
			s.rememberCompletedLease(leaseID, recentLeaseOpAck)
		}
		return LeaseBatchResult{
			Succeeded: ackedFromCompleted + res.Succeeded,
			Conflicts: append([]queue.LeaseBatchConflict(nil), res.Conflicts...),
		}, nil
	}

	acked := 0
	conflicts := make([]queue.LeaseBatchConflict, 0)

	for _, leaseID := range pendingLeaseIDs {
		err := s.Store.Ack(leaseID)
		if err == nil {
			acked++
			s.observeAck(route, 204, leaseID, false)
			s.rememberCompletedLease(leaseID, recentLeaseOpAck)
			continue
		}
		if errors.Is(err, queue.ErrLeaseExpired) {
			conflicts = append(conflicts, queue.LeaseBatchConflict{
				LeaseID: leaseID,
				Expired: true,
			})
			s.observeAck(route, 409, leaseID, true)
			continue
		}
		if errors.Is(err, queue.ErrLeaseNotFound) {
			conflicts = append(conflicts, queue.LeaseBatchConflict{
				LeaseID: leaseID,
				Expired: false,
			})
			s.observeAck(route, 409, leaseID, false)
			continue
		}

		s.observeAck(route, 500, leaseID, false)
		return LeaseBatchResult{}, &OpError{
			StatusCode: 500,
			Code:       pullErrInternal,
			Detail:     "ack failed",
		}
	}

	return LeaseBatchResult{
		Succeeded: ackedFromCompleted + acked,
		Conflicts: conflicts,
	}, nil
}

func (s *Server) NackSingle(route string, leaseID string, dead bool, reason string, delay time.Duration) *OpError {
	leaseID = strings.TrimSpace(leaseID)
	if leaseID == "" {
		s.observeNack(route, 400, "", false)
		return &OpError{
			StatusCode: 400,
			Code:       pullErrInvalidBody,
			Detail:     "lease_id is required",
		}
	}

	if s.isRecentlyCompletedLease(leaseID, recentLeaseOpNack) {
		s.observeNack(route, 204, leaseID, false)
		return nil
	}

	if dead {
		if err := s.Store.MarkDead(leaseID, reason); err != nil {
			if errors.Is(err, queue.ErrLeaseNotFound) || errors.Is(err, queue.ErrLeaseExpired) {
				s.observeNack(route, 409, leaseID, errors.Is(err, queue.ErrLeaseExpired))
				return &OpError{
					StatusCode: 409,
					Code:       pullErrLeaseConflict,
					Detail:     "lease is invalid or expired",
				}
			}
			s.observeNack(route, 500, leaseID, false)
			return &OpError{
				StatusCode: 500,
				Code:       pullErrInternal,
				Detail:     "mark dead failed",
			}
		}
		s.observeNack(route, 204, leaseID, false)
		s.rememberCompletedLease(leaseID, recentLeaseOpNack)
		return nil
	}

	if err := s.Store.Nack(leaseID, delay); err != nil {
		if errors.Is(err, queue.ErrLeaseNotFound) || errors.Is(err, queue.ErrLeaseExpired) {
			s.observeNack(route, 409, leaseID, errors.Is(err, queue.ErrLeaseExpired))
			return &OpError{
				StatusCode: 409,
				Code:       pullErrLeaseConflict,
				Detail:     "lease is invalid or expired",
			}
		}
		s.observeNack(route, 500, leaseID, false)
		return &OpError{
			StatusCode: 500,
			Code:       pullErrInternal,
			Detail:     "nack failed",
		}
	}

	s.observeNack(route, 204, leaseID, false)
	s.rememberCompletedLease(leaseID, recentLeaseOpNack)
	return nil
}

func (s *Server) NackBatch(route string, leaseIDs []string, dead bool, reason string, delay time.Duration) (LeaseBatchResult, *OpError) {
	pendingLeaseIDs, completedLeaseIDs := s.partitionRecentlyCompletedLeases(leaseIDs, recentLeaseOpNack)
	for _, leaseID := range completedLeaseIDs {
		s.observeNack(route, 204, leaseID, false)
	}

	succeededFromCompleted := len(completedLeaseIDs)
	if len(pendingLeaseIDs) == 0 {
		return LeaseBatchResult{Succeeded: succeededFromCompleted}, nil
	}

	if batchStore, ok := s.Store.(queue.LeaseBatchStore); ok {
		var (
			res queue.LeaseBatchResult
			err error
		)
		if dead {
			res, err = batchStore.MarkDeadBatch(pendingLeaseIDs, reason)
		} else {
			res, err = batchStore.NackBatch(pendingLeaseIDs, delay)
		}
		if err != nil {
			s.observeNack(route, 500, "", false)
			detail := "nack failed"
			if dead {
				detail = "mark dead failed"
			}
			return LeaseBatchResult{}, &OpError{
				StatusCode: 500,
				Code:       pullErrInternal,
				Detail:     detail,
			}
		}

		observeBatchNack(s, route, pendingLeaseIDs, res)
		for _, leaseID := range s.successfulLeaseIDs(pendingLeaseIDs, res.Conflicts) {
			s.rememberCompletedLease(leaseID, recentLeaseOpNack)
		}
		return LeaseBatchResult{
			Succeeded: succeededFromCompleted + res.Succeeded,
			Conflicts: append([]queue.LeaseBatchConflict(nil), res.Conflicts...),
		}, nil
	}

	succeeded := 0
	conflicts := make([]queue.LeaseBatchConflict, 0)

	for _, leaseID := range pendingLeaseIDs {
		var err error
		if dead {
			err = s.Store.MarkDead(leaseID, reason)
		} else {
			err = s.Store.Nack(leaseID, delay)
		}
		if err == nil {
			succeeded++
			s.observeNack(route, 204, leaseID, false)
			s.rememberCompletedLease(leaseID, recentLeaseOpNack)
			continue
		}
		if errors.Is(err, queue.ErrLeaseExpired) {
			conflicts = append(conflicts, queue.LeaseBatchConflict{
				LeaseID: leaseID,
				Expired: true,
			})
			s.observeNack(route, 409, leaseID, true)
			continue
		}
		if errors.Is(err, queue.ErrLeaseNotFound) {
			conflicts = append(conflicts, queue.LeaseBatchConflict{
				LeaseID: leaseID,
				Expired: false,
			})
			s.observeNack(route, 409, leaseID, false)
			continue
		}

		s.observeNack(route, 500, leaseID, false)
		detail := "nack failed"
		if dead {
			detail = "mark dead failed"
		}
		return LeaseBatchResult{}, &OpError{
			StatusCode: 500,
			Code:       pullErrInternal,
			Detail:     detail,
		}
	}

	return LeaseBatchResult{
		Succeeded: succeededFromCompleted + succeeded,
		Conflicts: conflicts,
	}, nil
}

func (s *Server) Extend(route string, leaseID string, extendBy time.Duration) *OpError {
	leaseID = strings.TrimSpace(leaseID)
	if leaseID == "" {
		s.observeExtend(route, 400, "", 0, false)
		return &OpError{
			StatusCode: 400,
			Code:       pullErrInvalidBody,
			Detail:     "lease_id is required",
		}
	}
	if err := s.Store.Extend(leaseID, extendBy); err != nil {
		if errors.Is(err, queue.ErrLeaseNotFound) || errors.Is(err, queue.ErrLeaseExpired) {
			s.observeExtend(route, 409, leaseID, extendBy, errors.Is(err, queue.ErrLeaseExpired))
			return &OpError{
				StatusCode: 409,
				Code:       pullErrLeaseConflict,
				Detail:     "lease is invalid or expired",
			}
		}
		s.observeExtend(route, 500, leaseID, extendBy, false)
		return &OpError{
			StatusCode: 500,
			Code:       pullErrInternal,
			Detail:     "extend failed",
		}
	}

	s.observeExtend(route, 204, leaseID, extendBy, false)
	return nil
}

package workerapi

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/nuetzliches/hookaido/internal/pullapi"
	"github.com/nuetzliches/hookaido/internal/queue"
	workerapipb "github.com/nuetzliches/hookaido/internal/workerapi/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Server exposes the optional worker gRPC transport while reusing pull semantics.
type Server struct {
	workerapipb.UnimplementedWorkerServiceServer

	Pull          *pullapi.Server
	ResolveRoute  func(endpoint string) (route string, ok bool)
	Authorize     Authorizer
	MaxLeaseBatch int
}

func NewServer(pullServer *pullapi.Server) *Server {
	return &Server{
		Pull: pullServer,
	}
}

func (s *Server) Dequeue(ctx context.Context, req *workerapipb.DequeueRequest) (*workerapipb.DequeueResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	endpoint := strings.TrimSpace(req.GetEndpoint())
	if endpoint == "" {
		return nil, status.Error(codes.InvalidArgument, "endpoint is required")
	}

	maxWait, hasMaxWait, err := durationFromProto(req.GetMaxWait(), "max_wait")
	if err != nil {
		return nil, err
	}
	leaseTTL, hasLeaseTTL, err := durationFromProto(req.GetLeaseTtl(), "lease_ttl")
	if err != nil {
		return nil, err
	}

	route, err := s.resolveAndAuthorize(ctx, endpoint)
	if err != nil {
		return nil, err
	}
	if s.Pull == nil {
		return nil, status.Error(codes.Internal, "pull server is not configured")
	}

	outcome, opErr := s.Pull.Dequeue(route, pullapi.DequeueParams{
		Batch:       int(req.GetBatch()),
		MaxWait:     maxWait,
		HasMaxWait:  hasMaxWait,
		LeaseTTL:    leaseTTL,
		HasLeaseTTL: hasLeaseTTL,
	})
	if opErr != nil {
		return nil, mapOpError(opErr)
	}

	items := make([]*workerapipb.DequeueItem, 0, len(outcome.Items))
	for _, it := range outcome.Items {
		items = append(items, &workerapipb.DequeueItem{
			Id:         it.ID,
			LeaseId:    it.LeaseID,
			ReceivedAt: timestamppb.New(it.ReceivedAt),
			Attempt:    int32(it.Attempt),
			NextRunAt:  timestamppb.New(it.NextRunAt),
			Route:      it.Route,
			Payload:    append([]byte(nil), it.Payload...),
			Headers:    cloneStringMap(it.Headers),
			Trace:      cloneStringMap(it.Trace),
		})
	}
	return &workerapipb.DequeueResponse{Items: items}, nil
}

func (s *Server) Ack(ctx context.Context, req *workerapipb.AckRequest) (*workerapipb.AckResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	endpoint := strings.TrimSpace(req.GetEndpoint())
	if endpoint == "" {
		return nil, status.Error(codes.InvalidArgument, "endpoint is required")
	}

	route, err := s.resolveAndAuthorize(ctx, endpoint)
	if err != nil {
		return nil, err
	}
	if s.Pull == nil {
		return nil, status.Error(codes.Internal, "pull server is not configured")
	}

	leaseIDs, isBatch, err := normalizeLeaseIDs(req.GetLeaseId(), req.GetLeaseIds(), s.leaseBatchLimit())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if !isBatch {
		if opErr := s.Pull.AckSingle(route, leaseIDs[0]); opErr != nil {
			return nil, mapOpError(opErr)
		}
		return &workerapipb.AckResponse{Acked: 1}, nil
	}

	outcome, opErr := s.Pull.AckBatch(route, leaseIDs)
	if opErr != nil {
		return nil, mapOpError(opErr)
	}
	return &workerapipb.AckResponse{
		Acked:     uint32(outcome.Succeeded),
		Conflicts: mapConflicts(outcome.Conflicts),
	}, nil
}

func (s *Server) Nack(ctx context.Context, req *workerapipb.NackRequest) (*workerapipb.NackResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	endpoint := strings.TrimSpace(req.GetEndpoint())
	if endpoint == "" {
		return nil, status.Error(codes.InvalidArgument, "endpoint is required")
	}

	route, err := s.resolveAndAuthorize(ctx, endpoint)
	if err != nil {
		return nil, err
	}
	if s.Pull == nil {
		return nil, status.Error(codes.Internal, "pull server is not configured")
	}

	delay, _, err := durationFromProto(req.GetDelay(), "delay")
	if err != nil {
		return nil, err
	}
	leaseIDs, isBatch, err := normalizeLeaseIDs(req.GetLeaseId(), req.GetLeaseIds(), s.leaseBatchLimit())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if !isBatch {
		if opErr := s.Pull.NackSingle(route, leaseIDs[0], req.GetDead(), req.GetReason(), delay); opErr != nil {
			return nil, mapOpError(opErr)
		}
		return &workerapipb.NackResponse{Succeeded: 1}, nil
	}

	outcome, opErr := s.Pull.NackBatch(route, leaseIDs, req.GetDead(), req.GetReason(), delay)
	if opErr != nil {
		return nil, mapOpError(opErr)
	}
	return &workerapipb.NackResponse{
		Succeeded: uint32(outcome.Succeeded),
		Conflicts: mapConflicts(outcome.Conflicts),
	}, nil
}

func (s *Server) Extend(ctx context.Context, req *workerapipb.ExtendRequest) (*emptypb.Empty, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	endpoint := strings.TrimSpace(req.GetEndpoint())
	if endpoint == "" {
		return nil, status.Error(codes.InvalidArgument, "endpoint is required")
	}
	leaseID := strings.TrimSpace(req.GetLeaseId())
	if leaseID == "" || req.GetExtendBy() == nil {
		return nil, status.Error(codes.InvalidArgument, "lease_id and extend_by are required")
	}

	extendBy, _, err := durationFromProto(req.GetExtendBy(), "extend_by")
	if err != nil {
		return nil, err
	}
	route, err := s.resolveAndAuthorize(ctx, endpoint)
	if err != nil {
		return nil, err
	}
	if s.Pull == nil {
		return nil, status.Error(codes.Internal, "pull server is not configured")
	}
	if opErr := s.Pull.Extend(route, leaseID, extendBy); opErr != nil {
		return nil, mapOpError(opErr)
	}
	return &emptypb.Empty{}, nil
}

func (s *Server) resolveAndAuthorize(ctx context.Context, endpoint string) (string, error) {
	if s.Authorize != nil && !s.Authorize(ctx, endpoint) {
		return "", status.Error(codes.Unauthenticated, "request is not authorized")
	}
	route, ok := s.resolveRoute(endpoint)
	if !ok {
		return "", status.Error(codes.NotFound, "pull endpoint is not configured")
	}
	return route, nil
}

func (s *Server) resolveRoute(endpoint string) (string, bool) {
	if s.ResolveRoute != nil {
		return s.ResolveRoute(endpoint)
	}
	if s.Pull != nil && s.Pull.ResolveRoute != nil {
		return s.Pull.ResolveRoute(endpoint)
	}
	if endpoint == "" {
		return "", false
	}
	return endpoint, true
}

func (s *Server) leaseBatchLimit() int {
	if s.MaxLeaseBatch > 0 {
		return s.MaxLeaseBatch
	}
	if s.Pull != nil && s.Pull.MaxLeaseBatch > 0 {
		return s.Pull.MaxLeaseBatch
	}
	return 100
}

func mapOpError(opErr *pullapi.OpError) error {
	if opErr == nil {
		return nil
	}
	switch opErr.StatusCode {
	case 400:
		return status.Error(codes.InvalidArgument, opErr.Detail)
	case 401:
		return status.Error(codes.Unauthenticated, opErr.Detail)
	case 404:
		return status.Error(codes.NotFound, opErr.Detail)
	case 409:
		return status.Error(codes.FailedPrecondition, opErr.Detail)
	case 503:
		return status.Error(codes.Unavailable, opErr.Detail)
	default:
		return status.Error(codes.Internal, opErr.Detail)
	}
}

func durationFromProto(d *durationpb.Duration, field string) (time.Duration, bool, error) {
	if d == nil {
		return 0, false, nil
	}
	if err := d.CheckValid(); err != nil {
		return 0, false, status.Error(codes.InvalidArgument, field+" must be a valid duration")
	}
	return d.AsDuration(), true, nil
}

func normalizeLeaseIDs(leaseID string, leaseIDs []string, maxBatch int) ([]string, bool, error) {
	single := strings.TrimSpace(leaseID)
	if single != "" && len(leaseIDs) > 0 {
		return nil, false, errors.New("use either lease_id or lease_ids, not both")
	}
	if single != "" {
		return []string{single}, false, nil
	}

	if len(leaseIDs) == 0 {
		return nil, false, errors.New("lease_id or lease_ids is required")
	}

	seen := make(map[string]struct{}, len(leaseIDs))
	out := make([]string, 0, len(leaseIDs))
	for _, raw := range leaseIDs {
		id := strings.TrimSpace(raw)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	if len(out) == 0 {
		return nil, false, errors.New("lease_ids must include at least one non-empty lease id")
	}
	if maxBatch > 0 && len(out) > maxBatch {
		return nil, false, errors.New("lease_ids exceeds max batch")
	}
	return out, true, nil
}

func mapConflicts(conflicts []queue.LeaseBatchConflict) []*workerapipb.LeaseConflict {
	out := make([]*workerapipb.LeaseConflict, 0, len(conflicts))
	for _, conflict := range conflicts {
		out = append(out, &workerapipb.LeaseConflict{
			LeaseId: conflict.LeaseID,
			Expired: conflict.Expired,
		})
	}
	return out
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

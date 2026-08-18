package pullapi

import "github.com/nuetzliches/hookaido/v2/internal/queue"

// Lease operations take only a lease ID. The route resolved from the request
// path was used for metrics and nothing else, so a client authorized for one
// endpoint could ack, nack, dead-letter or extend another route's in-flight
// message if it learned that lease ID -- from a shared dashboard, a log line, a
// support ticket. Exploiting it needs the random `lease_…` value, so the
// severity is low, but the route-scoped credential model the config offers was
// not enforced where it matters.
//
// The check costs one lookup per lease operation, so it runs only when the
// deployment actually uses route-scoped pull credentials: with a single global
// token every client is authorized for every route anyway, and there is nothing
// to enforce.

// leaseScopeEnforced reports whether lease operations must be checked against
// the route they were issued for.
func (s *Server) leaseScopeEnforced(route string) bool {
	if route == "" || s.LeaseRouteScoped == nil || !s.LeaseRouteScoped() {
		return false
	}
	_, ok := s.Store.(queue.LeaseRouteResolver)
	return ok
}

// leasesInRoute splits leaseIDs into those that belong to route and those that
// do not. Unknown leases count as in-route: they are the store's to reject, and
// reporting them here would tell a caller which lease IDs exist elsewhere.
//
// The error return is deliberate: a resolver failure denies the operation
// rather than waving it through, so a transient store problem cannot turn into
// an authorization bypass.
func (s *Server) leasesInRoute(route string, leaseIDs []string) (inRoute []string, outOfRoute []string, err error) {
	if !s.leaseScopeEnforced(route) || len(leaseIDs) == 0 {
		return leaseIDs, nil, nil
	}

	resolver, ok := s.Store.(queue.LeaseRouteResolver)
	if !ok {
		return leaseIDs, nil, nil
	}
	routes, err := resolver.LeaseRoutes(leaseIDs)
	if err != nil {
		return nil, nil, err
	}

	inRoute = make([]string, 0, len(leaseIDs))
	for _, leaseID := range leaseIDs {
		if owner, known := routes[leaseID]; known && owner != route {
			outOfRoute = append(outOfRoute, leaseID)
			continue
		}
		inRoute = append(inRoute, leaseID)
	}
	return inRoute, outOfRoute, nil
}

// leaseInRoute is leasesInRoute for a single lease.
func (s *Server) leaseInRoute(route string, leaseID string) (bool, error) {
	_, outOfRoute, err := s.leasesInRoute(route, []string{leaseID})
	if err != nil {
		return false, err
	}
	return len(outOfRoute) == 0, nil
}

// leaseScopeConflict is the response for a lease that belongs to another route.
// It is deliberately identical to the unknown-lease response, so the caller
// cannot use it to probe for lease IDs.
func leaseScopeConflict() *OpError {
	return &OpError{
		StatusCode: 409,
		Code:       pullErrLeaseConflict,
		Detail:     "lease is invalid or expired",
	}
}

func leaseScopeUnavailable(detail string) *OpError {
	return &OpError{
		StatusCode: 500,
		Code:       pullErrInternal,
		Detail:     detail,
	}
}

// observeForeignLeases records the metrics for leases rejected by the scope
// check and returns them as batch conflicts.
func (s *Server) observeForeignLeases(route string, leaseIDs []string, observe func(route string, statusCode int, leaseID string, leaseExpired bool)) []queue.LeaseBatchConflict {
	conflicts := make([]queue.LeaseBatchConflict, 0, len(leaseIDs))
	for _, leaseID := range leaseIDs {
		observe(route, 409, leaseID, false)
		conflicts = append(conflicts, queue.LeaseBatchConflict{LeaseID: leaseID})
	}
	return conflicts
}

package queue

import "strings"

// IDMutationTouchesManagedRoute reports whether the ID-scoped mutation request
// targets at least one managed route item in one of the allowed states.
func IDMutationTouchesManagedRoute(store Store, ids []string, allowedStates map[State]struct{}, managedRoutes map[string]struct{}) (bool, error) {
	if store == nil || len(ids) == 0 || len(managedRoutes) == 0 {
		return false, nil
	}

	lookup, err := store.LookupMessages(MessageLookupRequest{IDs: ids})
	if err != nil {
		return false, err
	}
	for _, item := range lookup.Items {
		if len(allowedStates) > 0 {
			if _, ok := allowedStates[item.State]; !ok {
				continue
			}
		}
		if _, ok := managedRoutes[strings.TrimSpace(item.Route)]; ok {
			return true, nil
		}
	}
	return false, nil
}

package pullapi

// Queue is the queue one pull endpoint reads from.
//
// A pull endpoint used to be identified by its route alone, because a route had
// exactly one queue and every envelope in it carried the literal target `pull`.
// Consumer groups break that: a route with groups fans out to one independent
// queue per group, so the route no longer says which messages an endpoint is
// entitled to — the target does.
//
// ConsumerGroup is empty for an ungrouped route and carries the group name
// otherwise. It exists next to Target rather than being parsed back out of it
// so that metrics and the consumer registry can label by group without
// re-deriving it from a string prefix.
type Queue struct {
	Route         string
	Target        string
	ConsumerGroup string
}

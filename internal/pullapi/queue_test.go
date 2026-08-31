package pullapi

// testQueue is the ungrouped queue of a route: what every pull endpoint
// resolved to before consumer groups existed.
func testQueue(route string) Queue {
	return Queue{Route: route, Target: "pull"}
}

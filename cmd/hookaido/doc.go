// Command hookaido runs the Hookaido webhook ingress queue.
//
// Hookaido receives webhooks at the edge, enqueues them durably, and exposes
// pull and admin APIs for internal processing and operations.
//
// Install:
//
//	go install github.com/nuetzliches/hookaido/cmd/hookaido@latest
//
// Usage:
//
//	hookaido run --config ./Hookaidofile --db ./.data/hookaido.db
package main

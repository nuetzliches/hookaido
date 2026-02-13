GOEXE := $(shell go env GOEXE)
BINARY := hookaido$(GOEXE)
BINDIR := bin

.PHONY: build test fmt lint check release-check dist dist-signed dist-verify

build:
	@mkdir -p "$(BINDIR)"
	go build -o "$(BINDIR)/$(BINARY)" ./cmd/hookaido

test:
	go test ./...

fmt:
	go fmt ./...

lint:
	go vet ./...

check: lint test

release-check: check build

dist:
	go run ./internal/tools/release -out dist

dist-signed:
	@test -n "$(HOOKAIDO_SIGNING_KEY_FILE)" || (echo "HOOKAIDO_SIGNING_KEY_FILE is required" && exit 1)
	go run ./internal/tools/release -out dist -signing-key "$(HOOKAIDO_SIGNING_KEY_FILE)"

dist-verify:
	@test -n "$(CHECKSUMS_FILE)" || (echo "CHECKSUMS_FILE is required" && exit 1)
	go run ./cmd/hookaido verify-release --checksums "$(CHECKSUMS_FILE)" --require-signature --require-sbom

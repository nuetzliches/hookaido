GOEXE := $(shell go env GOEXE)
BINARY := hookaido$(GOEXE)
BINDIR := bin
BENCH_DIR := .bench
PULL_BENCH_FLAGS := -run '^$$' -bench '^BenchmarkPull' -benchmem -benchtime=3s -count=5 -cpu 1
PULL_BENCH_CURRENT := $(BENCH_DIR)/pull.txt
PULL_BENCH_BASELINE := $(BENCH_DIR)/pull-baseline.txt
PULL_BENCH_COMPARE := $(BENCH_DIR)/pull-compare.txt
PULL_EXTEND_BENCH_FLAGS := -run '^$$' -bench '^BenchmarkPullDequeueExtendSingle$$' -benchmem -benchtime=5s -count=10 -cpu 1
PULL_EXTEND_CURRENT := $(BENCH_DIR)/pull-extend.txt
PULL_EXTEND_COMPARE := $(BENCH_DIR)/pull-extend-compare.txt
PULL_DRAIN_BENCH_FLAGS := -run '^$$' -bench '^BenchmarkPullDequeueAckBatch15$$' -benchmem -benchtime=5s -count=10 -cpu 1
PULL_DRAIN_CURRENT := $(BENCH_DIR)/pull-drain.txt
PULL_DRAIN_BASELINE := $(BENCH_DIR)/pull-drain-baseline.txt
PULL_DRAIN_COMPARE := $(BENCH_DIR)/pull-drain-compare.txt
PULL_CONTENTION_BENCH_FLAGS := -run '^$$' -bench '^BenchmarkPull(Ack|Nack)RetryParallel$$' -benchmem -benchtime=5s -count=10 -cpu 1,4
PULL_CONTENTION_CURRENT := $(BENCH_DIR)/pull-contention.txt
PULL_CONTENTION_BASELINE := $(BENCH_DIR)/pull-contention-baseline.txt
PULL_CONTENTION_COMPARE := $(BENCH_DIR)/pull-contention-compare.txt
PULL_MIXED_BENCH_FLAGS := -run '^$$' -bench '^BenchmarkMixedIngressDrain$$' -benchmem -benchtime=8s -count=5 -cpu 4
PULL_MIXED_CURRENT := $(BENCH_DIR)/pull-mixed.txt
PULL_MIXED_BASELINE := $(BENCH_DIR)/pull-mixed-baseline.txt
PULL_MIXED_COMPARE := $(BENCH_DIR)/pull-mixed-compare.txt
PUSH_MIXED_BENCH_FLAGS := -run '^$$' -bench '^BenchmarkPushIngressDrainSaturation$$' -benchmem -benchtime=8s -count=5 -cpu 4
PUSH_MIXED_CURRENT := $(BENCH_DIR)/push-mixed.txt
PUSH_MIXED_BASELINE := $(BENCH_DIR)/push-mixed-baseline.txt
PUSH_MIXED_COMPARE := $(BENCH_DIR)/push-mixed-compare.txt
PUSH_SKEWED_BENCH_FLAGS := -run '^$$' -bench '^BenchmarkPushIngressDrainSkewedTargets$$' -benchmem -benchtime=8s -count=5 -cpu 4
PUSH_SKEWED_CURRENT := $(BENCH_DIR)/push-skewed.txt
PUSH_SKEWED_BASELINE := $(BENCH_DIR)/push-skewed-baseline.txt
PUSH_SKEWED_COMPARE := $(BENCH_DIR)/push-skewed-compare.txt
BENCHSTAT_CMD := golang.org/x/perf/cmd/benchstat@v0.0.0-20260211190930-8161c38c6cdc
ADAPTIVE_AB_SCRIPT := scripts/adaptive-ab.sh
ADAPTIVE_GUARDRAIL_SCRIPT := scripts/adaptive-guardrail.sh
ADAPTIVE_AB_ROOT := .artifacts/adaptive-ab
ADAPTIVE_AB_BACKEND ?= sqlite
ADAPTIVE_AB_DURATION ?= 120
ADAPTIVE_AB_INGRESS_WORKERS ?= 16
ADAPTIVE_AB_MIXED_DRAIN_WORKERS ?= 8
ADAPTIVE_AB_DEQUEUE_BATCH ?= 15
ADAPTIVE_AB_QUEUE_MAX_DEPTH ?= 50000
ADAPTIVE_AB_MIXED_SAT_DURATION ?= 30
ADAPTIVE_AB_MIXED_SAT_INGRESS_WORKERS ?= 256
ADAPTIVE_AB_MIXED_SAT_DRAIN_WORKERS ?= 8
ADAPTIVE_AB_MIXED_SAT_DEQUEUE_BATCH ?= 5
ADAPTIVE_AB_MIXED_SAT_QUEUE_MAX_DEPTH ?= 2000
ADAPTIVE_ACK_CONFLICT_MAX_RATIO ?= 5.0
ADAPTIVE_ACK_CONFLICT_MIN_ACKED ?= 100
ADAPTIVE_ROUTE_ACK_CONFLICT_MAX_RATIO ?= 5.0
ADAPTIVE_ROUTE_ACK_CONFLICT_MIN_ACKED ?= 50

.PHONY: build test fmt lint check proto-worker bench-pull bench-pull-baseline bench-pull-compare bench-pull-extend bench-pull-extend-compare bench-pull-drain bench-pull-drain-baseline bench-pull-drain-compare bench-pull-contention bench-pull-contention-baseline bench-pull-contention-compare bench-pull-mixed bench-pull-mixed-baseline bench-pull-mixed-compare bench-push-mixed bench-push-mixed-baseline bench-push-mixed-compare bench-push-skewed bench-push-skewed-baseline bench-push-skewed-compare adaptive-ab adaptive-ab-all adaptive-ab-pull adaptive-ab-mixed adaptive-ab-mixed-saturation adaptive-ab-guardrail-check adaptive-ab-mixed-guardrail release-check dist dist-signed dist-verify

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

proto-worker:
	@GO_BIN_DIR="$$(go env GOPATH)/bin"; \
	BUF_BIN="$$(command -v buf 2>/dev/null || true)"; \
	[ -n "$$BUF_BIN" ] || BUF_BIN="$$GO_BIN_DIR/buf"; \
	PROTOC_GO_BIN="$$(command -v protoc-gen-go 2>/dev/null || true)"; \
	[ -n "$$PROTOC_GO_BIN" ] || PROTOC_GO_BIN="$$GO_BIN_DIR/protoc-gen-go"; \
	PROTOC_GRPC_BIN="$$(command -v protoc-gen-go-grpc 2>/dev/null || true)"; \
	[ -n "$$PROTOC_GRPC_BIN" ] || PROTOC_GRPC_BIN="$$GO_BIN_DIR/protoc-gen-go-grpc"; \
	[ -x "$$BUF_BIN" ] || { echo "buf is required (install: go install github.com/bufbuild/buf/cmd/buf@v1.57.0)"; exit 1; }; \
	[ -x "$$PROTOC_GO_BIN" ] || { echo "protoc-gen-go is required (install: go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.11)"; exit 1; }; \
	[ -x "$$PROTOC_GRPC_BIN" ] || { echo "protoc-gen-go-grpc is required (install: go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1)"; exit 1; }; \
	PATH="$$GO_BIN_DIR:$$PATH"; \
	cd internal/workerapi/proto && "$$BUF_BIN" generate

bench-pull:
	@mkdir -p "$(BENCH_DIR)"
	GOMAXPROCS=1 go test ./internal/pullapi $(PULL_BENCH_FLAGS) | tee "$(PULL_BENCH_CURRENT)"

bench-pull-baseline:
	@mkdir -p "$(BENCH_DIR)"
	GOMAXPROCS=1 go test ./internal/pullapi $(PULL_BENCH_FLAGS) | tee "$(PULL_BENCH_BASELINE)"

bench-pull-compare:
	@test -f "$(PULL_BENCH_BASELINE)" || (echo "baseline missing: run 'make bench-pull-baseline' first" && exit 1)
	@test -f "$(PULL_BENCH_CURRENT)" || (echo "current run missing: run 'make bench-pull' first" && exit 1)
	go run "$(BENCHSTAT_CMD)" "$(PULL_BENCH_BASELINE)" "$(PULL_BENCH_CURRENT)" | tee "$(PULL_BENCH_COMPARE)"

bench-pull-extend:
	@mkdir -p "$(BENCH_DIR)"
	GOMAXPROCS=1 go test ./internal/pullapi $(PULL_EXTEND_BENCH_FLAGS) | tee "$(PULL_EXTEND_CURRENT)"

bench-pull-extend-compare:
	@test -f "$(PULL_BENCH_BASELINE)" || (echo "baseline missing: run 'make bench-pull-baseline' first" && exit 1)
	@test -f "$(PULL_EXTEND_CURRENT)" || (echo "current extend run missing: run 'make bench-pull-extend' first" && exit 1)
	go run "$(BENCHSTAT_CMD)" "$(PULL_BENCH_BASELINE)" "$(PULL_EXTEND_CURRENT)" | tee "$(PULL_EXTEND_COMPARE)"

bench-pull-drain:
	@mkdir -p "$(BENCH_DIR)"
	GOMAXPROCS=1 go test ./internal/pullapi $(PULL_DRAIN_BENCH_FLAGS) | tee "$(PULL_DRAIN_CURRENT)"

bench-pull-drain-baseline:
	@mkdir -p "$(BENCH_DIR)"
	GOMAXPROCS=1 go test ./internal/pullapi $(PULL_DRAIN_BENCH_FLAGS) | tee "$(PULL_DRAIN_BASELINE)"

bench-pull-drain-compare:
	@test -f "$(PULL_DRAIN_BASELINE)" || (echo "baseline missing: run 'make bench-pull-drain-baseline' first" && exit 1)
	@test -f "$(PULL_DRAIN_CURRENT)" || (echo "current drain run missing: run 'make bench-pull-drain' first" && exit 1)
	go run "$(BENCHSTAT_CMD)" "$(PULL_DRAIN_BASELINE)" "$(PULL_DRAIN_CURRENT)" | tee "$(PULL_DRAIN_COMPARE)"

bench-pull-contention:
	@mkdir -p "$(BENCH_DIR)"
	GOMAXPROCS=4 go test ./internal/pullapi $(PULL_CONTENTION_BENCH_FLAGS) | tee "$(PULL_CONTENTION_CURRENT)"

bench-pull-contention-baseline:
	@mkdir -p "$(BENCH_DIR)"
	GOMAXPROCS=4 go test ./internal/pullapi $(PULL_CONTENTION_BENCH_FLAGS) | tee "$(PULL_CONTENTION_BASELINE)"

bench-pull-contention-compare:
	@test -f "$(PULL_CONTENTION_BASELINE)" || (echo "baseline missing: run 'make bench-pull-contention-baseline' first" && exit 1)
	@test -f "$(PULL_CONTENTION_CURRENT)" || (echo "current contention run missing: run 'make bench-pull-contention' first" && exit 1)
	go run "$(BENCHSTAT_CMD)" "$(PULL_CONTENTION_BASELINE)" "$(PULL_CONTENTION_CURRENT)" | tee "$(PULL_CONTENTION_COMPARE)"

bench-pull-mixed:
	@mkdir -p "$(BENCH_DIR)"
	GOMAXPROCS=4 go test ./internal/pullapi $(PULL_MIXED_BENCH_FLAGS) | tee "$(PULL_MIXED_CURRENT)"

bench-pull-mixed-baseline:
	@mkdir -p "$(BENCH_DIR)"
	GOMAXPROCS=4 go test ./internal/pullapi $(PULL_MIXED_BENCH_FLAGS) | tee "$(PULL_MIXED_BASELINE)"

bench-pull-mixed-compare:
	@test -f "$(PULL_MIXED_BASELINE)" || (echo "baseline missing: run 'make bench-pull-mixed-baseline' first" && exit 1)
	@test -f "$(PULL_MIXED_CURRENT)" || (echo "current mixed run missing: run 'make bench-pull-mixed' first" && exit 1)
	go run "$(BENCHSTAT_CMD)" "$(PULL_MIXED_BASELINE)" "$(PULL_MIXED_CURRENT)" | tee "$(PULL_MIXED_COMPARE)"

bench-push-mixed:
	@mkdir -p "$(BENCH_DIR)"
	GOMAXPROCS=4 go test ./internal/dispatcher $(PUSH_MIXED_BENCH_FLAGS) | tee "$(PUSH_MIXED_CURRENT)"

bench-push-mixed-baseline:
	@mkdir -p "$(BENCH_DIR)"
	GOMAXPROCS=4 go test ./internal/dispatcher $(PUSH_MIXED_BENCH_FLAGS) | tee "$(PUSH_MIXED_BASELINE)"

bench-push-mixed-compare:
	@test -f "$(PUSH_MIXED_BASELINE)" || (echo "baseline missing: run 'make bench-push-mixed-baseline' first" && exit 1)
	@test -f "$(PUSH_MIXED_CURRENT)" || (echo "current mixed push run missing: run 'make bench-push-mixed' first" && exit 1)
	go run "$(BENCHSTAT_CMD)" "$(PUSH_MIXED_BASELINE)" "$(PUSH_MIXED_CURRENT)" | tee "$(PUSH_MIXED_COMPARE)"

bench-push-skewed:
	@mkdir -p "$(BENCH_DIR)"
	GOMAXPROCS=4 go test ./internal/dispatcher $(PUSH_SKEWED_BENCH_FLAGS) | tee "$(PUSH_SKEWED_CURRENT)"

bench-push-skewed-baseline:
	@mkdir -p "$(BENCH_DIR)"
	GOMAXPROCS=4 go test ./internal/dispatcher $(PUSH_SKEWED_BENCH_FLAGS) | tee "$(PUSH_SKEWED_BASELINE)"

bench-push-skewed-compare:
	@test -f "$(PUSH_SKEWED_BASELINE)" || (echo "baseline missing: run 'make bench-push-skewed-baseline' first" && exit 1)
	@test -f "$(PUSH_SKEWED_CURRENT)" || (echo "current skewed push run missing: run 'make bench-push-skewed' first" && exit 1)
	go run "$(BENCHSTAT_CMD)" "$(PUSH_SKEWED_BASELINE)" "$(PUSH_SKEWED_CURRENT)" | tee "$(PUSH_SKEWED_COMPARE)"

adaptive-ab:
	"$(ADAPTIVE_AB_SCRIPT)" \
		--scenario mixed \
		--output-root "$(ADAPTIVE_AB_ROOT)" \
		--backend "$(ADAPTIVE_AB_BACKEND)" \
		--duration-seconds "$(ADAPTIVE_AB_DURATION)" \
		--ingress-workers "$(ADAPTIVE_AB_INGRESS_WORKERS)" \
		--mixed-drain-workers "$(ADAPTIVE_AB_MIXED_DRAIN_WORKERS)" \
		--dequeue-batch "$(ADAPTIVE_AB_DEQUEUE_BATCH)" \
		--queue-max-depth "$(ADAPTIVE_AB_QUEUE_MAX_DEPTH)"

adaptive-ab-all:
	"$(ADAPTIVE_AB_SCRIPT)" \
		--scenario all \
		--output-root "$(ADAPTIVE_AB_ROOT)" \
		--backend "$(ADAPTIVE_AB_BACKEND)" \
		--duration-seconds "$(ADAPTIVE_AB_DURATION)" \
		--ingress-workers "$(ADAPTIVE_AB_INGRESS_WORKERS)" \
		--mixed-drain-workers "$(ADAPTIVE_AB_MIXED_DRAIN_WORKERS)" \
		--dequeue-batch "$(ADAPTIVE_AB_DEQUEUE_BATCH)" \
		--queue-max-depth "$(ADAPTIVE_AB_QUEUE_MAX_DEPTH)"

adaptive-ab-pull:
	"$(ADAPTIVE_AB_SCRIPT)" \
		--scenario pull \
		--output-root "$(ADAPTIVE_AB_ROOT)" \
		--backend "$(ADAPTIVE_AB_BACKEND)" \
		--duration-seconds "$(ADAPTIVE_AB_DURATION)" \
		--ingress-workers "$(ADAPTIVE_AB_INGRESS_WORKERS)" \
		--mixed-drain-workers "$(ADAPTIVE_AB_MIXED_DRAIN_WORKERS)" \
		--dequeue-batch "$(ADAPTIVE_AB_DEQUEUE_BATCH)" \
		--queue-max-depth "$(ADAPTIVE_AB_QUEUE_MAX_DEPTH)"

adaptive-ab-mixed:
	"$(ADAPTIVE_AB_SCRIPT)" \
		--scenario mixed \
		--output-root "$(ADAPTIVE_AB_ROOT)" \
		--backend "$(ADAPTIVE_AB_BACKEND)" \
		--duration-seconds "$(ADAPTIVE_AB_DURATION)" \
		--ingress-workers "$(ADAPTIVE_AB_INGRESS_WORKERS)" \
		--mixed-drain-workers "$(ADAPTIVE_AB_MIXED_DRAIN_WORKERS)" \
		--dequeue-batch "$(ADAPTIVE_AB_DEQUEUE_BATCH)" \
		--queue-max-depth "$(ADAPTIVE_AB_QUEUE_MAX_DEPTH)"

adaptive-ab-mixed-saturation:
	"$(ADAPTIVE_AB_SCRIPT)" \
		--scenario mixed \
		--output-root "$(ADAPTIVE_AB_ROOT)" \
		--backend "$(ADAPTIVE_AB_BACKEND)" \
		--duration-seconds "$(ADAPTIVE_AB_MIXED_SAT_DURATION)" \
		--ingress-workers "$(ADAPTIVE_AB_MIXED_SAT_INGRESS_WORKERS)" \
		--mixed-drain-workers "$(ADAPTIVE_AB_MIXED_SAT_DRAIN_WORKERS)" \
		--dequeue-batch "$(ADAPTIVE_AB_MIXED_SAT_DEQUEUE_BATCH)" \
		--queue-max-depth "$(ADAPTIVE_AB_MIXED_SAT_QUEUE_MAX_DEPTH)"

adaptive-ab-guardrail-check:
	@test -n "$(RUN_ROOT)" || (echo "RUN_ROOT is required, e.g. make adaptive-ab-guardrail-check RUN_ROOT=.artifacts/adaptive-ab/<run-id>" && exit 1)
	"$(ADAPTIVE_GUARDRAIL_SCRIPT)" \
		--run-root "$(RUN_ROOT)" \
		--scenario mixed \
		--max-ack-conflict-ratio "$(ADAPTIVE_ACK_CONFLICT_MAX_RATIO)" \
		--min-acked-total "$(ADAPTIVE_ACK_CONFLICT_MIN_ACKED)" \
		--max-route-ack-conflict-ratio "$(ADAPTIVE_ROUTE_ACK_CONFLICT_MAX_RATIO)" \
		--min-route-acked-total "$(ADAPTIVE_ROUTE_ACK_CONFLICT_MIN_ACKED)"

adaptive-ab-mixed-guardrail:
	@RUN_ID="$$(date -u +%Y%m%d-%H%M%S)"; \
	"$(ADAPTIVE_AB_SCRIPT)" \
		--scenario mixed \
		--output-root "$(ADAPTIVE_AB_ROOT)" \
		--backend "$(ADAPTIVE_AB_BACKEND)" \
		--duration-seconds "$(ADAPTIVE_AB_MIXED_SAT_DURATION)" \
		--ingress-workers "$(ADAPTIVE_AB_MIXED_SAT_INGRESS_WORKERS)" \
		--mixed-drain-workers "$(ADAPTIVE_AB_MIXED_SAT_DRAIN_WORKERS)" \
		--dequeue-batch "$(ADAPTIVE_AB_MIXED_SAT_DEQUEUE_BATCH)" \
		--queue-max-depth "$(ADAPTIVE_AB_MIXED_SAT_QUEUE_MAX_DEPTH)" \
		--run-id "$$RUN_ID"; \
	"$(ADAPTIVE_GUARDRAIL_SCRIPT)" \
		--run-root "$(ADAPTIVE_AB_ROOT)/$$RUN_ID" \
		--scenario mixed \
		--max-ack-conflict-ratio "$(ADAPTIVE_ACK_CONFLICT_MAX_RATIO)" \
		--min-acked-total "$(ADAPTIVE_ACK_CONFLICT_MIN_ACKED)" \
		--max-route-ack-conflict-ratio "$(ADAPTIVE_ROUTE_ACK_CONFLICT_MAX_RATIO)" \
		--min-route-acked-total "$(ADAPTIVE_ROUTE_ACK_CONFLICT_MIN_ACKED)"

release-check: check build

dist:
	go run ./internal/tools/release -out dist

dist-signed:
	@test -n "$(HOOKAIDO_SIGNING_KEY_FILE)" || (echo "HOOKAIDO_SIGNING_KEY_FILE is required" && exit 1)
	go run ./internal/tools/release -out dist -signing-key "$(HOOKAIDO_SIGNING_KEY_FILE)"

dist-verify:
	@test -n "$(CHECKSUMS_FILE)" || (echo "CHECKSUMS_FILE is required" && exit 1)
	go run ./cmd/hookaido verify-release --checksums "$(CHECKSUMS_FILE)" --require-signature --require-sbom

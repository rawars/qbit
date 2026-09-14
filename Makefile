REDIS_ADDR ?= 127.0.0.1:6379
QBIT_QUEUE ?= default
FUZZ_TIME ?= 10s
COVERAGE_MIN ?= 70
QBIT_METRICS_IMAGE ?= qbit-metrics:dev
VERSION ?= dev
COMMIT ?= working-tree

.PHONY: test integration staticcheck govulncheck audit fuzz coverage benchmark chaos metrics qbitctl image-metrics observability demo-producer demo-worker

test:
	go test -race ./...

integration:
	QBIT_REDIS_ADDR=$(REDIS_ADDR) go test -race -count=1 ./...

staticcheck:
	go tool staticcheck ./...

govulncheck:
	go tool govulncheck ./...

audit: staticcheck govulncheck
	go vet ./...

fuzz:
	go test ./packages/go/qbit -run '^$$' -fuzz '^FuzzIdentifiers$$' -fuzztime=$(FUZZ_TIME)

coverage:
	QBIT_REDIS_ADDR=$(REDIS_ADDR) go test -race -covermode=atomic -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out
	@total=$$(go tool cover -func=coverage.out | awk '/^total:/ {gsub("%", "", $$3); print $$3}'); \
	awk -v total="$$total" -v minimum="$(COVERAGE_MIN)" 'BEGIN { if (total + 0 < minimum + 0) { printf "coverage %.1f%% is below %.1f%%\n", total, minimum; exit 1 } }'

benchmark:
	QBIT_REDIS_ADDR=$(REDIS_ADDR) go test -run '^$$' -bench . -benchmem ./packages/go/qbit

chaos:
	QBIT_REDIS_ADDR=$(REDIS_ADDR) go test -race -tags=chaos -run '^TestChaos' -count=1 ./packages/go/qbit

metrics:
	QBIT_REDIS_ADDR=$(REDIS_ADDR) go run ./cmd/qbit-metrics

qbitctl:
	QBIT_REDIS_ADDR=$(REDIS_ADDR) go run ./cmd/qbitctl $(ARGS)

image-metrics:
	docker build -f cmd/qbit-metrics/Dockerfile \
		--build-arg VERSION=$(VERSION) \
		--build-arg COMMIT=$(COMMIT) \
		-t $(QBIT_METRICS_IMAGE) .

observability:
	docker compose -f docs/observability/docker-compose.yml up --build

demo-producer:
	QBIT_REDIS_ADDR=$(REDIS_ADDR) QBIT_QUEUE=$(QBIT_QUEUE) go run ./apps/qbit-demo/producer

demo-worker:
	QBIT_REDIS_ADDR=$(REDIS_ADDR) QBIT_QUEUE=$(QBIT_QUEUE) go run ./apps/qbit-demo/worker

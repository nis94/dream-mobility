SHELL := /usr/bin/env bash
.DEFAULT_GOAL := help

COMPOSE := docker compose -f deploy/docker-compose.yml -p dream-mobility

# ---- Help ---------------------------------------------------------------------

.PHONY: help
help: ## Show this help
	@awk 'BEGIN {FS = ":.*##"; printf "Targets:\n"} /^[a-zA-Z0-9_.-]+:.*##/ { printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

# ---- Local infra (docker-compose) --------------------------------------------

.PHONY: up
up: ## Bring up the local infra stack (Kafka, SR, Postgres, ClickHouse, MinIO, Iceberg REST)
	$(COMPOSE) up -d --wait
	@echo
	@echo "Stack is healthy. Endpoints:"
	@echo "  Kafka broker (host):    localhost:29092"
	@echo "  Schema Registry:        http://localhost:8081"
	@echo "  Postgres:               postgres://postgres:postgres@localhost:5432/mobility"
	@echo "  ClickHouse HTTP:        http://localhost:8123  (default user, no password)"
	@echo "  ClickHouse native:      tcp://localhost:9000"
	@echo "  MinIO API:              http://localhost:9100  (minioadmin/minioadmin)"
	@echo "  MinIO Console:          http://localhost:9101"
	@echo "  Iceberg REST catalog:   http://localhost:8181"
	@echo "  Kafka UI:               http://localhost:8088"

.PHONY: down
down: ## Stop the stack (keep volumes)
	$(COMPOSE) down

.PHONY: down-v
down-v: ## Stop the stack AND wipe all data volumes (destructive)
	$(COMPOSE) down -v

.PHONY: ps
ps: ## Show stack status
	$(COMPOSE) ps

.PHONY: logs
logs: ## Tail logs from all services
	$(COMPOSE) logs -f --tail=100

.PHONY: kafka-topics
kafka-topics: ## List Kafka topics
	$(COMPOSE) exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# ---- Go ----------------------------------------------------------------------

.PHONY: build
build: ## Build all Go binaries
	go build ./...

.PHONY: test
test: ## Run unit tests
	go test -race -count=1 ./...

.PHONY: test-integration
test-integration: ## Run integration tests (requires Docker; uses testcontainers-go)
	go test -race -count=1 -tags=integration -timeout=10m ./...

.PHONY: lint
lint: ## Run linters
	golangci-lint run ./...

.PHONY: fmt
fmt: ## Format Go code
	gofmt -w .
	goimports -w .

.PHONY: gen
gen: ## Regenerate Go types from Avro schemas (requires avrogen: go install github.com/hamba/avro/v2/cmd/avrogen@latest)
	go generate ./...

.PHONY: register-schemas
register-schemas: ## Register Avro schemas with the Schema Registry (stack must be running)
	./scripts/register-schemas.sh

# ---- Python generator --------------------------------------------------------

.PHONY: generator-install
generator-install: ## Install Python generator deps via uv
	cd tools/generator && uv sync

.PHONY: generator-run
generator-run: ## Run the synthetic event generator (override ARGS=...)
	cd tools/generator && uv run python gen.py $(ARGS)

# ---- End-to-end --------------------------------------------------------------

.PHONY: e2e
e2e: ## End-to-end test against the running stack -- populated in Phase 3
	@echo "Phase 3 will populate this target with the dedupe + out-of-order correctness test"

# ---- Cleanliness -------------------------------------------------------------

.PHONY: clean
clean: ## Remove build artifacts
	rm -rf bin/ dist/ coverage.* *.out

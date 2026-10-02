# FlowMesh developer commands. Requires `make` (Git Bash/WSL/macOS/Linux --
# there is no native `make` on plain Windows cmd/PowerShell) and either
# `uv` (https://docs.astral.sh/uv/) or an activated .venv. Targets that need
# `uv` say so in their help text.

.DEFAULT_GOAL := help

.PHONY: help install sync api fraud orders inventory review loadtest chaos reconcile seed migrate proto lint format format-check typecheck test test-integration smoke evidence check up down logs clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

install: ## Create/refresh .venv, install all deps (incl. dev), install git hooks
	uv sync --extra dev
	uv run pre-commit install

sync: ## Sync .venv to exactly match pyproject.toml
	uv sync --extra dev

run: api ## Alias for `make api`

api: ## Run the FastAPI API (http://localhost:8000/docs)
	uv run uvicorn backend.main:app --reload --host 0.0.0.0 --port 8000

fraud: ## Run the gRPC fraud scoring service (port 50052)
	uv run python -m backend.scripts.run_fraud_server

orders: ## Run the order processor (consume order-events, score, route)
	uv run python -m backend.scripts.run_order_consumer

inventory: ## Run the inventory worker + the reservation expiry sweeper
	uv run python -m backend.scripts.run_inventory_worker

review: ## Run the review worker (consume review-queue, apply decisions)
	uv run python -m backend.scripts.run_review_worker

all-local: ## Run the whole pipeline in one terminal (SQLite; in-process transports)
	uv run python -m backend.scripts.dev_stack

seed: ## Seed 12 warehouses + stock (idempotent; `--overwrite-stock` is destructive)
	uv run python -m backend.scripts.seed

migrate: ## Apply migrations to the configured database
	uv run alembic upgrade head

proto: ## Regenerate the gRPC stubs from proto/flowmesh/v1/fraud.proto
	uv run python -m grpc_tools.protoc -I proto \
		--python_out=backend/grpc_service/generated \
		--pyi_out=backend/grpc_service/generated \
		--grpc_python_out=backend/grpc_service/generated \
		proto/flowmesh/v1/fraud.proto
	@echo "Regenerated. Generated output is excluded from ruff and mypy;"
	@echo "run 'make lint typecheck test' afterwards."

lint: ## Lint with ruff
	uv run ruff check .

format: ## Auto-format with ruff (rewrites files)
	uv run ruff format .

format-check: ## Check formatting without modifying files (what CI runs)
	uv run ruff format --check .

typecheck: ## Type-check with mypy
	uv run mypy .

test: ## Run the whole suite (integration tests skip themselves without services)
	uv run pytest

test-integration: ## Run only the real-service tests (needs `make up` first)
	uv run pytest -m integration

loadtest: ## Sustained-rate load test; writes docs/load-test-report.md; non-zero on breach
	uv run python -m backend.scripts.loadtest --rate 500 --duration 10

chaos: ## Kill a consumer mid-stream; writes docs/chaos-test.md; non-zero on data loss
	uv run python -m backend.scripts.chaos_test --stream 400

reconcile: ## Compare the event log against the database; writes docs/reconciliation.md
	uv run python -m backend.scripts.reconcile --report docs/reconciliation.md \
		$(if $(LOG_FILE),--log-file $(LOG_FILE) --from-stream 0,)

# Reconcile a live broker's log instead of generating one:
#   make reconcile LOG_FILE=events.json
# produced by
#   kcat -C -b localhost:9092 -t order-events -o beginning -J > events.json
# Without LOG_FILE the script generates the log itself from a short in-process run,
# so the comparison is between one run's log and that same run's database.

evidence: ## Load test + chaos test + reconciliation, i.e. every measured claim
	@echo "Running the three evidence scripts. Each exits non-zero on failure."
	$(MAKE) loadtest
	$(MAKE) chaos
	$(MAKE) reconcile

smoke: ## End-to-end across two real processes (needs postgres; see scripts/smoke_e2e.py)
	uv run python scripts/smoke_e2e.py

check: format-check lint typecheck test ## Full quality gate (what CI runs)

up: ## Start the full local stack: kafka, rabbitmq, postgres, redis, api, fraud, workers
	docker compose up -d --build
	docker compose exec api python -m backend.scripts.seed

down: ## Stop the stack and delete its volumes
	docker compose down -v

logs: ## Tail the stack's logs
	docker compose logs -f

clean: ## Remove caches and build artifacts (keeps .venv and the database volumes)
	rm -rf .pytest_cache .mypy_cache .ruff_cache htmlcov .coverage dist build ./*.egg-info
	find . -type d -name __pycache__ -not -path "./.venv/*" -exec rm -rf {} +

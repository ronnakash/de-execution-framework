.PHONY: test test-unit test-integration test-e2e test-e2e-ui test-all local-unit local-e2e lint format run migrate build-ui dev-ui dev dev-stop infra-up infra-down setup setup-full

PYTHON ?= python3
PYTEST = $(PYTHON) -m pytest

test:
	$(PYTEST) de_platform/ tests/unit/ -v -m "not integration and not e2e" -n auto

test-unit:
	$(PYTEST) de_platform/ tests/unit/ -v -m "not integration and not e2e" -n auto

test-integration:
	$(PYTEST) tests/integration/ -v -m integration --tb=short

test-e2e:
	$(PYTEST) tests/e2e/ -v -m e2e --tb=short -n auto

test-e2e-ui:
	$(PYTEST) tests/e2e_ui/ -v -m e2e_ui --tb=short

test-all:
	$(PYTEST) -v --tb=short -n auto

lint:
	$(PYTHON) -m ruff check de_platform/

format:
	$(PYTHON) -m ruff format de_platform/

run:
	$(PYTHON) -m de_platform run $(module) $(args)

migrate:
	$(PYTHON) -m de_platform migrate $(cmd) $(args)

local-unit: setup
	.venv/bin/python -m pytest de_platform/ tests/unit/ -v -m "not integration and not e2e"

local-e2e: setup-full infra-up
	.venv/bin/python -m pytest tests/e2e/ -v -m e2e --tb=short

infra-up:
	docker compose -f .devcontainer/docker-compose.yml up -d postgres redis minio zookeeper kafka clickhouse prometheus loki grafana

infra-down:
	docker compose -f .devcontainer/docker-compose.yml down

build-ui:
	cd ui && npm install && npm run build

dev-ui:
	cd ui && npm run dev

dev:
	./scripts/dev.sh

dev-stop:
	@if [ -f .dev-pids ]; then \
		while IFS= read -r pid; do kill "$$pid" 2>/dev/null || true; done < .dev-pids; \
		rm -f .dev-pids; \
		echo "Services stopped."; \
	else \
		echo "No .dev-pids file found."; \
	fi

setup:
	python3.12 -m venv .venv
	.venv/bin/pip install -e '.[dev]'

setup-full:
	python3.12 -m venv .venv
	.venv/bin/pip install -e '.[dev,infra]'

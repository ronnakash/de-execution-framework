.PHONY: test test-unit test-integration test-e2e test-all local-unit local-e2e lint format run migrate infra-up infra-down setup setup-full

PYTHON ?= python3
PYTEST = $(PYTHON) -m pytest

test:
	$(PYTEST) de_platform/ tests/unit/ -v -m "not integration and not e2e"

test-unit:
	$(PYTEST) de_platform/ tests/unit/ -v -m "not integration and not e2e"

test-integration:
	$(PYTEST) tests/integration/ -v -m integration --tb=short

test-e2e:
	$(PYTEST) tests/e2e/ -v -m e2e --tb=short -n auto

test-all:
	$(PYTEST) -v --tb=short

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

setup:
	python3.12 -m venv .venv
	.venv/bin/pip install -e '.[dev]'

setup-full:
	python3.12 -m venv .venv
	.venv/bin/pip install -e '.[dev,infra]'

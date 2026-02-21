.PHONY: test test-unit test-integration test-e2e test-real-infra local-unit local-e2e local-e2e-infra lint format run migrate infra-up infra-down setup setup-full

PYTHON ?= python3
PYTEST = $(PYTHON) -m pytest

test:
	$(PYTEST) de_platform/ tests/ -v -m "not real_infra"

test-unit:
	$(PYTEST) de_platform/ tests/ -v -k "not postgres" -m "not real_infra"

test-e2e:
	$(PYTEST) tests/e2e/ -v -m "not real_infra" --tb=short

test-real-infra:
	$(PYTEST) tests/e2e/ tests/integration/ -v -m real_infra --tb=short -n 12

lint:
	$(PYTHON) -m ruff check de_platform/

format:
	$(PYTHON) -m ruff format de_platform/

run:
	$(PYTHON) -m de_platform run $(module) $(args)

migrate:
	$(PYTHON) -m de_platform migrate $(cmd) $(args)

test-integration:
	$(PYTEST) tests/integration/ -v -m real_infra --tb=short

local-unit: setup
	.venv/bin/python -m pytest de_platform/ tests/ -v -k "not postgres" -m "not real_infra"

local-e2e: setup
	.venv/bin/python -m pytest tests/e2e/ -v -m "not real_infra" --tb=short

local-e2e-infra: setup-full infra-up
	.venv/bin/python -m pytest tests/e2e/ tests/integration/ -v -m real_infra --tb=short -n 4

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

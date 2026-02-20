.PHONY: test test-unit test-integration test-e2e test-real-infra test-real-infra-dev lint format run migrate infra-up infra-down

test:
	pytest de_platform/ tests/ -v -m "not real_infra"

test-unit:
	pytest de_platform/ tests/ -v -k "not postgres" -m "not real_infra"

test-e2e:
	pytest tests/e2e/ -v -m "not real_infra" --tb=short

test-real-infra:
	pytest tests/e2e/ -v -m real_infra --tb=short -x

test-real-infra-dev:
	USE_DEV_INFRA=1 pytest tests/e2e/ -v -m real_infra --tb=short -x

lint:
	ruff check de_platform/

format:
	ruff format de_platform/

run:
	python -m de_platform run $(module) $(args)

migrate:
	python -m de_platform migrate $(cmd) $(args)

test-integration:
	pytest -v -k "postgres" --tb=short

infra-up:
	docker compose -f .devcontainer/docker-compose.yml up -d postgres redis minio zookeeper kafka clickhouse

infra-down:
	docker compose -f .devcontainer/docker-compose.yml down

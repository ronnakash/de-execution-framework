.PHONY: test test-unit test-integration lint format run migrate infra-up infra-down

test:
	pytest de_platform/ -v

test-unit:
	pytest de_platform/ -v -k "not postgres"

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
	docker compose -f .devcontainer/docker-compose.yml up -d postgres redis minio zookeeper kafka

infra-down:
	docker compose -f .devcontainer/docker-compose.yml down

.PHONY: test test-unit lint format run migrate

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

.PHONY: test lint format run

test:
	pytest de_platform/modules/ -v

lint:
	ruff check de_platform/

format:
	ruff format de_platform/

run:
	python -m de_platform run $(module) $(args)

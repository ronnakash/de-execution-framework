.PHONY: test lint format run

test:
	pytest modules/ -v

lint:
	ruff check de_platform/ modules/

format:
	ruff format de_platform/ modules/

run:
	python -m de_platform run $(module) $(args)

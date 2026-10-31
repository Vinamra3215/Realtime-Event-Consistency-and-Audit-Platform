.PHONY: install test lint up down build smoke live benchmark
install:
	python -m pip install -e '.[dev]'
test:
	python -m pytest tests/unit -q
lint:
	python -m ruff check src tests
	python -m ruff format --check src tests
up:
	docker compose up -d --wait postgres kafka
down:
	docker compose down
build:
	docker compose build app
smoke:
	python -m recap.cli --database-url sqlite:///outputs/demo.db simulate --csv tests/fixtures/paysim_sample.csv --output outputs/demo
live:
	RECAP_INTEGRATION=1 python -m pytest tests/integration tests/end_to_end -q
benchmark:
	python -m recap.cli --database-url sqlite:///outputs/benchmark.db benchmark --csv tests/fixtures/paysim_sample.csv --output outputs/benchmark

.PHONY: help install test lint format clean pre-commit docker-build

help:
	@echo "Available commands:"
	@echo "  install          - Install dependencies"
	@echo "  test             - Run tests"
	@echo "  test-cov         - Run tests with coverage report"
	@echo "  lint             - Run linting checks"
	@echo "  format           - Format code"
	@echo "  pre-commit       - Install and run pre-commit hooks"
	@echo "  clean            - Remove build artifacts"
	@echo "  docker-build     - Build Docker image"
	@echo "  run-api          - Run FastAPI locally"

install:
	poetry install

test:
	poetry run pytest -v

test-cov:
	poetry run pytest --cov=src --cov-report=html --cov-report=term --cov-report=xml

lint:
	poetry run black --check src tests
	poetry run ruff check src tests
	poetry run mypy src

format:
	poetry run black src tests
	poetry run ruff check --fix src tests

pre-commit:
	poetry run pre-commit install
	poetry run pre-commit install --hook-type commit-msg
	poetry run pre-commit run --all-files

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	rm -rf .pytest_cache .mypy_cache .coverage htmlcov coverage.xml build dist

docker-build:
	docker build -t togather-ml/feature-pipeline:latest .

run-api:
	poetry run uvicorn src.api.main:app --reload --host 0.0.0.0 --port 8000

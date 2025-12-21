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



# Flink Docker Image (for PyFlink session job)

FLINK_IMAGE ?= togather-ml/flink-feature-job
FLINK_TAG ?= latest

flink-build: ## Build PyFlink session job Docker image
	docker build -t $(FLINK_IMAGE):$(FLINK_TAG) -f docker/Dockerfile.flink .
	@echo "Built $(FLINK_IMAGE):$(FLINK_TAG)"

flink-push: ## Push Flink image to registry (set FLINK_IMAGE for your registry)
	docker push $(FLINK_IMAGE):$(FLINK_TAG)
	@echo "Pushed $(FLINK_IMAGE):$(FLINK_TAG)"

flink-deploy: ## Deploy Flink job to Kubernetes
	kubectl apply -f k8s/flink-feature-job.yaml -n stream-processing
	@echo "Deployed Flink feature job"

flink-logs: ## View Flink job logs
	kubectl logs -l app=flink-feature-job -n stream-processing --tail=100 -f



# DVC Commands (Data Version Control with MinIO)

dvc-init: ## Initialize DVC (already done if .dvc/ exists)
	@if [ ! -d ".dvc" ]; then poetry run dvc init; fi
	@echo "DVC initialized"

dvc-pull: ## Pull datasets from MinIO (dvc-data bucket)
	@poetry run dvc pull
	@echo "Datasets pulled from MinIO (dvc-data)"

dvc-push: ## Push datasets to MinIO (dvc-data bucket)
	@poetry run dvc push
	@echo "Datasets pushed to MinIO (dvc-data)"

dvc-status: ## Check DVC status
	@poetry run dvc status

dvc-add: ## Add data files to DVC tracking (usage: make dvc-add FILE=data/myfile.parquet)
	@poetry run dvc add $(FILE)
	@echo "Added $(FILE) to DVC tracking"

dvc-gc: ## Garbage collect unused cache
	@poetry run dvc gc --workspace -f
	@echo "DVC cache cleaned"

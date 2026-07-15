# NY IEDR Data Platform - Makefile
.PHONY: help install install-dev test lint format type-check clean deploy validate run

help:
	@echo "NY IEDR Data Platform - Makefile Commands"
	@echo ""
	@echo "Setup:"
	@echo "  make install       Install runtime dependencies"
	@echo "  make install-dev   Install all dependencies (runtime + dev)"
	@echo ""
	@echo "Development:"
	@echo "  make test          Run all tests with coverage"
	@echo "  make test-unit     Run unit tests only"
	@echo "  make lint          Run linter (ruff)"
	@echo "  make format        Auto-format code (ruff)"
	@echo "  make type-check    Run type checker (mypy)"
	@echo "  make clean         Remove build artifacts"
	@echo ""
	@echo "Databricks:"
	@echo "  make validate      Validate DABs bundle"
	@echo "  make deploy        Deploy to dev environment"
	@echo "  make deploy-prod   Deploy to production"
	@echo "  make run           Run pipeline in dev environment"

install:
	pip install -r requirements.txt

install-dev:
	pip install -r requirements.txt -r requirements-dev.txt
	pre-commit install

test:
	pytest

test-unit:
	pytest -m unit

lint:
	ruff check pipelines/ tests/

lint-fix:
	ruff check --fix pipelines/ tests/

format:
	ruff format pipelines/ tests/

format-check:
	ruff format --check pipelines/ tests/

type-check:
	mypy pipelines/

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	rm -rf htmlcov/ .coverage coverage.xml

validate:
	databricks bundle validate --target dev

deploy:
	databricks bundle deploy --target dev

deploy-prod:
	databricks bundle deploy --target prod

run:
	databricks bundle run ny_iedr_pipeline --target dev

check-all: lint format-check type-check test

dev-setup: install-dev
	@echo "✓ Development environment ready!"

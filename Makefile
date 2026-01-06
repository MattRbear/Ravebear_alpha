.PHONY: install lint typecheck test smoke clean all

# Default target
all: lint typecheck test

# Install dependencies
install:
	pip install -e .
	pip install ruff mypy pytest

# Run linter
lint:
	python -m ruff check . --fix

# Run type checker
typecheck:
	python -m mypy --ignore-missing-imports --no-error-summary features/ feeds/ storage/ detectors/ utils/

# Run tests
test:
	python -m pytest tests/ -v

# Run smoke test (60 second WebSocket connectivity test)
smoke:
	python scripts/smoke_collect.py

# Run the collector (main entrypoint)
run:
	python main_collector.py

# Clean build artifacts
clean:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".ruff_cache" -exec rm -rf {} + 2>/dev/null || true
	rm -rf _smoke_out/ 2>/dev/null || true

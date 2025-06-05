.PHONY: help install sample-data pipeline pipeline-fast test lint format clean clean-all all export-json validate-json venv

# Virtual environment
VENV := .venv
PYTHON := $(VENV)/bin/python
PIP := $(VENV)/bin/pip

help:
	@echo "IDA ICE Energy Simulation ETL Pipeline - Available Commands"
	@echo "============================================================"
	@echo ""
	@echo "Setup:"
	@echo "  make install       Install Python dependencies"
	@echo "  make all           Install dependencies and generate sample data"
	@echo ""
	@echo "Data Generation:"
	@echo "  make sample-data   Generate synthetic simulation data"
	@echo ""
	@echo "Pipeline:"
	@echo "  make pipeline      Run the ETL pipeline with validation"
	@echo "  make pipeline-fast Run the ETL pipeline without validation"
	@echo ""
	@echo "Testing:"
	@echo "  make test          Run all unit tests"
	@echo "  make lint          Run code linting (flake8)"
	@echo "  make format        Format code with black"
	@echo ""
	@echo "Cleanup:"
	@echo "  make clean         Remove generated data and output files"
	@echo "  make clean-all     Remove generated data, output files, and temp files"
	@echo ""

install: venv  ## Install dependencies (via venv)

venv: $(VENV)/bin/activate  ## Create virtual environment and install dependencies

$(VENV)/bin/activate: requirements.txt
	python -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	touch $(VENV)/bin/activate

sample-data: venv
	@echo "Generating synthetic simulation data..."
	$(PYTHON) src/generate_synthetic_idaice.py

pipeline: venv
	@echo "Running ETL pipeline..."
	$(PYTHON) run_pipeline.py --non-interactive

pipeline-fast: venv
	@echo "Running ETL pipeline (without validation)..."
	$(PYTHON) run_pipeline.py --skip-validation --non-interactive

test: venv
	@echo "Running unit tests..."
	$(PYTHON) -m unittest discover tests/ -v && echo "✓ Tests completed"

lint: venv
	@echo "Running linter..."
	$(VENV)/bin/flake8 src/ tests/ run_pipeline.py && echo "✓ Linting completed"

format: venv
	@echo "Formatting code..."
	$(VENV)/bin/black src/ tests/ run_pipeline.py --line-length=120 && echo "✓ Formatting completed"

clean:
	@echo "Cleaning generated data and output files..."
	rm -rf data/raw/simulations/run_*
	rm -rf data/processed/parquet/*.parquet
	rm -rf data/processed/duckdb/*.duckdb
	@echo "✓ Cleaned"

clean-all: clean
	@echo "Removing temporary files..."
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
	@echo "✓ Deep clean completed"

all: install sample-data
	@echo ""
	@echo "============================================================"
	@echo "✓ Setup complete! Ready to run the pipeline."
	@echo "============================================================"
	@echo ""
	@echo "Run 'make pipeline' to process the data."

export-json: venv  ## Export simulation summary to canonical JSON for frontend
	@echo "Exporting simulation summary to canonical JSON..."
	@mkdir -p artifacts/json
	$(PYTHON) src/export_json.py
	@test -f artifacts/json/ida_ice_simulation_summary.json || (echo "ERROR: ida_ice_simulation_summary.json not created" && exit 1)
	@echo "Done! Output: artifacts/json/ida_ice_simulation_summary.json"

validate-json: venv  ## Validate exported JSON against schema
	@echo "Validating JSON schema..."
	$(PYTHON) src/validate_json.py

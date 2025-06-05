# IDA ICE Energy Simulation ETL Pipeline - Project Structure

## Overview
This project provides an ETL (Extract, Transform, Load) pipeline for processing IDA ICE (Indoor Climate and Energy) building energy simulation data.

## Directory Structure

```
.
├── data/                          # Data storage directory
│   ├── raw/                       # Raw simulation data
│   │   └── simulations/           # IDA ICE simulation output files
│   └── processed/                 # Processed data storage
│       ├── parquet/               # Parquet format output
│       └── duckdb/                # DuckDB database storage
│
├── src/                           # Source code
│   ├── __init__.py               # Package initialization
│   ├── idaice_client.py          # IDA ICE client interface
│   ├── generate_synthetic_idaice.py  # Synthetic data generator
│   └── etl/                       # ETL pipeline modules
│       ├── __init__.py           # ETL package initialization
│       ├── extract/              # Data extraction module
│       │   └── __init__.py
│       ├── transform/            # Data transformation module
│       │   └── __init__.py
│       ├── load/                 # Data loading module
│       │   └── __init__.py
│       └── validate/             # Data validation module
│           └── __init__.py
│
├── notebooks/                     # Jupyter notebooks for analysis
│   └── README.md                 # Notebooks documentation
│
├── tests/                         # Test suite
│   ├── __init__.py               # Test package initialization
│   ├── test_extract.py           # Tests for extraction module
│   ├── test_transform.py         # Tests for transformation module
│   ├── test_load.py              # Tests for loading module
│   ├── test_validate.py          # Tests for validation module
│   ├── test_idaice_client.py     # Tests for IDA ICE client
│   └── test_generate_synthetic_idaice.py  # Tests for synthetic data generator
│
└── README.md                      # Project README

```

## Module Descriptions

### Core Modules

#### `src/idaice_client.py`
Client interface for interacting with IDA ICE simulation software. Provides methods to:
- Configure simulation parameters
- Launch simulations
- Monitor simulation status
- Retrieve results

#### `src/generate_synthetic_idaice.py`
Utility for generating synthetic IDA ICE simulation data for testing and development.

### ETL Pipeline Modules

#### `src/etl/extract/`
Handles extraction of raw simulation data from IDA ICE output files and formats.

#### `src/etl/transform/`
Provides data transformation functions including:
- Data cleaning and normalization
- Time-series formatting
- Metric aggregation

#### `src/etl/load/`
Manages loading processed data into storage formats:
- Parquet files
- DuckDB databases
- Bulk loading operations

#### `src/etl/validate/`
Data validation and quality assurance functions:
- Schema validation
- Data quality checks
- Completeness verification
- Range validation

## Getting Started

### Running Tests
```bash
python -m unittest discover tests/ -v
```

### Importing Modules
```python
import sys
sys.path.insert(0, '.')

from src.idaice_client import IDAICEClient
from src.etl.extract import extract_simulation_data
from src.etl.transform import clean_data
from src.etl.load import load_to_parquet
from src.etl.validate import validate_schema
```

## Development Notes

- All modules include comprehensive docstrings
- Placeholder functions are marked with `# TODO: Implement` comments
- Test files provide structure for implementing unit tests
- Data directories include `.gitkeep` files to ensure they're tracked by git

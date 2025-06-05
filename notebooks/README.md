# Analysis Notebooks

This directory contains Jupyter notebooks for exploring and analyzing IDA ICE simulation data.

## Setup

```bash
# Install Jupyter if not already installed
pip install jupyter matplotlib seaborn

# Start Jupyter
jupyter notebook
```

## Notebooks

### 01_explore_raw.ipynb

**Purpose**: Explore raw simulation data before transformation

**Contents**:
- Load and inspect ZIP file contents
- Examine data structures and schemas
- Check data types and missing values
- Basic statistics and distributions
- Sample visualizations

**Example code**:
```python
from src.etl.extract import extract_simulation_data

# Extract a single run
run = extract_simulation_data('data/raw/simulations/run_BLDG_01_BASE.zip')

# Explore metadata
print(run['metadata'])

# Inspect zones data
print(run['zones'].head())
print(run['zones'].describe())

# Plot zone temperatures
import matplotlib.pyplot as plt
zones_df = run['zones']
for zone_id in zones_df['zone_id'].unique():
    zone_data = zones_df[zones_df['zone_id'] == zone_id]
    plt.plot(zone_data['timestamp'], zone_data['air_temp_C'], label=zone_id)
plt.legend()
plt.show()
```

### 02_quality_checks.ipynb

**Purpose**: Perform data quality checks and validation

**Contents**:
- Run validation suite
- Examine validation errors in detail
- Check for outliers and anomalies
- Temporal consistency checks
- Energy balance verification

### 03_analysis_and_viz.ipynb

**Purpose**: Perform analytical queries and create visualizations

**Key Analyses**:
1. Zone Temperature Drift Analysis
2. COP Proxy vs Outdoor Temperature
3. Scenario Comparison
4. Hourly Load Profiles
5. Energy Breakdown

## Data Access

All notebooks have access to:
- Raw simulation ZIPs in `data/raw/simulations/`
- Processed Parquet files in `data/processed/parquet/`
- DuckDB database at `data/processed/duckdb/simulations.duckdb`

## Resources

- [DuckDB SQL Documentation](https://duckdb.org/docs/sql/introduction)
- [Pandas Documentation](https://pandas.pydata.org/docs/)
- [Matplotlib Gallery](https://matplotlib.org/stable/gallery/index.html)

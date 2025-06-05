# IDA ICE Energy Simulation ETL Pipeline

A robust, production-style ETL (Extract, Transform, Load) pipeline for processing IDA ICE building energy simulation data. This pipeline extracts simulation outputs, standardizes and validates them, and loads them into an analytics-ready star schema (DuckDB / Parquet) for downstream HVAC analytics.

## Features

- **Synthetic Data Generation**: Generate realistic IDA ICE simulation data for testing
- **Mock Cloud API**: Simulates IDA ICE Cloud API for data extraction
- **Star Schema Design**: Organized dimension and fact tables for analytics
- **Data Validation**: Comprehensive quality checks including:
  - Schema validation
  - Value range validation
  - Temporal coverage checks
  - Energy plausibility checks
- **Multiple Storage Formats**: Exports to both Parquet files and DuckDB database
- **Analytical Views**: Pre-built views for common analyses

## Project Structure

```
.
├── data/
│   ├── raw/
│   │   └── simulations/          # Simulation ZIP files
│   └── processed/
│       ├── parquet/               # Parquet format output
│       └── duckdb/                # DuckDB database
├── src/
│   ├── generate_synthetic_idaice.py   # Synthetic data generator
│   ├── idaice_client.py               # Mock IDA ICE Cloud API
│   └── etl/
│       ├── extract/               # Data extraction module
│       ├── transform/             # Star schema transformation
│       ├── load/                  # Parquet & DuckDB loading
│       └── validate/              # Data validation
├── notebooks/                     # Jupyter notebooks for analysis
├── tests/                         # Unit tests
├── run_pipeline.py                # Main pipeline runner
├── requirements.txt               # Python dependencies
└── README.md
```

## Quick Start

Get started in just a few commands:

### Option 1: Using Make (Recommended)

```bash
# Install dependencies and generate sample data
make all

# Run the pipeline
make pipeline
```

### Option 2: Manual Commands

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Generate sample data
python src/generate_synthetic_idaice.py

# 3. Run the pipeline
python run_pipeline.py
```

That's it! Your data is now processed and ready for analysis.

### 🎬 Demo Steps (Frontend Integration)

Run the full pipeline and export data for the frontend demo:

```bash
# 1. Setup and activate virtual environment
python -m venv .venv
source .venv/bin/activate

# 2. Install dependencies and generate sample data
make all

# 3. Run the ETL pipeline
make pipeline

# 4. Export canonical JSON for frontend
make export-json

# 5. Validate JSON schema
make validate-json
```

**Artifact paths:**
| Output | Path | Size |
|--------|------|------|
| Raw simulation ZIPs | `data/raw/simulations/run_*/` | ~50 KB each |
| Parquet tables | `data/processed/parquet/*.parquet` | ~100 KB total |
| DuckDB database | `data/processed/duckdb/simulations.duckdb` | ~200 KB |
| **Frontend JSON** | `artifacts/json/ida_ice_simulation_summary.json` | ~2 KB |

Sync to frontend demo:
```bash
cd ../energy-pipeline-demo && ./sync-data.sh
```

### Common Commands

```bash
# Generate sample data (one-line command)
make sample-data

# Run tests
make test

# Clean up generated files
make clean

# See all available commands
make help
```

### Detailed Setup

#### 1. Setup Environment

```bash
# Create virtual environment (recommended)
python -m venv .venv
source .venv/bin/activate    # Linux/macOS
# .venv\Scripts\activate     # Windows

# Install dependencies
pip install -r requirements.txt
```

#### 2. Generate Sample Data

Generate realistic IDA ICE simulation data with a single command:

```bash
python src/generate_synthetic_idaice.py
```

This generates 6 simulation runs (3 buildings × 2 scenarios) with:
- 168 hours (7 days) of hourly data
- Zone conditions (temperature, CO₂, humidity)
- HVAC system data (power, heating, cooling)
- Building meters (electric, heating, cooling energy)
- Weather data (temperature, humidity, solar)

**Custom generation options:**

```bash
# Generate data for specific buildings and scenarios
python src/generate_synthetic_idaice.py --buildings BLDG_01 BLDG_02 --scenarios BASE

# Change duration and start date
python src/generate_synthetic_idaice.py --days 14 --start-date "2024-06-01 00:00"

# Custom output directory
python src/generate_synthetic_idaice.py --output data/custom/simulations
```

#### 3. Run ETL Pipeline

```bash
# Run full pipeline with validation
python run_pipeline.py

# Skip validation (faster)
python run_pipeline.py --skip-validation

# Custom paths
python run_pipeline.py --simulations data/raw/simulations \
                       --parquet data/output/parquet \
                       --duckdb data/output/db/simulations.duckdb
```

## Data Schema

### Dimension Tables

- **dim_building**: Building metadata (ID, name, location, floor area)
- **dim_scenario**: Scenario descriptions (baseline, retrofit, etc.)
- **dim_zone**: HVAC zones (zone ID, name, building)
- **dim_ahu**: Air handling units (AHU ID, building)
- **dim_time**: Time dimension (timestamp, year, month, day, hour, day of week)

### Fact Tables

- **fact_zone_conditions**: Zone-level conditions (temperature, CO₂, humidity)
  - Grain: one row per (timestamp, zone, scenario)
- **fact_hvac**: HVAC system performance (power, heating, cooling, COP proxy)
  - Grain: one row per (timestamp, AHU, scenario)
- **fact_meters**: Building-level energy meters (electric, heating, cooling)
  - Grain: one row per (timestamp, building, scenario)
- **fact_weather**: Weather conditions (outdoor temp, humidity, solar)
  - Grain: one row per (timestamp, building)

### Analytical Views

Pre-built DuckDB views for common analyses:

- **vw_zone_with_weather**: Zone conditions joined with weather data
- **vw_hvac_with_meters**: HVAC performance with building meters and weather
- **vw_energy_summary**: Energy consumption summary by building and scenario

## Usage Examples

### Python API

```python
from src.etl.extract import extract_runs
from src.etl.transform import transform_all
from src.etl.load import load_to_duckdb, query_duckdb
from src.etl.validate import validate_all, print_validation_results

# Extract simulation runs
runs = extract_runs('data/raw/simulations')

# Transform to star schema
schema = transform_all(runs)

# Validate data quality
results = validate_all(schema)
print_validation_results(results)

# Load to DuckDB
load_to_duckdb(schema, 'data/processed/duckdb/simulations.duckdb')

# Query the database
df = query_duckdb('data/processed/duckdb/simulations.duckdb', '''
    SELECT building_id, scenario_id, 
           SUM(electric_kwh) as total_electric,
           SUM(heating_kwh) as total_heating
    FROM vw_energy_summary
    GROUP BY building_id, scenario_id
''')
print(df)
```

### SQL Queries

```sql
-- Compare energy use between scenarios
SELECT 
    building_id,
    scenario_id,
    total_electric_kwh,
    total_heating_kwh,
    total_cooling_kwh
FROM vw_energy_summary
ORDER BY building_id, scenario_id;

-- Zone temperature drift analysis
SELECT 
    building_id,
    zone_id,
    scenario_id,
    COUNT(*) as total_hours,
    SUM(CASE WHEN ABS(temp_deviation) > 1.0 THEN 1 ELSE 0 END) as hours_out_of_bounds,
    AVG(temp_deviation) as avg_deviation
FROM vw_zone_with_weather
GROUP BY building_id, zone_id, scenario_id;

-- COP proxy vs outdoor temperature
SELECT 
    ROUND(outdoor_temp_C, 0) as temp_bin,
    AVG(cop_proxy) as avg_cop,
    COUNT(*) as num_observations
FROM vw_hvac_with_meters
WHERE power_kw > 0
GROUP BY temp_bin
ORDER BY temp_bin;
```

## Data Validation

The pipeline includes comprehensive validation checks:

### Schema Validation
- Ensures all required tables and columns exist
- Checks for null values in critical columns

### Value Range Validation
- Zone air temperature: 10-35°C
- CO₂ levels: 400-2500 ppm
- Relative humidity: 0-100%
- Outdoor temperature: -30 to 40°C
- All energy values: non-negative

### Temporal Coverage
- Verifies hourly time intervals
- Checks for missing time periods
- Ensures all fact tables cover the full time range

### Energy Plausibility
- Total energy consumption > 0
- Electric energy reasonable relative to heating/cooling
- Basic energy balance checks

## Development

### Running Tests

```bash
# Run all tests using make
make test

# Or use unittest directly
python -m unittest discover tests/ -v

# Run specific test file
python -m unittest tests/test_extract.py -v
```

### Code Quality

```bash
# Format code with black
make format

# Run linter
make lint

# Type checking (manual)
mypy src/
```

### Available Make Commands

Run `make help` to see all available commands:

- **Setup**: `make install`, `make all`
- **Data Generation**: `make sample-data`
- **Pipeline**: `make pipeline`, `make pipeline-fast`
- **Testing**: `make test`, `make lint`, `make format`
- **Cleanup**: `make clean`, `make clean-all`
- **Export**: `make export-json`

## 📤 Frontend Export

Export processed energy data to JSON for integration with the `energy-pipeline-demo` frontend:

### Environment Setup

```bash
# Create and activate virtual environment (recommended)
python -m venv .venv
source .venv/bin/activate    # Linux/macOS
# .venv\Scripts\activate     # Windows

# Install dependencies
pip install -r requirements.txt
```

### Export Command

```bash
# Export energy data to JSON
make export-json

# Output: artifacts/json/ida_ice_simulation_summary.json (canonical)
```

### Canonical Schema (Recommended)

The **canonical** export `ida_ice_simulation_summary.json` provides a rich, production-ready schema:

```json
{
  "pipeline": "ida_ice_energy_simulation",
  "generated_at": "2025-11-28T12:00:00Z",
  "scenario": {
    "name": "Baseline Energy Analysis",
    "building_type": "Office Complex",
    "location": "Tallinn, Estonia",
    "floor_area_m2": 2170,
    "simulation_period": "2024-01-01 to 2024-12-31"
  },
  "annual": {
    "total_kwh": 185000,
    "heating_kwh": 92000,
    "cooling_kwh": 18500,
    "lighting_kwh": 32400,
    "equipment_kwh": 42100
  },
  "monthly_breakdown": [
    { "month": "Jan", "heating_kwh": 16500, "cooling_kwh": 0, "total_kwh": 22300 },
    ...
  ],
  "kpis": {
    "energy_intensity_kwh_m2": 85.2,
    "heating_intensity_kwh_m2": 42.4,
    "cooling_intensity_kwh_m2": 8.5,
    "annual_co2_tons": 42.3,
    "co2_intensity_kg_m2": 19.5,
    "peak_demand_kw": 156.8,
    "comfort_hours_percent": 94.2
  }
}
```

| Section | Fields | Description |
|---------|--------|-------------|
| `pipeline` | string | Pipeline identifier |
| `generated_at` | ISO 8601 UTC | Export timestamp |
| `scenario` | object | Simulation scenario metadata |
| `annual` | object | Annual energy totals by category |
| `monthly_breakdown` | array | Monthly energy breakdown |
| `kpis` | object | Key performance indicators |

### Sync to Frontend

After export, copy to the frontend demo:

```bash
# From workspace root
cp IDA-ICE-Energy-Simulation-ETL-Pipeline/artifacts/json/ida_ice_simulation_summary.json \
   energy-pipeline-demo/public/data/

# Or use the sync script
cd energy-pipeline-demo && ./sync-data.sh
```

## Productionization Path

This pipeline is designed for easy transition to production databases:

### Database Integration

The canonical JSON schema maps directly to a star schema:

| JSON Section | Database Table | Notes |
|--------------|----------------|-------|
| `scenario` | `dim_scenarios` | Dimension table for simulation metadata |
| `annual` | `fact_annual_energy` | Annual aggregates fact table |
| `monthly_breakdown` | `fact_monthly_energy` | Monthly time-series fact table |
| `kpis` | `fact_kpis` | Computed KPI metrics |

### MySQL/PostgreSQL Example

```sql
-- Dimension table for scenarios
CREATE TABLE dim_scenarios (
    scenario_id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    building_type VARCHAR(100),
    location VARCHAR(255),
    floor_area_m2 NUMERIC,
    simulation_start DATE,
    simulation_end DATE
);

-- Fact table for monthly energy
CREATE TABLE fact_monthly_energy (
    id SERIAL PRIMARY KEY,
    scenario_id INT REFERENCES dim_scenarios(scenario_id),
    month_num INT,
    month_name VARCHAR(3),
    heating_kwh NUMERIC,
    cooling_kwh NUMERIC,
    lighting_kwh NUMERIC,
    equipment_kwh NUMERIC,
    total_kwh NUMERIC
);

-- Fact table for KPIs
CREATE TABLE fact_kpis (
    id SERIAL PRIMARY KEY,
    scenario_id INT REFERENCES dim_scenarios(scenario_id),
    energy_intensity_kwh_m2 NUMERIC,
    annual_co2_tons NUMERIC,
    peak_demand_kw NUMERIC,
    comfort_hours_percent NUMERIC,
    calculated_at TIMESTAMPTZ DEFAULT NOW()
);
```

### Migration Strategy

1. **Development**: Use JSON exports + `make export-json`
2. **Staging**: Add database loader using same schema structure
3. **Production**: Integrate with DuckDB/Parquet for analytics, write summaries to relational DB

The star schema design in this pipeline (dim_building, fact_meters, etc.) already mirrors production patterns.

## File Formats

### Input: Simulation ZIP Files

Each ZIP contains:
- `metadata.json`: Building and scenario information
- `zones.csv`: Zone-level conditions (hourly)
- `hvac.csv`: HVAC system data (hourly)
- `meters.csv`: Building energy meters (hourly)
- `weather.csv`: Weather conditions (hourly)

### Output: Star Schema

- **Parquet files**: One file per table in `data/processed/parquet/`
- **DuckDB database**: Single file `simulations.duckdb` with all tables and views

## Analytics Examples

Common analyses supported by this pipeline:

1. **Zone Temperature Drift**: Identify hours where zones deviate from setpoint
2. **HVAC Energy Performance**: Analyze COP proxy vs outdoor conditions
3. **Scenario Comparison**: Compare energy use between baseline and retrofit
4. **Occupancy Patterns**: Analyze energy use during occupied vs unoccupied hours
5. **Peak Demand Analysis**: Identify peak electric and thermal loads

## Requirements

- Python 3.12+
- pandas >= 2.0.0
- numpy >= 1.24.0
- duckdb >= 0.9.0
- pyarrow >= 14.0.0

See `requirements.txt` for full list of dependencies.

## License

MIT License - see [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Feel free to fork and submit pull requests.

## Contact

For questions or issues, please open an issue in the repository.

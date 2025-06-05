# IDA ICE Energy Simulation ETL Pipeline - Implementation Summary

## Overview
An ETL pipeline that turns synthetic IDA ICE building simulation outputs into an analytics-ready star schema. It covers data generation, extraction, transformation, validation, and loading to both Parquet and DuckDB, with example views for downstream analysis.

## What It Does
- **Data generation**: `src/generate_synthetic_idaice.py` produces six simulation runs (three buildings, two scenarios) with hourly weather, zone conditions, HVAC readings, and meters.
- **Extraction**: `src/idaice_client.py` and `src/etl/extract/` read ZIP bundles, parse metadata and CSVs, and normalize timestamps.
- **Transformation**: `src/etl/transform/` builds a star schema with five dimensions (building, scenario, zone, AHU, time) and four fact tables (zone conditions, HVAC, meters, weather) plus derived comfort/energy metrics.
- **Loading**: `src/etl/load/` writes nine Parquet tables under `data/processed/parquet/` and a DuckDB database at `data/processed/duckdb/simulations.duckdb`. Views such as `vw_zone_with_weather`, `vw_hvac_with_meters`, and `vw_energy_summary` combine facts for analysis.
- **Validation**: `src/etl/validate/` checks schema completeness, value ranges (temperatures, CO2, humidity, energy), hourly coverage, and basic energy plausibility. Current validation passes on the generated datasets.
- **Runner**: `run_pipeline.py` orchestrates extract → transform → validate → load with optional flags like `--skip-validation` and custom input/output paths.

## How to Run
```bash
# Generate synthetic simulations
python src/generate_synthetic_idaice.py

# Run full pipeline with validation
python run_pipeline.py

# Faster run without validation
python run_pipeline.py --skip-validation

# Custom locations
python run_pipeline.py --simulations data/raw/simulations \
                       --parquet data/output/parquet \
                       --duckdb data/output/db/simulations.duckdb
```

## Example Queries
- Energy comparison per building and scenario:
  ```sql
  SELECT building_id, scenario_id,
         total_electric_kwh, total_heating_kwh, total_cooling_kwh
  FROM vw_energy_summary
  ORDER BY building_id, scenario_id;
  ```
- Temperature drift coverage:
  ```sql
  SELECT building_id, zone_id, scenario_id,
         COUNT(*) AS total_hours,
         SUM(CASE WHEN ABS(temp_deviation) > 1.0 THEN 1 ELSE 0 END) AS hours_out_of_bounds
  FROM vw_zone_with_weather
  GROUP BY building_id, zone_id, scenario_id;
  ```

## Status
- Validation suite passes on the synthetic datasets.
- Outputs available in both Parquet and DuckDB for downstream tools.
- Documentation and sample queries are included for quick adoption.

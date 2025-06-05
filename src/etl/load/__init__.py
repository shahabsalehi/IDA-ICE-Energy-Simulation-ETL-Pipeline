"""
Data loading module for persisting processed simulation data.

This module handles loading transformed data into various storage formats
including Parquet files and DuckDB databases.
"""

import os
from pathlib import Path
from typing import Dict
import pandas as pd
import duckdb


def load_to_parquet(schema: Dict[str, pd.DataFrame], output_dir: str) -> None:
    """
    Load processed data into Parquet format.

    Args:
        schema: Dictionary containing dimension and fact tables as DataFrames
        output_dir: Path where Parquet files should be saved

    Raises:
        IOError: If the files cannot be written
        ValueError: If the data format is invalid
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    for table_name, df in schema.items():
        parquet_file = output_path / f"{table_name}.parquet"
        df.to_parquet(parquet_file, index=False)
        print(f"Saved {table_name} to {parquet_file} ({len(df)} rows)")


def load_to_duckdb(schema: Dict[str, pd.DataFrame], db_path: str) -> None:
    """
    Load processed data into DuckDB database.
    
    Args:
        schema: Dictionary containing dimension and fact tables as DataFrames
        db_path: Path to DuckDB database file
        
    Raises:
        IOError: If the database cannot be created or written
    """
    # Ensure directory exists
    db_file = Path(db_path)
    db_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Use context manager for proper resource cleanup
    with duckdb.connect(str(db_file)) as con:
        # Load each table
        # Note: Table names come from a trusted dictionary, but this pattern
        # could be vulnerable if extended to accept user input
        for table_name, df in schema.items():
            con.execute(f"DROP TABLE IF EXISTS {table_name}")
            con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
            print(f"Loaded {table_name} to DuckDB ({len(df)} rows)")
        
        # Create views for common queries
        create_views(con)
        
        print(f"\nDatabase saved to {db_path}")


def create_views(con: duckdb.DuckDBPyConnection) -> None:
    """
    Create analytical views in DuckDB database.
    
    Args:
        con: DuckDB connection
    """
    # View: zones with weather data
    con.execute("""
        CREATE OR REPLACE VIEW vw_zone_with_weather AS
        SELECT 
            t.timestamp,
            t.year,
            t.month,
            t.day,
            t.hour,
            t.is_weekend,
            b.building_id,
            b.building_name,
            b.location,
            z.zone_id,
            z.zone_name,
            zc.scenario_id,
            zc.air_temp_C,
            zc.setpoint_C,
            (zc.air_temp_C - zc.setpoint_C) AS temp_deviation,
            zc.co2_ppm,
            zc.rh_pct,
            w.drybulb_C,
            w.relhum_pct,
            w.ghi_W_m2
        FROM fact_zone_conditions zc
        JOIN dim_time t ON zc.time_key = t.time_key
        JOIN dim_zone z ON zc.zone_key = z.zone_key
        JOIN dim_building b ON z.building_id = b.building_id
        JOIN fact_weather w ON t.time_key = w.time_key AND z.building_id = w.building_id
    """)
    print("Created view: vw_zone_with_weather")
    
    # View: HVAC with meters and weather
    con.execute("""
        CREATE OR REPLACE VIEW vw_hvac_with_meters AS
        SELECT 
            t.timestamp,
            t.year,
            t.month,
            t.day,
            t.hour,
            t.is_weekend,
            b.building_id,
            b.building_name,
            a.ahu_id,
            h.scenario_id,
            h.supply_temp_C,
            h.return_temp_C,
            h.power_kw,
            h.cooling_kw,
            h.heating_kw,
            h.cop_proxy,
            m.electric_kwh,
            m.heating_kwh AS meter_heating_kwh,
            m.cooling_kwh AS meter_cooling_kwh,
            w.drybulb_C AS outdoor_temp_C,
            w.relhum_pct AS outdoor_rh_pct
        FROM fact_hvac h
        JOIN dim_time t ON h.time_key = t.time_key
        JOIN dim_ahu a ON h.ahu_key = a.ahu_key
        JOIN dim_building b ON a.building_id = b.building_id
        JOIN fact_meters m ON t.time_key = m.time_key 
            AND a.building_id = m.building_id 
            AND h.scenario_id = m.scenario_id
        JOIN fact_weather w ON t.time_key = w.time_key AND a.building_id = w.building_id
    """)
    print("Created view: vw_hvac_with_meters")
    
    # View: Building energy summary by scenario
    con.execute("""
        CREATE OR REPLACE VIEW vw_energy_summary AS
        SELECT 
            b.building_id,
            b.building_name,
            s.scenario_id,
            s.description AS scenario_description,
            COUNT(DISTINCT t.time_key) AS num_hours,
            SUM(m.electric_kwh) AS total_electric_kwh,
            SUM(m.heating_kwh) AS total_heating_kwh,
            SUM(m.cooling_kwh) AS total_cooling_kwh,
            AVG(m.electric_kwh) AS avg_electric_kw,
            AVG(m.heating_kwh) AS avg_heating_kw,
            AVG(m.cooling_kwh) AS avg_cooling_kw
        FROM fact_meters m
        JOIN dim_building b ON m.building_id = b.building_id
        JOIN dim_scenario s ON m.scenario_id = s.scenario_id
        JOIN dim_time t ON m.time_key = t.time_key
        GROUP BY b.building_id, b.building_name, s.scenario_id, s.description
        ORDER BY b.building_id, s.scenario_id
    """)
    print("Created view: vw_energy_summary")


def query_duckdb(db_path: str, query: str) -> pd.DataFrame:
    """
    Execute a query against DuckDB and return results as DataFrame.
    
    Note: This function does not sanitize the SQL query parameter. While read-only
    mode provides some protection, consider adding query validation for production use:
    - Reject queries with multiple statements (checking for ';')
    - Limit query complexity or execution time
    - Log queries for audit purposes
    
    Args:
        db_path: Path to DuckDB database file
        query: SQL query to execute
        
    Returns:
        DataFrame containing query results
    """
    with duckdb.connect(str(db_path), read_only=True) as con:
        return con.execute(query).df()


def load_parquet_to_duckdb(parquet_dir: str, db_path: str) -> None:
    """
    Load Parquet files into DuckDB database.
    
    Args:
        parquet_dir: Directory containing Parquet files
        db_path: Path to DuckDB database file
    """
    import re
    
    parquet_path = Path(parquet_dir)
    db_file = Path(db_path)
    db_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Use context manager for proper resource cleanup
    with duckdb.connect(str(db_file)) as con:
        # Load each Parquet file as a table
        for parquet_file in parquet_path.glob("*.parquet"):
            table_name = parquet_file.stem
            
            # Validate table name to contain only safe characters
            if not re.match(r'^[a-zA-Z0-9_]+$', table_name):
                raise ValueError(f"Invalid table name: {table_name}")
            
            con.execute(f"DROP TABLE IF EXISTS {table_name}")
            con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{parquet_file}')")
            row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            print(f"Loaded {table_name} from Parquet ({row_count} rows)")
        
        # Create views
        create_views(con)
        
        print(f"\nDatabase saved to {db_path}")

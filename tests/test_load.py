"""
Tests for the ETL load module.
"""

import os
import tempfile
import unittest
import shutil
import pandas as pd

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from etl.load import load_to_parquet, load_to_duckdb, query_duckdb


class TestLoad(unittest.TestCase):
    """Test cases for data loading functionality."""

    def setUp(self):
        """Create temporary directory and sample schema for testing."""
        self.temp_dir = tempfile.mkdtemp()
        # Complete schema with all required tables for view creation
        self.sample_schema = {
            "dim_building": pd.DataFrame({
                "building_id": ["BLDG_01", "BLDG_02"],
                "building_name": ["Building 01", "Building 02"],
                "location": ["Tallinn, Estonia", "Tartu, Estonia"],
                "floor_area_m2": [4000, 3500]
            }),
            "dim_scenario": pd.DataFrame({
                "scenario_id": ["BASE", "RETROFIT"],
                "description": ["Base scenario", "Retrofit scenario"]
            }),
            "dim_time": pd.DataFrame({
                "time_key": [1, 2],
                "timestamp": pd.to_datetime(["2024-01-01T00:00:00Z", "2024-01-01T01:00:00Z"]),
                "year": [2024, 2024],
                "month": [1, 1],
                "day": [1, 1],
                "hour": [0, 1],
                "dow": [0, 0],
                "is_weekend": [False, False]
            }),
            "dim_zone": pd.DataFrame({
                "zone_key": [1, 2],
                "zone_id": ["Z01", "Z02"],
                "zone_name": ["Zone 01", "Zone 02"],
                "building_id": ["BLDG_01", "BLDG_01"],
                "floor_area_m2": [500, 600]
            }),
            "dim_ahu": pd.DataFrame({
                "ahu_key": [1, 2],
                "ahu_id": ["AHU_01", "AHU_02"],
                "building_id": ["BLDG_01", "BLDG_01"],
                "ahu_name": ["AHU 01", "AHU 02"]
            }),
            "fact_zone_conditions": pd.DataFrame({
                "time_key": [1, 2],
                "zone_key": [1, 2],
                "scenario_id": ["BASE", "BASE"],
                "air_temp_C": [21.5, 22.0],
                "setpoint_C": [22.0, 22.0],
                "co2_ppm": [450, 480],
                "rh_pct": [45, 48]
            }),
            "fact_weather": pd.DataFrame({
                "time_key": [1, 2],
                "building_id": ["BLDG_01", "BLDG_01"],
                "drybulb_C": [5.0, 6.0],
                "relhum_pct": [60, 65],
                "ghi_W_m2": [0, 50]
            }),
            "fact_hvac": pd.DataFrame({
                "time_key": [1, 2],
                "ahu_key": [1, 2],
                "scenario_id": ["BASE", "BASE"],
                "supply_temp_C": [18.0, 18.5],
                "return_temp_C": [22.0, 22.5],
                "power_kw": [15.0, 16.0],
                "cooling_kw": [10.0, 11.0],
                "heating_kw": [0.0, 0.0],
                "cop_proxy": [3.5, 3.4]
            }),
            "fact_meters": pd.DataFrame({
                "time_key": [1, 2],
                "building_id": ["BLDG_01", "BLDG_01"],
                "scenario_id": ["BASE", "BASE"],
                "electric_kwh": [100.0, 105.0],
                "heating_kwh": [50.0, 52.0],
                "cooling_kwh": [30.0, 32.0]
            })
        }

    def tearDown(self):
        """Clean up temporary files."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_load_to_parquet(self):
        """Test loading data to Parquet format."""
        output_dir = os.path.join(self.temp_dir, "parquet")
        load_to_parquet(self.sample_schema, output_dir)
        
        # Verify files were created
        for table_name in self.sample_schema.keys():
            parquet_file = os.path.join(output_dir, f"{table_name}.parquet")
            self.assertTrue(os.path.exists(parquet_file), f"Missing {parquet_file}")
            
            # Verify content
            df = pd.read_parquet(parquet_file)
            self.assertEqual(len(df), len(self.sample_schema[table_name]))

    def test_load_to_duckdb(self):
        """Test loading data to DuckDB."""
        db_path = os.path.join(self.temp_dir, "test.duckdb")
        load_to_duckdb(self.sample_schema, db_path)
        
        self.assertTrue(os.path.exists(db_path))
        
        # Verify tables exist by querying
        result = query_duckdb(db_path, "SELECT * FROM dim_building")
        self.assertEqual(len(result), 2)

    def test_query_duckdb(self):
        """Test querying DuckDB database."""
        db_path = os.path.join(self.temp_dir, "test.duckdb")
        load_to_duckdb(self.sample_schema, db_path)
        
        # Test simple query
        result = query_duckdb(db_path, "SELECT building_id FROM dim_building ORDER BY building_id")
        self.assertEqual(list(result["building_id"]), ["BLDG_01", "BLDG_02"])
        
        # Test aggregation query
        result = query_duckdb(db_path, "SELECT COUNT(*) as cnt FROM dim_scenario")
        self.assertEqual(result.iloc[0]["cnt"], 2)

    def test_bulk_load(self):
        """Test bulk loading functionality with larger dataset."""
        # Use the complete sample_schema but with more rows in dim_time
        large_schema = self.sample_schema.copy()
        large_schema["dim_time"] = pd.DataFrame({
            "time_key": range(1000),
            "timestamp": pd.date_range("2024-01-01", periods=1000, freq="h"),
            "year": [2024] * 1000,
            "month": [1] * 1000,
            "day": list(range(1, 42)) * 24 + [42] * 16,  # Simplified
            "hour": list(range(24)) * 41 + list(range(16)),
            "dow": [0] * 1000,
            "is_weekend": [False] * 1000
        })
        
        db_path = os.path.join(self.temp_dir, "bulk_test.duckdb")
        load_to_duckdb(large_schema, db_path)
        
        result = query_duckdb(db_path, "SELECT COUNT(*) as cnt FROM dim_time")
        self.assertEqual(result.iloc[0]["cnt"], 1000)


if __name__ == "__main__":
    unittest.main()

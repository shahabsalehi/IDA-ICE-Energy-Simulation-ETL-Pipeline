"""
Tests for the ETL validate module.
"""

import os
import unittest
import pandas as pd
import numpy as np

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from etl.validate import (
    validate_schema,
    validate_value_ranges,
    validate_temporal_coverage,
    ValidationError,
)


class TestValidate(unittest.TestCase):
    """Test cases for data validation functionality."""

    def setUp(self):
        """Create sample schema for testing."""
        self.valid_schema = {
            "dim_building": pd.DataFrame({
                "building_id": ["BLDG_01"],
                "building_name": ["Building 01"],
                "location": ["Tallinn, Estonia"],
                "floor_area_m2": [4000]
            }),
            "dim_time": pd.DataFrame({
                "time_key": [1, 2, 3],
                "timestamp": pd.to_datetime([
                    "2024-01-01T00:00:00Z",
                    "2024-01-01T01:00:00Z", 
                    "2024-01-01T02:00:00Z"
                ]),
                "year": [2024, 2024, 2024],
                "month": [1, 1, 1],
                "day": [1, 1, 1],
                "hour": [0, 1, 2],
                "dow": [0, 0, 0],
                "is_weekend": [False, False, False]
            }),
            "fact_zone_conditions": pd.DataFrame({
                "time_key": [1, 2, 3],
                "zone_key": [1, 1, 1],
                "scenario_id": ["BASE", "BASE", "BASE"],
                "air_temp_C": [21.5, 21.8, 22.0],
                "setpoint_C": [21.0, 21.0, 21.0],
                "co2_ppm": [450, 480, 500],
                "rh_pct": [45, 46, 47]
            }),
            "fact_weather": pd.DataFrame({
                "time_key": [1, 2, 3],
                "building_id": ["BLDG_01", "BLDG_01", "BLDG_01"],
                "drybulb_C": [5.0, 5.5, 6.0],
                "relhum_pct": [70, 72, 74],
                "ghi_W_m2": [0, 50, 100]
            }),
            "fact_hvac": pd.DataFrame({
                "time_key": [1, 2, 3],
                "ahu_key": [1, 1, 1],
                "scenario_id": ["BASE", "BASE", "BASE"],
                "power_kw": [5.0, 5.5, 6.0],
                "heating_kw": [3.0, 3.3, 3.6],
                "cooling_kw": [2.0, 2.2, 2.4],
                "cop_proxy": [None, 3.5, 3.6]  # cop_proxy can be NULL
            }),
            "fact_meters": pd.DataFrame({
                "time_key": [1, 2, 3],
                "building_id": ["BLDG_01", "BLDG_01", "BLDG_01"],
                "scenario_id": ["BASE", "BASE", "BASE"],
                "electric_kwh": [100, 105, 110],
                "heating_kwh": [50, 55, 60],
                "cooling_kwh": [30, 35, 40]
            })
        }

    def test_validate_schema_valid(self):
        """Test schema validation with valid data."""
        expected_schema = {
            "dim_building": ["building_id", "building_name", "location", "floor_area_m2"],
            "dim_time": ["time_key", "timestamp", "year", "month", "day", "hour"]
        }
        
        is_valid, errors = validate_schema(self.valid_schema, expected_schema)
        self.assertTrue(is_valid, f"Validation failed: {errors}")

    def test_validate_schema_missing_table(self):
        """Test schema validation with missing table."""
        expected_schema = {
            "missing_table": ["col1", "col2"]
        }
        
        is_valid, errors = validate_schema(self.valid_schema, expected_schema)
        self.assertFalse(is_valid)
        self.assertTrue(any("Missing table" in e for e in errors))

    def test_validate_schema_missing_column(self):
        """Test schema validation with missing column."""
        expected_schema = {
            "dim_building": ["building_id", "nonexistent_column"]
        }
        
        is_valid, errors = validate_schema(self.valid_schema, expected_schema)
        self.assertFalse(is_valid)
        self.assertTrue(any("missing columns" in e for e in errors))

    def test_validate_data_quality(self):
        """Test data quality validation."""
        is_valid, errors = validate_value_ranges(self.valid_schema)
        self.assertTrue(is_valid, f"Validation failed: {errors}")

    def test_validate_value_ranges_invalid_temp(self):
        """Test value range validation with invalid temperatures."""
        invalid_schema = self.valid_schema.copy()
        invalid_schema["fact_zone_conditions"] = pd.DataFrame({
            "time_key": [1],
            "zone_key": [1],
            "scenario_id": ["BASE"],
            "air_temp_C": [50.0],  # Invalid: too high
            "setpoint_C": [21.0],
            "co2_ppm": [450],
            "rh_pct": [45]
        })
        
        is_valid, errors = validate_value_ranges(invalid_schema)
        self.assertFalse(is_valid)
        self.assertTrue(any("temperature" in e.lower() for e in errors))

    def test_validate_completeness(self):
        """Test completeness validation."""
        is_valid, errors = validate_temporal_coverage(self.valid_schema)
        self.assertTrue(is_valid, f"Validation failed: {errors}")

    def test_validate_temporal_coverage_gaps(self):
        """Test temporal validation with gaps."""
        invalid_schema = self.valid_schema.copy()
        invalid_schema["dim_time"] = pd.DataFrame({
            "time_key": [1, 2],
            "timestamp": pd.to_datetime([
                "2024-01-01T00:00:00Z",
                "2024-01-01T05:00:00Z"  # Gap: missing hours 1-4
            ]),
            "year": [2024, 2024],
            "month": [1, 1],
            "day": [1, 1],
            "hour": [0, 5],
            "dow": [0, 0],
            "is_weekend": [False, False]
        })
        
        is_valid, errors = validate_temporal_coverage(invalid_schema)
        self.assertFalse(is_valid)
        self.assertTrue(any("gap" in e.lower() for e in errors))

    def test_validate_ranges(self):
        """Test range validation for different data types."""
        # Test with negative power values (invalid)
        invalid_schema = self.valid_schema.copy()
        invalid_schema["fact_hvac"] = pd.DataFrame({
            "time_key": [1],
            "ahu_key": [1],
            "scenario_id": ["BASE"],
            "power_kw": [-5.0],  # Invalid: negative
            "heating_kw": [3.0],
            "cooling_kw": [2.0],
            "cop_proxy": [3.5]
        })
        
        is_valid, errors = validate_value_ranges(invalid_schema)
        self.assertFalse(is_valid)
        self.assertTrue(any("negative" in e.lower() for e in errors))


if __name__ == "__main__":
    unittest.main()

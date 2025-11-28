"""
Tests for the ETL transform module.
"""

import os
import unittest
import pandas as pd
import numpy as np

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from etl.transform import (
    create_dim_building,
    create_dim_scenario,
    create_dim_zone,
    create_dim_ahu,
    create_dim_time,
)


class TestTransform(unittest.TestCase):
    """Test cases for data transformation functionality."""

    def setUp(self):
        """Create sample extracted run data for testing."""
        self.sample_runs = [
            {
                "run_id": "run_BLDG_01_BASE",
                "metadata": {
                    "building_id": "BLDG_01",
                    "scenario_id": "BASE",
                    "building_name": "Building 01",
                    "location": "Tallinn, Estonia",
                    "floor_area_m2": 4000,
                    "description": "Base scenario for BLDG_01"
                },
                "zones": pd.DataFrame({
                    "timestamp": pd.to_datetime(["2024-01-01T00:00:00Z", "2024-01-01T01:00:00Z"]),
                    "zone_id": ["Z1", "Z1"],
                    "zone_name": ["Zone 1", "Zone 1"],
                    "air_temp_C": [21.5, 21.8],
                    "setpoint_C": [21.0, 21.0],
                    "co2_ppm": [450, 480],
                    "rh_pct": [45, 46]
                }),
                "hvac": pd.DataFrame({
                    "timestamp": pd.to_datetime(["2024-01-01T00:00:00Z", "2024-01-01T01:00:00Z"]),
                    "ahu_id": ["AHU1", "AHU1"],
                    "supply_temp_C": [18.0, 18.5],
                    "return_temp_C": [22.0, 22.5],
                    "power_kw": [5.0, 5.5],
                    "cooling_kw": [2.0, 2.2],
                    "heating_kw": [3.0, 3.3]
                }),
                "meters": pd.DataFrame({
                    "timestamp": pd.to_datetime(["2024-01-01T00:00:00Z", "2024-01-01T01:00:00Z"]),
                    "electric_kwh": [100, 105],
                    "heating_kwh": [50, 55],
                    "cooling_kwh": [30, 35]
                }),
                "weather": pd.DataFrame({
                    "timestamp": pd.to_datetime(["2024-01-01T00:00:00Z", "2024-01-01T01:00:00Z"]),
                    "drybulb_C": [5.0, 5.5],
                    "relhum_pct": [70, 72],
                    "ghi_W_m2": [0, 50]
                })
            }
        ]

    def test_create_dim_building(self):
        """Test building dimension table creation."""
        dim_building = create_dim_building(self.sample_runs)
        
        self.assertIsInstance(dim_building, pd.DataFrame)
        self.assertEqual(len(dim_building), 1)
        self.assertIn("building_id", dim_building.columns)
        self.assertIn("building_name", dim_building.columns)
        self.assertIn("location", dim_building.columns)
        self.assertIn("floor_area_m2", dim_building.columns)
        self.assertEqual(dim_building.iloc[0]["building_id"], "BLDG_01")

    def test_create_dim_building_empty(self):
        """Test building dimension with empty input."""
        dim_building = create_dim_building([])
        self.assertEqual(len(dim_building), 0)
        self.assertIn("building_id", dim_building.columns)

    def test_create_dim_scenario(self):
        """Test scenario dimension table creation."""
        dim_scenario = create_dim_scenario(self.sample_runs)
        
        self.assertIsInstance(dim_scenario, pd.DataFrame)
        self.assertEqual(len(dim_scenario), 1)
        self.assertIn("scenario_id", dim_scenario.columns)
        self.assertEqual(dim_scenario.iloc[0]["scenario_id"], "BASE")

    def test_create_dim_zone(self):
        """Test zone dimension table creation."""
        dim_zone = create_dim_zone(self.sample_runs)
        
        self.assertIsInstance(dim_zone, pd.DataFrame)
        self.assertIn("zone_key", dim_zone.columns)
        self.assertIn("building_id", dim_zone.columns)
        self.assertIn("zone_id", dim_zone.columns)
        self.assertIn("zone_name", dim_zone.columns)

    def test_create_dim_ahu(self):
        """Test AHU dimension table creation."""
        dim_ahu = create_dim_ahu(self.sample_runs)
        
        self.assertIsInstance(dim_ahu, pd.DataFrame)
        self.assertIn("ahu_key", dim_ahu.columns)
        self.assertIn("building_id", dim_ahu.columns)
        self.assertIn("ahu_id", dim_ahu.columns)

    def test_create_dim_time(self):
        """Test time dimension table creation."""
        dim_time = create_dim_time(self.sample_runs)
        
        self.assertIsInstance(dim_time, pd.DataFrame)
        self.assertIn("time_key", dim_time.columns)
        self.assertIn("timestamp", dim_time.columns)
        self.assertIn("year", dim_time.columns)
        self.assertIn("month", dim_time.columns)
        self.assertIn("day", dim_time.columns)
        self.assertIn("hour", dim_time.columns)
        self.assertIn("dow", dim_time.columns)
        self.assertIn("is_weekend", dim_time.columns)

    def test_transform_to_timeseries(self):
        """Test time-series transformation produces consistent timestamps."""
        dim_time = create_dim_time(self.sample_runs)
        
        # Verify timestamps are sorted
        timestamps = pd.to_datetime(dim_time["timestamp"])
        self.assertTrue(timestamps.is_monotonic_increasing)

    def test_aggregate_metrics(self):
        """Test that dimension tables don't have duplicates."""
        # Add another run for the same building but different scenario
        self.sample_runs.append({
            "run_id": "run_BLDG_01_RETROFIT",
            "metadata": {
                "building_id": "BLDG_01",
                "scenario_id": "RETROFIT",
                "building_name": "Building 01",
                "location": "Tallinn, Estonia",
                "floor_area_m2": 4000,
                "description": "Retrofit scenario for BLDG_01"
            },
            "zones": self.sample_runs[0]["zones"].copy(),
            "hvac": self.sample_runs[0]["hvac"].copy(),
            "meters": self.sample_runs[0]["meters"].copy(),
            "weather": self.sample_runs[0]["weather"].copy()
        })
        
        dim_building = create_dim_building(self.sample_runs)
        dim_scenario = create_dim_scenario(self.sample_runs)
        
        # Should have 1 building but 2 scenarios
        self.assertEqual(len(dim_building), 1)
        self.assertEqual(len(dim_scenario), 2)


if __name__ == "__main__":
    unittest.main()

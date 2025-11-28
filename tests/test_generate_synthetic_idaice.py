"""
Tests for the synthetic data generator module.
"""

import os
import tempfile
import shutil
import unittest
import zipfile
import json
import pandas as pd

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from generate_synthetic_idaice import (
    generate_time_index,
    generate_weather,
    generate_zones,
    generate_hvac,
    generate_meters,
    write_run_zip,
)


class TestGenerateSyntheticData(unittest.TestCase):
    """Test cases for synthetic data generation functionality."""

    def setUp(self):
        """Create temporary directory for test outputs."""
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Clean up temporary files."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_generate_time_index(self):
        """Test time index generation."""
        time_index = generate_time_index("2024-01-01", periods=24, freq="h")
        
        self.assertEqual(len(time_index), 24)
        self.assertEqual(time_index[0].hour, 0)
        self.assertEqual(time_index[-1].hour, 23)

    def test_generate_time_index_week(self):
        """Test time index generation for a week."""
        time_index = generate_time_index("2024-01-01", periods=24*7, freq="h")
        
        self.assertEqual(len(time_index), 168)  # 7 days * 24 hours

    def test_generate_weather(self):
        """Test weather data generation."""
        time_index = generate_time_index("2024-01-01", periods=24)
        weather = generate_weather(time_index)
        
        self.assertIsInstance(weather, pd.DataFrame)
        self.assertEqual(len(weather), 24)
        self.assertIn("timestamp", weather.columns)
        self.assertIn("drybulb_C", weather.columns)
        self.assertIn("relhum_pct", weather.columns)
        self.assertIn("ghi_W_m2", weather.columns)
        
        # Check value ranges
        self.assertTrue(all(weather["relhum_pct"] >= 30))
        self.assertTrue(all(weather["relhum_pct"] <= 100))
        self.assertTrue(all(weather["ghi_W_m2"] >= 0))

    def test_generate_zones(self):
        """Test zone data generation."""
        time_index = generate_time_index("2024-01-01", periods=24)
        zones = generate_zones(time_index, "BLDG_01", "BASE", n_zones=3)
        
        self.assertIsInstance(zones, pd.DataFrame)
        self.assertEqual(len(zones), 24 * 3)  # 24 hours * 3 zones
        self.assertIn("zone_id", zones.columns)
        self.assertIn("air_temp_C", zones.columns)
        self.assertIn("co2_ppm", zones.columns)
        
        # Check unique zones
        unique_zones = zones["zone_id"].unique()
        self.assertEqual(len(unique_zones), 3)

    def test_generate_hvac(self):
        """Test HVAC data generation."""
        time_index = generate_time_index("2024-01-01", periods=24)
        hvac = generate_hvac(time_index, "BLDG_01", "BASE", n_ahu=2)
        
        self.assertIsInstance(hvac, pd.DataFrame)
        self.assertEqual(len(hvac), 24 * 2)  # 24 hours * 2 AHUs
        self.assertIn("ahu_id", hvac.columns)
        self.assertIn("power_kw", hvac.columns)
        self.assertIn("heating_kw", hvac.columns)
        self.assertIn("cooling_kw", hvac.columns)

    def test_generate_meters(self):
        """Test meter data generation."""
        time_index = generate_time_index("2024-01-01", periods=24)
        meters = generate_meters(time_index, "BLDG_01", "BASE")
        
        self.assertIsInstance(meters, pd.DataFrame)
        self.assertEqual(len(meters), 24)
        self.assertIn("electric_kwh", meters.columns)
        self.assertIn("heating_kwh", meters.columns)
        self.assertIn("cooling_kwh", meters.columns)
        
        # Energy should be non-negative
        self.assertTrue(all(meters["electric_kwh"] > 0))

    def test_write_run_zip(self):
        """Test writing a complete simulation run to ZIP."""
        write_run_zip(
            out_dir=self.temp_dir,
            building_id="BLDG_TEST",
            scenario_id="TEST",
            start="2024-01-01",
            days=1,
            quiet=True
        )
        
        zip_path = os.path.join(self.temp_dir, "run_BLDG_TEST_TEST.zip")
        self.assertTrue(os.path.exists(zip_path))
        
        # Verify ZIP contents
        with zipfile.ZipFile(zip_path, 'r') as zf:
            names = zf.namelist()
            self.assertTrue(any("metadata.json" in n for n in names))
            self.assertTrue(any("zones.csv" in n for n in names))
            self.assertTrue(any("hvac.csv" in n for n in names))
            self.assertTrue(any("meters.csv" in n for n in names))
            self.assertTrue(any("weather.csv" in n for n in names))

    def test_add_noise_to_data(self):
        """Test that generated data includes realistic noise/variation."""
        time_index = generate_time_index("2024-01-01", periods=48)
        weather = generate_weather(time_index)
        
        # Temperature should have variation (not constant)
        temp_std = weather["drybulb_C"].std()
        self.assertGreater(temp_std, 0.5, "Temperature should have variation")

    def test_save_synthetic_data(self):
        """Test that saved data can be read back correctly."""
        write_run_zip(
            out_dir=self.temp_dir,
            building_id="BLDG_SAVE",
            scenario_id="TEST",
            start="2024-01-01",
            days=1,
            quiet=True
        )
        
        # Extract and read metadata
        zip_path = os.path.join(self.temp_dir, "run_BLDG_SAVE_TEST.zip")
        with zipfile.ZipFile(zip_path, 'r') as zf:
            with zf.open("run_BLDG_SAVE_TEST/metadata.json") as f:
                metadata = json.load(f)
        
        self.assertEqual(metadata["building_id"], "BLDG_SAVE")
        self.assertEqual(metadata["scenario_id"], "TEST")
        self.assertIn("location", metadata)
        self.assertIn("floor_area_m2", metadata)


if __name__ == "__main__":
    unittest.main()

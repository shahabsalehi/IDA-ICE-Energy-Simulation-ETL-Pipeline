"""
Tests for the ETL extract module.
"""

import os
import json
import zipfile
import tempfile
import unittest
import pandas as pd

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from etl.extract import extract_simulation_data, extract_runs, extract_run_by_id


class TestExtract(unittest.TestCase):
    """Test cases for data extraction functionality."""

    def setUp(self):
        """Create temporary directory and test ZIP file."""
        self.temp_dir = tempfile.mkdtemp()
        self._create_test_zip()

    def tearDown(self):
        """Clean up temporary files."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _create_test_zip(self):
        """Create a valid test ZIP file for testing."""
        run_id = "run_BLDG_TEST_BASE"
        run_dir = os.path.join(self.temp_dir, run_id)
        os.makedirs(run_dir)

        # Create metadata.json
        metadata = {
            "building_id": "BLDG_TEST",
            "scenario_id": "BASE",
            "building_name": "Test Building",
            "location": "Tallinn, Estonia",
            "floor_area_m2": 1000,
            "description": "Test simulation run",
            "generated_at": "2024-01-01T00:00:00Z"
        }
        with open(os.path.join(run_dir, "metadata.json"), "w") as f:
            json.dump(metadata, f)

        # Create zones.csv
        zones_df = pd.DataFrame({
            "timestamp": ["2024-01-01T00:00:00Z", "2024-01-01T01:00:00Z"],
            "building_id": ["BLDG_TEST", "BLDG_TEST"],
            "scenario_id": ["BASE", "BASE"],
            "zone_id": ["Z1", "Z1"],
            "zone_name": ["Zone 1", "Zone 1"],
            "air_temp_C": [21.5, 21.8],
            "setpoint_C": [21.0, 21.0],
            "co2_ppm": [450, 480],
            "rh_pct": [45, 46]
        })
        zones_df.to_csv(os.path.join(run_dir, "zones.csv"), index=False)

        # Create hvac.csv
        hvac_df = pd.DataFrame({
            "timestamp": ["2024-01-01T00:00:00Z", "2024-01-01T01:00:00Z"],
            "building_id": ["BLDG_TEST", "BLDG_TEST"],
            "scenario_id": ["BASE", "BASE"],
            "ahu_id": ["AHU1", "AHU1"],
            "supply_temp_C": [18.0, 18.5],
            "return_temp_C": [22.0, 22.5],
            "power_kw": [5.0, 5.5],
            "cooling_kw": [2.0, 2.2],
            "heating_kw": [3.0, 3.3]
        })
        hvac_df.to_csv(os.path.join(run_dir, "hvac.csv"), index=False)

        # Create meters.csv
        meters_df = pd.DataFrame({
            "timestamp": ["2024-01-01T00:00:00Z", "2024-01-01T01:00:00Z"],
            "building_id": ["BLDG_TEST", "BLDG_TEST"],
            "scenario_id": ["BASE", "BASE"],
            "electric_kwh": [100, 105],
            "heating_kwh": [50, 55],
            "cooling_kwh": [30, 35]
        })
        meters_df.to_csv(os.path.join(run_dir, "meters.csv"), index=False)

        # Create weather.csv
        weather_df = pd.DataFrame({
            "timestamp": ["2024-01-01T00:00:00Z", "2024-01-01T01:00:00Z"],
            "drybulb_C": [5.0, 5.5],
            "relhum_pct": [70, 72],
            "ghi_W_m2": [0, 50]
        })
        weather_df.to_csv(os.path.join(run_dir, "weather.csv"), index=False)

        # Create ZIP file
        self.test_zip = os.path.join(self.temp_dir, f"{run_id}.zip")
        with zipfile.ZipFile(self.test_zip, "w", zipfile.ZIP_DEFLATED) as zf:
            for fname in ["metadata.json", "zones.csv", "hvac.csv", "meters.csv", "weather.csv"]:
                zf.write(os.path.join(run_dir, fname), os.path.join(run_id, fname))

    def test_extract_simulation_data(self):
        """Test extraction of simulation data from files."""
        result = extract_simulation_data(self.test_zip)
        
        self.assertIn("run_id", result)
        self.assertIn("metadata", result)
        self.assertIn("zones", result)
        self.assertIn("hvac", result)
        self.assertIn("meters", result)
        self.assertIn("weather", result)
        
        self.assertEqual(result["metadata"]["building_id"], "BLDG_TEST")
        self.assertEqual(result["metadata"]["scenario_id"], "BASE")
        self.assertEqual(len(result["zones"]), 2)

    def test_extract_metadata(self):
        """Test extraction of metadata from simulation files."""
        result = extract_simulation_data(self.test_zip)
        metadata = result["metadata"]
        
        self.assertEqual(metadata["building_name"], "Test Building")
        self.assertEqual(metadata["location"], "Tallinn, Estonia")
        self.assertEqual(metadata["floor_area_m2"], 1000)

    def test_extract_nonexistent_file(self):
        """Test that extracting a non-existent file raises an error."""
        with self.assertRaises(FileNotFoundError):
            extract_simulation_data("/nonexistent/path/file.zip")

    def test_extract_invalid_file(self):
        """Test that extracting a non-ZIP file raises an error."""
        invalid_file = os.path.join(self.temp_dir, "not_a_zip.txt")
        with open(invalid_file, "w") as f:
            f.write("not a zip file")
        
        with self.assertRaises(ValueError):
            extract_simulation_data(invalid_file)

    def test_extract_runs(self):
        """Test extraction of multiple runs from a directory."""
        runs = extract_runs(self.temp_dir)
        
        self.assertEqual(len(runs), 1)
        self.assertEqual(runs[0]["metadata"]["building_id"], "BLDG_TEST")

    def test_extract_run_by_id(self):
        """Test extraction of a specific run by building and scenario ID."""
        result = extract_run_by_id(self.temp_dir, "BLDG_TEST", "BASE")
        
        self.assertEqual(result["metadata"]["building_id"], "BLDG_TEST")
        self.assertEqual(result["metadata"]["scenario_id"], "BASE")

    def test_extract_run_by_id_not_found(self):
        """Test that extracting a non-existent run raises an error."""
        with self.assertRaises(FileNotFoundError):
            extract_run_by_id(self.temp_dir, "NONEXISTENT", "SCENARIO")


if __name__ == "__main__":
    unittest.main()

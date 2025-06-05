"""
Data extraction module for IDA ICE simulation results.

This module handles the extraction of raw simulation data from
IDA ICE output files and formats.
"""

import os
import json
import zipfile
import tempfile
from pathlib import Path
from typing import Dict, Any, List
import pandas as pd


def extract_simulation_data(file_path: str) -> Dict[str, Any]:
    """
    Extract simulation data from IDA ICE output files (ZIP format).

    Args:
        file_path: Path to the IDA ICE simulation output ZIP file

    Returns:
        Dictionary containing extracted simulation data with keys:
        - run_id: Unique identifier for the run
        - metadata: Dictionary from metadata.json
        - zones: DataFrame of zone conditions
        - hvac: DataFrame of HVAC system data
        - meters: DataFrame of meter readings
        - weather: DataFrame of weather data

    Raises:
        FileNotFoundError: If the specified file does not exist
        ValueError: If the ZIP structure is invalid
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    
    if not file_path.endswith('.zip'):
        raise ValueError("Expected a ZIP file")
    
    # Use TemporaryDirectory context manager for automatic cleanup
    with tempfile.TemporaryDirectory() as temp_dir:
        # Extract the ZIP file
        with zipfile.ZipFile(file_path, 'r') as zf:
            zf.extractall(temp_dir)
        
        # Determine the run directory (should be single folder inside)
        extracted_items = os.listdir(temp_dir)
        if len(extracted_items) != 1:
            raise ValueError(f"Expected single directory in ZIP, found: {extracted_items}")
        
        run_dir = os.path.join(temp_dir, extracted_items[0])
        if not os.path.isdir(run_dir):
            raise ValueError(f"Expected directory, found file: {extracted_items[0]}")
        
        run_id = extracted_items[0]
        
        # Read metadata
        metadata_path = os.path.join(run_dir, "metadata.json")
        if not os.path.exists(metadata_path):
            raise ValueError(f"metadata.json not found in {run_id}")
        
        with open(metadata_path, 'r') as f:
            metadata = json.load(f)
        
        # Check for required CSV files before reading
        required_files = ["zones.csv", "hvac.csv", "meters.csv", "weather.csv"]
        for csv_file in required_files:
            csv_path = os.path.join(run_dir, csv_file)
            if not os.path.exists(csv_path):
                raise ValueError(f"{csv_file} not found in {run_id}")
        
        # Read CSV files
        zones_df = pd.read_csv(os.path.join(run_dir, "zones.csv"))
        hvac_df = pd.read_csv(os.path.join(run_dir, "hvac.csv"))
        meters_df = pd.read_csv(os.path.join(run_dir, "meters.csv"))
        weather_df = pd.read_csv(os.path.join(run_dir, "weather.csv"))
        
        # Convert timestamps to datetime
        for df in [zones_df, hvac_df, meters_df, weather_df]:
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        return {
            "run_id": run_id,
            "metadata": metadata,
            "zones": zones_df,
            "hvac": hvac_df,
            "meters": meters_df,
            "weather": weather_df,
        }


def extract_runs(simulations_dir: str) -> List[Dict[str, Any]]:
    """
    Extract all simulation runs from a directory.
    
    Args:
        simulations_dir: Path to directory containing simulation ZIP files
        
    Returns:
        List of dictionaries, each containing extracted run data
        
    Raises:
        FileNotFoundError: If the simulations directory does not exist
    """
    if not os.path.exists(simulations_dir):
        raise FileNotFoundError(f"Simulations directory not found: {simulations_dir}")
    
    runs = []
    sim_path = Path(simulations_dir)
    failed_extractions = []
    
    for zip_file in sorted(sim_path.glob("run_*.zip")):
        try:
            run_data = extract_simulation_data(str(zip_file))
            runs.append(run_data)
            print(f"Extracted: {run_data['run_id']}")
        except (FileNotFoundError, zipfile.BadZipFile, ValueError) as e:
            failed_extractions.append((zip_file.name, str(e)))
            print(f"Error extracting {zip_file.name}: {e}")
        except Exception as e:
            # Log unexpected errors but continue
            failed_extractions.append((zip_file.name, f"Unexpected error: {str(e)}"))
            print(f"Unexpected error extracting {zip_file.name}: {e}")
    
    # Report failures if any
    if failed_extractions and len(failed_extractions) > len(runs) * 0.5:
        # If more than 50% of files failed, raise an error
        raise RuntimeError(
            f"Too many extraction failures: {len(failed_extractions)} out of "
            f"{len(runs) + len(failed_extractions)} files failed"
        )
    
    return runs


def extract_run_by_id(simulations_dir: str, building_id: str, scenario_id: str) -> Dict[str, Any]:
    """
    Extract a specific simulation run by building and scenario ID.
    
    Args:
        simulations_dir: Path to directory containing simulation ZIP files
        building_id: Building identifier
        scenario_id: Scenario identifier
        
    Returns:
        Dictionary containing extracted run data
        
    Raises:
        FileNotFoundError: If the run is not found
    """
    zip_filename = f"run_{building_id}_{scenario_id}.zip"
    zip_path = os.path.join(simulations_dir, zip_filename)
    
    if not os.path.exists(zip_path):
        raise FileNotFoundError(f"Run not found: {zip_path}")
    
    return extract_simulation_data(zip_path)

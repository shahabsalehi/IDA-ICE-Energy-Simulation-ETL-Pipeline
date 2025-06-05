"""
IDA ICE Client for interfacing with IDA ICE simulation software.

This module provides a client interface for interacting with IDA ICE
(Indoor Climate and Energy) simulation software, including launching
simulations and retrieving results.
"""

from pathlib import Path
from typing import Dict, Any, List


class IdaIceClient:
    """
    Mock client for IDA ICE Cloud API.
    
    This class simulates downloading simulation runs from the IDA ICE Cloud API.
    In reality, it just locates ZIP files in the local simulations directory.
    """

    def __init__(self, simulations_dir: str):
        """
        Initialize the IDA ICE client.

        Args:
            simulations_dir: Path to directory containing simulation ZIP files
        """
        self.simulations_dir = Path(simulations_dir)
        if not self.simulations_dir.exists():
            raise FileNotFoundError(f"Simulations directory not found: {simulations_dir}")

    def list_runs(self) -> List[Dict[str, str]]:
        """
        Return list of runs with building_id, scenario_id, and path to ZIP.
        
        Returns:
            List of dictionaries with keys: building_id, scenario_id, zip_path
        """
        runs = []
        for zip_path in self.simulations_dir.glob("run_*.zip"):
            # Parse filename: run_{building_id}_{scenario_id}.zip
            filename = zip_path.stem  # Remove .zip extension
            # Remove 'run_' prefix
            if filename.startswith("run_"):
                filename = filename[4:]
            
            # Use rsplit to robustly separate building_id and scenario_id
            # This handles scenario names with underscores (e.g., RETROFIT_V2)
            if "_" in filename:
                building_id, scenario_id = filename.rsplit("_", 1)
                
                runs.append({
                    "building_id": building_id,
                    "scenario_id": scenario_id,
                    "zip_path": str(zip_path)
                })
        return runs

    def download_run(self, building_id: str, scenario_id: str) -> str:
        """
        Return local path to ZIP (no real download; just locate file).
        
        Args:
            building_id: Building identifier
            scenario_id: Scenario identifier
            
        Returns:
            Path to the ZIP file
            
        Raises:
            FileNotFoundError: If the run ZIP file is not found
        """
        zip_filename = f"run_{building_id}_{scenario_id}.zip"
        zip_path = self.simulations_dir / zip_filename
        
        if not zip_path.exists():
            raise FileNotFoundError(
                f"Run not found: {zip_filename} in {self.simulations_dir}"
            )
        
        return str(zip_path)
    
    def get_run_metadata(self, building_id: str, scenario_id: str) -> Dict[str, Any]:
        """
        Get metadata for a specific run without extracting the full ZIP.
        
        Args:
            building_id: Building identifier
            scenario_id: Scenario identifier
            
        Returns:
            Dictionary containing run metadata
            
        Raises:
            FileNotFoundError: If the run is not found
        """
        import zipfile
        import json
        
        zip_path = self.download_run(building_id, scenario_id)
        run_id = f"run_{building_id}_{scenario_id}"
        
        with zipfile.ZipFile(zip_path, 'r') as zf:
            metadata_path = f"{run_id}/metadata.json"
            if metadata_path in zf.namelist():
                with zf.open(metadata_path) as f:
                    return json.load(f)
            else:
                raise FileNotFoundError(f"metadata.json not found in {zip_path}")


# Keep backward compatibility with old class name
class IDAICEClient(IdaIceClient):
    """Alias for backward compatibility."""
    pass

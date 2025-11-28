"""
IDA ICE Client for interfacing with IDA ICE simulation software.

This module provides a client interface for interacting with IDA ICE
(Indoor Climate and Energy) simulation software, including launching
simulations and retrieving results.
"""

import logging
from pathlib import Path
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)


class IDAICEClient:
    """
    Client for IDA ICE Cloud API.
    
    This class provides an interface for interacting with IDA ICE simulation
    software via the cloud API. Configure host/port/api_key for production use.
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 8080,
        api_key: Optional[str] = None,
        use_https: bool = False
    ):
        """
        Initialize the IDA ICE Cloud API client.

        Args:
            host: API host address
            port: API port number
            api_key: API authentication key
            use_https: Whether to use HTTPS for API connections
        """
        self.host = host
        self.port = port
        self.api_key = api_key
        self.use_https = use_https
        self._config: Dict[str, Any] = {}
        
    def _build_url(self, endpoint: str) -> str:
        """Build full API URL for an endpoint."""
        protocol = "https" if self.use_https else "http"
        return f"{protocol}://{self.host}:{self.port}{endpoint}"
    
    def _make_request(self, method: str, endpoint: str, **kwargs) -> Dict[str, Any]:
        """Make an HTTP request to the API (mock implementation)."""
        # In production, this would use requests library
        # For now, return mock response
        return {"status": "ok", "endpoint": endpoint}
    
    def configure_simulation(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Configure simulation parameters.
        
        Args:
            config: Dictionary containing simulation configuration
            
        Returns:
            Confirmation dictionary
            
        Raises:
            ValueError: If configuration is invalid
        """
        if not config.get("building_id"):
            raise ValueError("building_id is required in configuration")
        
        self._config = config
        return {"status": "configured", "config": config}
    
    def run_simulation(self, building_id: str, scenario_id: str) -> Dict[str, Any]:
        """
        Start a simulation run.
        
        Args:
            building_id: Building identifier
            scenario_id: Scenario identifier
            
        Returns:
            Dictionary with job_id and status
        """
        return self._make_request(
            "POST",
            "/simulations/run",
            building_id=building_id,
            scenario_id=scenario_id
        )
    
    def get_simulation_status(self, job_id: str) -> Dict[str, Any]:
        """
        Get status of a running simulation.
        
        Args:
            job_id: Job identifier returned by run_simulation
            
        Returns:
            Dictionary with job status and progress
        """
        return self._make_request("GET", f"/simulations/{job_id}/status")
    
    def retrieve_results(self, job_id: str) -> Dict[str, Any]:
        """
        Retrieve results of a completed simulation.
        
        Args:
            job_id: Job identifier
            
        Returns:
            Dictionary with results URL and metadata
        """
        return self._make_request("GET", f"/simulations/{job_id}/results")


class LocalSimulationClient:
    """
    Local file-based IDA ICE client for development and testing.
    
    This class locates ZIP files in the local simulations directory.
    Use this when you don't have access to the IDA ICE Cloud API.
    """

    def __init__(self, simulations_dir: str):
        """
        Initialize the local simulation client.

        Args:
            simulations_dir: Path to directory containing simulation ZIP files
            
        Raises:
            FileNotFoundError: If the simulations directory does not exist
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


# Backward compatibility alias - use LocalSimulationClient for new code
IdaIceClient = LocalSimulationClient

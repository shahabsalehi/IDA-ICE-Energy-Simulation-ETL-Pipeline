"""
Tests for the IDA ICE client module.

Note: These tests mock the IDA ICE API since it's proprietary software.
In a real deployment, integration tests would require access to IDA ICE.
"""

import os
import unittest
from unittest.mock import Mock, patch, MagicMock

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from idaice_client import IDAICEClient


class TestIDAICEClient(unittest.TestCase):
    """Test cases for IDA ICE client functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.client = IDAICEClient(
            host="localhost",
            port=8080,
            api_key="test_api_key"
        )

    def test_client_initialization(self):
        """Test client initialization with parameters."""
        client = IDAICEClient(
            host="192.168.1.100",
            port=9000,
            api_key="my_api_key"
        )
        
        self.assertEqual(client.host, "192.168.1.100")
        self.assertEqual(client.port, 9000)
        self.assertEqual(client.api_key, "my_api_key")

    def test_client_default_initialization(self):
        """Test client initialization with defaults."""
        client = IDAICEClient()
        
        self.assertEqual(client.host, "localhost")
        self.assertEqual(client.port, 8080)
        self.assertIsNone(client.api_key)

    def test_configure_simulation(self):
        """Test simulation configuration."""
        config = {
            "building_id": "BLDG_01",
            "scenario": "BASE",
            "start_date": "2024-01-01",
            "end_date": "2024-01-07",
            "time_step": "1h"
        }
        
        # Client should accept configuration without error
        result = self.client.configure_simulation(config)
        self.assertIsNotNone(result)

    def test_configure_simulation_invalid(self):
        """Test simulation configuration with invalid parameters."""
        invalid_config = {
            "building_id": None,  # Invalid
        }
        
        with self.assertRaises(ValueError):
            self.client.configure_simulation(invalid_config)

    @patch.object(IDAICEClient, '_make_request')
    def test_run_simulation(self, mock_request):
        """Test running a simulation."""
        mock_request.return_value = {"job_id": "job_123", "status": "submitted"}
        
        result = self.client.run_simulation("BLDG_01", "BASE")
        
        self.assertIn("job_id", result)
        self.assertEqual(result["status"], "submitted")

    @patch.object(IDAICEClient, '_make_request')
    def test_get_simulation_status(self, mock_request):
        """Test getting simulation status."""
        mock_request.return_value = {"job_id": "job_123", "status": "completed", "progress": 100}
        
        result = self.client.get_simulation_status("job_123")
        
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["progress"], 100)

    @patch.object(IDAICEClient, '_make_request')  
    def test_retrieve_results(self, mock_request):
        """Test retrieving simulation results."""
        mock_request.return_value = {
            "job_id": "job_123",
            "results_url": "/results/job_123.zip",
            "metadata": {"building_id": "BLDG_01"}
        }
        
        result = self.client.retrieve_results("job_123")
        
        self.assertIn("results_url", result)
        self.assertIn("metadata", result)

    def test_build_api_url(self):
        """Test API URL construction."""
        url = self.client._build_url("/simulations")
        self.assertEqual(url, "http://localhost:8080/simulations")

    def test_build_api_url_with_https(self):
        """Test API URL construction with HTTPS."""
        client = IDAICEClient(host="localhost", port=443, use_https=True)
        url = client._build_url("/simulations")
        self.assertEqual(url, "https://localhost:443/simulations")


if __name__ == "__main__":
    unittest.main()

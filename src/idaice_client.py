import requests
import json
from typing import Dict, Any

class IDAICEClient:
    def __init__(self, base_url: str, api_key: str = None):
        self.base_url = base_url
        self.api_key = api_key
        self.session = requests.Session()
        if api_key:
            self.session.headers['Authorization'] = f'Bearer {api_key}'

    def get_simulation(self, sim_id: str) -> Dict[str, Any]:
        response = self.session.get(f"{self.base_url}/simulations/{sim_id}")
        response.raise_for_status()
        return response.json()

    def list_simulations(self) -> list:
        response = self.session.get(f"{self.base_url}/simulations")
        response.raise_for_status()
        return response.json()

    def get_results(self, sim_id: str) -> Dict[str, Any]:
        response = self.session.get(f"{self.base_url}/simulations/{sim_id}/results")
        response.raise_for_status()
        return response.json()

#!/usr/bin/env python3
"""
Validate exported JSON against schema for frontend consumption.

Usage:
    python src/validate_json.py [json_path]
    
If no path given, validates artifacts/json/ida_ice_simulation_summary.json (canonical)
"""

import json
import sys
from datetime import datetime
from pathlib import Path


def validate_iso8601(value: str) -> bool:
    """Check if string is valid ISO 8601 timestamp."""
    try:
        if "+" in value or value.endswith("Z"):
            datetime.fromisoformat(value.replace("Z", "+00:00"))
        else:
            datetime.fromisoformat(value)
        return True
    except ValueError:
        return False


def validate_ida_ice_simulation(data: dict) -> list[str]:
    """Validate canonical ida_ice_simulation_summary.json schema."""
    errors = []
    
    # Required top-level fields
    required_top = ["pipeline", "generated_at", "scenario", "annual", "kpis"]
    for field in required_top:
        if field not in data:
            errors.append(f"Missing required field: {field}")
    
    # Validate generated_at timestamp
    if "generated_at" in data:
        if not validate_iso8601(data["generated_at"]):
            errors.append("Field 'generated_at' is not a valid ISO 8601 timestamp")
    
    # Validate scenario object
    if "scenario" in data:
        scenario = data["scenario"]
        scenario_fields = ["name", "building_type", "location", "floor_area_m2"]
        for field in scenario_fields:
            if field not in scenario:
                errors.append(f"Missing scenario.{field}")
    
    # Validate annual object
    if "annual" in data:
        annual = data["annual"]
        annual_fields = ["total_kwh", "heating_kwh", "cooling_kwh"]
        for field in annual_fields:
            if field not in annual:
                errors.append(f"Missing annual.{field}")
            elif not isinstance(annual[field], (int, float)) or annual[field] < 0:
                errors.append(f"annual.{field} must be a non-negative number")
    
    # Validate kpis object
    if "kpis" in data:
        kpis = data["kpis"]
        kpi_fields = ["energy_intensity_kwh_m2", "comfort_hours_percent"]
        for field in kpi_fields:
            if field not in kpis:
                errors.append(f"Missing kpis.{field}")
    
    # Validate monthly_breakdown array if present
    if "monthly_breakdown" in data:
        if not isinstance(data["monthly_breakdown"], list):
            errors.append("monthly_breakdown must be an array")
        elif len(data["monthly_breakdown"]) > 0:
            sample = data["monthly_breakdown"][0]
            if "month" not in sample:
                errors.append("monthly_breakdown items must have 'month' field")
    
    return errors


def main():
    json_path = sys.argv[1] if len(sys.argv) > 1 else "artifacts/json/ida_ice_simulation_summary.json"
    path = Path(json_path)
    
    if not path.exists():
        print(f"✗ File not found: {path}")
        sys.exit(1)
    
    try:
        with open(path) as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        print(f"✗ Invalid JSON: {e}")
        sys.exit(1)
    
    errors = validate_ida_ice_simulation(data)
    
    if errors:
        print(f"✗ Validation failed for {path}:")
        for error in errors:
            print(f"  - {error}")
        sys.exit(1)
    else:
        print(f"✓ Validation passed: {path}")
        print(f"  pipeline: {data.get('pipeline', 'N/A')}")
        print(f"  generated_at: {data.get('generated_at', 'N/A')}")
        if "scenario" in data:
            print(f"  scenario: {data['scenario'].get('name', 'N/A')}")
        if "annual" in data:
            print(f"  total_kwh: {data['annual'].get('total_kwh', 'N/A')}")
        if "monthly_breakdown" in data:
            print(f"  monthly_breakdown: {len(data['monthly_breakdown'])} months")
        sys.exit(0)


if __name__ == "__main__":
    main()

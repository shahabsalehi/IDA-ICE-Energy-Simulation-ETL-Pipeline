"""
Synthetic IDA ICE data generator.

This module provides utilities for generating synthetic IDA ICE simulation
data for testing and development purposes.
"""

import os
import sys
import json
import zipfile
import argparse
from datetime import datetime, timezone
from pathlib import Path
import numpy as np
import pandas as pd


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


# Constants
HOURS_PER_DAY = 24


def generate_time_index(start: str, periods: int = 24 * 7, freq: str = "h") -> pd.DatetimeIndex:
    return pd.date_range(start=start, periods=periods, freq=freq, tz="UTC")


def generate_weather(time_index: pd.DatetimeIndex) -> pd.DataFrame:
    """Simple synthetic weather for Stockholm-like climate."""
    hours = np.arange(len(time_index))
    # Daily sinusoidal temperature around 5°C with some noise
    base_temp = 5 + 7 * np.sin(2 * np.pi * hours / 24)
    noise = np.random.normal(scale=1.0, size=len(time_index))
    drybulb = base_temp + noise

    # Rel. humidity inversely correlated with temp, clipped
    relhum = np.clip(80 - (drybulb - 5) * 2 + np.random.normal(scale=5, size=len(time_index)), 30, 100)

    # Simple daily solar pattern
    ghi = np.clip(400 * np.sin(2 * np.pi * (hours % 24) / 24), 0, None)

    return pd.DataFrame(
        {
            "timestamp": time_index,
            "drybulb_C": drybulb,
            "relhum_pct": relhum,
            "ghi_W_m2": ghi,
        }
    )


def generate_zones(time_index: pd.DatetimeIndex, building_id: str, scenario_id: str, n_zones: int = 5) -> pd.DataFrame:
    records = []
    for z in range(1, n_zones + 1):
        zone_id = f"Z{z}"
        zone_name = f"Zone {z}"
        # Different setpoints per zone
        base_setpoint = 21 + (z - 3) * 0.5
        setpoint = base_setpoint

        # Start temperatures slightly off from setpoint
        temp = setpoint - np.random.uniform(-1.0, 1.0)

        for ts in time_index:
            # Simple first-order approach to setpoint with noise
            temp += 0.2 * (setpoint - temp) + np.random.normal(scale=0.2)
            co2 = np.clip(450 + 40 * z + np.random.normal(scale=50), 400, 2000)
            rh = np.clip(45 + np.random.normal(scale=5), 20, 80)

            records.append(
                {
                    "timestamp": ts,
                    "building_id": building_id,
                    "scenario_id": scenario_id,
                    "zone_id": zone_id,
                    "zone_name": zone_name,
                    "air_temp_C": temp,
                    "setpoint_C": setpoint,
                    "co2_ppm": co2,
                    "rh_pct": rh,
                }
            )
    return pd.DataFrame.from_records(records)


def generate_hvac(time_index: pd.DatetimeIndex, building_id: str, scenario_id: str, n_ahu: int = 2) -> pd.DataFrame:
    records = []
    hours = np.arange(len(time_index))
    for a in range(1, n_ahu + 1):
        ahu_id = f"AHU{a}"
        for i, ts in enumerate(time_index):
            # Schedule: more load during working hours (8-18)
            hour_of_day = hours[i] % 24
            occupied = 8 <= hour_of_day <= 18
            base_load = 5 if occupied else 1

            # Heating vs cooling split based on a fictive "heating season"
            heating_kw = max(base_load * 0.6 + np.random.normal(scale=0.5), 0)
            cooling_kw = max(base_load * 0.4 + np.random.normal(scale=0.5), 0)

            # Total power approx sum with some overhead
            power_kw = heating_kw + cooling_kw + np.random.uniform(0.5, 1.5)

            supply_temp = 18 + (2 if occupied else 0) + np.random.normal(scale=0.5)
            return_temp = supply_temp + np.random.uniform(3, 8)

            records.append(
                {
                    "timestamp": ts,
                    "building_id": building_id,
                    "scenario_id": scenario_id,
                    "ahu_id": ahu_id,
                    "supply_temp_C": supply_temp,
                    "return_temp_C": return_temp,
                    "power_kw": power_kw,
                    "cooling_kw": cooling_kw,
                    "heating_kw": heating_kw,
                }
            )
    return pd.DataFrame.from_records(records)


def generate_meters(time_index: pd.DatetimeIndex, building_id: str, scenario_id: str) -> pd.DataFrame:
    """Aggregate meter from a simple load model."""
    records = []
    hours = np.arange(len(time_index))

    for i, ts in enumerate(time_index):
        hour_of_day = hours[i] % 24
        occupied = 8 <= hour_of_day <= 18
        # Synthetic hourly energy (kWh)
        electric = 50 + (60 if occupied else 20) + np.random.normal(scale=5)
        heating = max(30 + (40 if occupied else 10) + np.random.normal(scale=5), 0)
        cooling = max(10 + (20 if occupied else 5) + np.random.normal(scale=3), 0)

        records.append(
            {
                "timestamp": ts,
                "building_id": building_id,
                "scenario_id": scenario_id,
                "electric_kwh": electric,
                "heating_kwh": heating,
                "cooling_kwh": cooling,
            }
        )

    return pd.DataFrame.from_records(records)


def write_run_zip(
    out_dir: str, building_id: str, scenario_id: str, start: str, days: int = 7, quiet: bool = False
) -> None:
    periods = days * HOURS_PER_DAY  # hourly data
    time_index = generate_time_index(start=start, periods=periods)
    weather = generate_weather(time_index)
    zones = generate_zones(time_index, building_id, scenario_id)
    hvac = generate_hvac(time_index, building_id, scenario_id)
    meters = generate_meters(time_index, building_id, scenario_id)

    run_id = f"run_{building_id}_{scenario_id}"
    run_folder = os.path.join(out_dir, run_id)
    ensure_dir(run_folder)

    metadata = {
        "building_id": building_id,
        "scenario_id": scenario_id,
        "building_name": f"Building {building_id}",
        "location": "Tallinn, Estonia",
        "floor_area_m2": 4000 + np.random.randint(-500, 500),
        "description": f"Synthetic IDA ICE run for {building_id} - {scenario_id}",
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }

    # Write files
    with open(os.path.join(run_folder, "metadata.json"), "w") as f:
        json.dump(metadata, f, indent=2)

    zones.to_csv(os.path.join(run_folder, "zones.csv"), index=False)
    hvac.to_csv(os.path.join(run_folder, "hvac.csv"), index=False)
    meters.to_csv(os.path.join(run_folder, "meters.csv"), index=False)
    weather.to_csv(os.path.join(run_folder, "weather.csv"), index=False)

    # Zip folder
    zip_path = os.path.join(out_dir, f"{run_id}.zip")
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for fname in ["metadata.json", "zones.csv", "hvac.csv", "meters.csv", "weather.csv"]:
            full_path = os.path.join(run_folder, fname)
            arcname = os.path.join(run_id, fname)
            zf.write(full_path, arcname=arcname)

    if not quiet:
        print(f"Created {zip_path}")


def main():
    """Main entry point with CLI argument parsing."""
    parser = argparse.ArgumentParser(
        description="Generate synthetic IDA ICE simulation data for testing and development",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate default sample data (3 buildings × 2 scenarios, 7 days)
  python src/generate_synthetic_idaice.py

  # Generate data for specific buildings and scenarios
  python src/generate_synthetic_idaice.py --buildings BLDG_01 BLDG_02 --scenarios BASE

  # Generate 14 days of data starting from a specific date
  python src/generate_synthetic_idaice.py --days 14 --start-date "2024-06-01 00:00"

  # Custom output directory
  python src/generate_synthetic_idaice.py --output data/custom/simulations
        """,
    )

    # Default base directory (repository root)
    default_base = Path(__file__).parent.parent
    default_output = default_base / "data" / "raw" / "simulations"

    parser.add_argument(
        "--buildings",
        nargs="+",
        default=["BLDG_01", "BLDG_02", "BLDG_03"],
        help="Building IDs to generate data for (default: BLDG_01 BLDG_02 BLDG_03)",
    )

    parser.add_argument(
        "--scenarios",
        nargs="+",
        default=["BASE", "RETROFIT"],
        help="Scenario IDs to generate data for (default: BASE RETROFIT)",
    )

    parser.add_argument(
        "--start-date",
        default="2024-01-01 00:00",
        help="Start date and time for simulation (default: 2024-01-01 00:00)",
    )

    parser.add_argument("--days", type=int, default=7, help="Number of days to simulate (default: 7)")

    parser.add_argument(
        "--output",
        default=str(default_output),
        help=f"Output directory for simulation files (default: {default_output})",
    )

    parser.add_argument("--quiet", action="store_true", help="Suppress progress output")

    args = parser.parse_args()

    # Validate arguments
    if args.days < 1:
        print("ERROR: --days must be at least 1", file=sys.stderr)
        sys.exit(1)

    if not args.buildings:
        print("ERROR: At least one building ID must be specified", file=sys.stderr)
        sys.exit(1)
    if not args.scenarios:
        print("ERROR: At least one scenario ID must be specified", file=sys.stderr)
        sys.exit(1)

    # Validate start date format
    try:
        pd.to_datetime(args.start_date)
    except Exception as e:
        print(f"ERROR: Invalid start date format '{args.start_date}': {e}", file=sys.stderr)
        print("Expected format: YYYY-MM-DD HH:MM", file=sys.stderr)
        sys.exit(1)

    # Create output directory
    output_dir = args.output
    ensure_dir(output_dir)

    if not args.quiet:
        print("=" * 70)
        print("SYNTHETIC IDA ICE DATA GENERATOR")
        print("=" * 70)
        print("\nConfiguration:")
        print(f"  Buildings: {', '.join(args.buildings)}")
        print(f"  Scenarios: {', '.join(args.scenarios)}")
        print(f"  Start date: {args.start_date}")
        print(f"  Duration: {args.days} days")
        print(f"  Output: {output_dir}")
        num_runs = len(args.buildings) * len(args.scenarios)
        print(f"\nGenerating {len(args.buildings)} × {len(args.scenarios)} = {num_runs} simulation runs...")
        print()

    # Generate all combinations
    for building_id in args.buildings:
        for scenario_id in args.scenarios:
            write_run_zip(output_dir, building_id, scenario_id, start=args.start_date, days=args.days, quiet=args.quiet)

    if not args.quiet:
        print()
        print("=" * 70)
        print("✓ GENERATION COMPLETE")
        print("=" * 70)
        print(f"\nGenerated {len(args.buildings) * len(args.scenarios)} simulation runs in: {output_dir}")
        print("\nNext steps:")
        print("  1. Run the ETL pipeline: python run_pipeline.py")
        print("  2. Query the results: duckdb data/processed/duckdb/simulations.duckdb")
        print("  3. Explore notebooks: jupyter notebook notebooks/")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Main ETL Pipeline Runner for IDA ICE Simulation Data.

This script runs the complete ETL pipeline:
1. Extract simulation runs from ZIP files
2. Transform data into star schema
3. Validate data quality
4. Load into Parquet and DuckDB
"""

import sys
import argparse
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from src.etl.extract import extract_runs
from src.etl.transform import transform_all
from src.etl.validate import validate_all, print_validation_results
from src.etl.load import load_to_parquet, load_to_duckdb


def run_pipeline(
    simulations_dir: str = "data/raw/simulations",
    parquet_dir: str = "data/processed/parquet",
    duckdb_path: str = "data/processed/duckdb/simulations.duckdb",
    skip_validation: bool = False,
    non_interactive: bool = False
) -> bool:
    """
    Run the complete ETL pipeline.
    
    Args:
        simulations_dir: Directory containing simulation ZIP files
        parquet_dir: Output directory for Parquet files
        duckdb_path: Path to DuckDB database file
        skip_validation: Skip validation step if True
        non_interactive: Fail immediately on validation errors without prompting
        
    Returns:
        True if pipeline completed successfully, False otherwise
    """
    print("=" * 70)
    print("IDA ICE ENERGY SIMULATION ETL PIPELINE")
    print("=" * 70)
    
    # Validate input paths
    sim_path = Path(simulations_dir)
    if not sim_path.exists():
        print(f"  ERROR: Simulations directory does not exist: {simulations_dir}")
        return False
    
    if not sim_path.is_dir():
        print(f"  ERROR: Simulations path is not a directory: {simulations_dir}")
        return False
    
    # Validate output paths are writable
    try:
        parquet_path = Path(parquet_dir)
        parquet_path.mkdir(parents=True, exist_ok=True)
        
        duckdb_file = Path(duckdb_path)
        duckdb_file.parent.mkdir(parents=True, exist_ok=True)
    except (PermissionError, OSError) as e:
        print(f"  ERROR: Cannot create output directories: {e}")
        return False
    
    # Step 1: Extract
    print("\n[1/4] EXTRACTING simulation data...")
    print(f"  Source: {simulations_dir}")
    try:
        runs = extract_runs(simulations_dir)
        if not runs:
            print("  ERROR: No simulation runs found!")
            return False
        print(f"  ✓ Extracted {len(runs)} simulation runs")
    except Exception as e:
        print(f"  ERROR: Extraction failed: {e}")
        return False
    
    # Step 2: Transform
    print("\n[2/4] TRANSFORMING to star schema...")
    try:
        schema = transform_all(runs)
        print(f"  ✓ Created {len(schema)} tables:")
        for table_name, df in schema.items():
            print(f"     - {table_name}: {len(df):,} rows")
    except Exception as e:
        print(f"  ERROR: Transformation failed: {e}")
        return False
    
    # Step 3: Validate
    if not skip_validation:
        print("\n[3/4] VALIDATING data quality...")
        try:
            results = validate_all(schema)
            
            if results['is_valid']:
                print("  ✓ All validation checks passed")
            else:
                print("  ✗ Validation failed!")
                print_validation_results(results)
                
                # In non-interactive mode, fail immediately
                if non_interactive:
                    print("  Pipeline aborted (non-interactive mode).")
                    return False
                
                # In interactive mode, prompt for continuation
                try:
                    response = input("\n  Continue with loading? (y/n): ")
                    if response.lower() != 'y':
                        print("  Pipeline aborted.")
                        return False
                except (EOFError, KeyboardInterrupt):
                    # Handle case where stdin is not available
                    print("\n  Pipeline aborted (no input available).")
                    return False
        except Exception as e:
            print(f"  ERROR: Validation failed: {e}")
            return False
    else:
        print("\n[3/4] VALIDATION skipped")
    
    # Step 4: Load
    print("\n[4/4] LOADING to storage...")
    try:
        # Load to Parquet
        print(f"  Parquet: {parquet_dir}")
        load_to_parquet(schema, parquet_dir)
        
        # Load to DuckDB
        print(f"\n  DuckDB: {duckdb_path}")
        load_to_duckdb(schema, duckdb_path)
        
        print("\n  ✓ Data loaded successfully")
    except Exception as e:
        print(f"  ERROR: Loading failed: {e}")
        return False
    
    # Summary
    print("\n" + "=" * 70)
    print("PIPELINE COMPLETED SUCCESSFULLY")
    print("=" * 70)
    print(f"\nOutputs:")
    print(f"  - Parquet files: {parquet_dir}")
    print(f"  - DuckDB database: {duckdb_path}")
    print(f"\nNext steps:")
    print(f"  - Run analysis notebooks in notebooks/")
    print(f"  - Query DuckDB views:")
    print(f"    • vw_zone_with_weather")
    print(f"    • vw_hvac_with_meters")
    print(f"    • vw_energy_summary")
    
    return True


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Run IDA ICE ETL Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run full pipeline with default paths
  python run_pipeline.py
  
  # Run with custom paths
  python run_pipeline.py --simulations data/raw/simulations --parquet data/output/parquet
  
  # Skip validation
  python run_pipeline.py --skip-validation
        """
    )
    
    parser.add_argument(
        '--simulations',
        default='data/raw/simulations',
        help='Directory containing simulation ZIP files (default: data/raw/simulations)'
    )
    
    parser.add_argument(
        '--parquet',
        default='data/processed/parquet',
        help='Output directory for Parquet files (default: data/processed/parquet)'
    )
    
    parser.add_argument(
        '--duckdb',
        default='data/processed/duckdb/simulations.duckdb',
        help='Path to DuckDB database file (default: data/processed/duckdb/simulations.duckdb)'
    )
    
    parser.add_argument(
        '--skip-validation',
        action='store_true',
        help='Skip data validation step'
    )
    
    parser.add_argument(
        '--non-interactive',
        action='store_true',
        help='Fail immediately on validation errors without prompting (for CI/CD)'
    )
    
    args = parser.parse_args()
    
    # Run pipeline
    success = run_pipeline(
        simulations_dir=args.simulations,
        parquet_dir=args.parquet,
        duckdb_path=args.duckdb,
        skip_validation=args.skip_validation,
        non_interactive=args.non_interactive
    )
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()

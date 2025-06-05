"""
Data validation module for IDA ICE simulation data.

This module provides validation functions to ensure data quality and
integrity throughout the ETL pipeline.
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, List, Tuple


class ValidationError(Exception):
    """Custom exception for validation failures."""
    pass


def validate_schema(data: Dict[str, pd.DataFrame], expected_schema: Dict[str, List[str]]) -> Tuple[bool, List[str]]:
    """
    Validate data against expected schema.

    Args:
        data: Dictionary containing DataFrames
        expected_schema: Dictionary mapping table names to list of expected columns

    Returns:
        Tuple of (is_valid, error_messages)
    """
    errors = []
    
    # Columns that are allowed to have NULL values
    nullable_columns = {
        'fact_hvac': ['cop_proxy']  # COP proxy can be NULL for low-power conditions
    }
    
    for table_name, expected_cols in expected_schema.items():
        if table_name not in data:
            errors.append(f"Missing table: {table_name}")
            continue
            
        df = data[table_name]
        actual_cols = set(df.columns)
        expected_cols_set = set(expected_cols)
        
        missing_cols = expected_cols_set - actual_cols
        if missing_cols:
            errors.append(f"Table '{table_name}' missing columns: {missing_cols}")
        
        # Check for required columns having null values
        # Skip columns that are explicitly allowed to be nullable
        allowed_nulls = nullable_columns.get(table_name, [])
        for col in expected_cols:
            if col in df.columns and col not in allowed_nulls and df[col].isnull().any():
                null_count = df[col].isnull().sum()
                errors.append(f"Table '{table_name}' has {null_count} null values in column '{col}'")
    
    return len(errors) == 0, errors


def validate_value_ranges(schema: Dict[str, pd.DataFrame]) -> Tuple[bool, List[str]]:
    """
    Validate that values are within plausible ranges.
    
    Args:
        schema: Dictionary containing dimension and fact tables
        
    Returns:
        Tuple of (is_valid, error_messages)
    """
    errors = []
    
    # Validate zone conditions
    if 'fact_zone_conditions' in schema:
        df = schema['fact_zone_conditions']
        
        # Air temperature: 10-35°C
        invalid_temp = df[(df['air_temp_C'] < 10) | (df['air_temp_C'] > 35)]
        if len(invalid_temp) > 0:
            errors.append(f"Zone conditions: {len(invalid_temp)} air temperatures out of range (10-35°C)")
        
        # CO2: 400-2500 ppm
        invalid_co2 = df[(df['co2_ppm'] < 400) | (df['co2_ppm'] > 2500)]
        if len(invalid_co2) > 0:
            errors.append(f"Zone conditions: {len(invalid_co2)} CO2 values out of range (400-2500 ppm)")
        
        # RH: 0-100%
        invalid_rh = df[(df['rh_pct'] < 0) | (df['rh_pct'] > 100)]
        if len(invalid_rh) > 0:
            errors.append(f"Zone conditions: {len(invalid_rh)} RH values out of range (0-100%)")
    
    # Validate weather data
    if 'fact_weather' in schema:
        df = schema['fact_weather']
        
        # Outdoor temperature: -30 to 40°C
        invalid_temp = df[(df['drybulb_C'] < -30) | (df['drybulb_C'] > 40)]
        if len(invalid_temp) > 0:
            errors.append(f"Weather: {len(invalid_temp)} temperatures out of range (-30 to 40°C)")
        
        # RH: 0-100%
        invalid_rh = df[(df['relhum_pct'] < 0) | (df['relhum_pct'] > 100)]
        if len(invalid_rh) > 0:
            errors.append(f"Weather: {len(invalid_rh)} RH values out of range (0-100%)")
        
        # GHI: >= 0
        invalid_ghi = df[df['ghi_W_m2'] < 0]
        if len(invalid_ghi) > 0:
            errors.append(f"Weather: {len(invalid_ghi)} GHI values negative")
    
    # Validate HVAC data
    if 'fact_hvac' in schema:
        df = schema['fact_hvac']
        
        # Power should be non-negative
        invalid_power = df[df['power_kw'] < 0]
        if len(invalid_power) > 0:
            errors.append(f"HVAC: {len(invalid_power)} negative power values")
        
        # Heating and cooling should be non-negative
        invalid_heating = df[df['heating_kw'] < 0]
        if len(invalid_heating) > 0:
            errors.append(f"HVAC: {len(invalid_heating)} negative heating values")
        
        invalid_cooling = df[df['cooling_kw'] < 0]
        if len(invalid_cooling) > 0:
            errors.append(f"HVAC: {len(invalid_cooling)} negative cooling values")
    
    # Validate meters
    if 'fact_meters' in schema:
        df = schema['fact_meters']
        
        # All energy values should be non-negative
        for col in ['electric_kwh', 'heating_kwh', 'cooling_kwh']:
            if col in df.columns:
                invalid = df[df[col] < 0]
                if len(invalid) > 0:
                    errors.append(f"Meters: {len(invalid)} negative {col} values")
    
    return len(errors) == 0, errors


def validate_temporal_coverage(schema: Dict[str, pd.DataFrame]) -> Tuple[bool, List[str]]:
    """
    Validate temporal coverage and consistency.
    
    Args:
        schema: Dictionary containing dimension and fact tables
        
    Returns:
        Tuple of (is_valid, error_messages)
    """
    errors = []
    
    if 'dim_time' not in schema:
        errors.append("Time dimension table not found")
        return False, errors
    
    time_df = schema['dim_time']
    timestamps = pd.to_datetime(time_df['timestamp']).sort_values()
    
    # Check for gaps in time series (assuming hourly data)
    if len(timestamps) > 1:
        time_diffs = timestamps.diff().dropna()
        expected_diff = pd.Timedelta(hours=1)
        
        gaps = time_diffs[time_diffs != expected_diff]
        if len(gaps) > 0:
            errors.append(f"Time series has {len(gaps)} gaps (expected hourly intervals)")
    
    # Check that fact tables have data for all time periods
    time_keys = set(time_df['time_key'])
    
    for fact_table in ['fact_zone_conditions', 'fact_hvac', 'fact_meters', 'fact_weather']:
        if fact_table in schema:
            df = schema[fact_table]
            fact_time_keys = set(df['time_key'].unique())
            
            missing_times = time_keys - fact_time_keys
            if missing_times:
                errors.append(f"{fact_table}: missing data for {len(missing_times)} time periods")
    
    return len(errors) == 0, errors


def validate_energy_plausibility(schema: Dict[str, pd.DataFrame]) -> Tuple[bool, List[str]]:
    """
    Validate energy plausibility checks.
    
    Args:
        schema: Dictionary containing dimension and fact tables
        
    Returns:
        Tuple of (is_valid, error_messages)
    """
    errors = []
    
    if 'fact_meters' not in schema:
        errors.append("Meters fact table not found")
        return False, errors
    
    meters_df = schema['fact_meters']
    
    # Group by building and scenario
    for (building_id, scenario_id), group in meters_df.groupby(['building_id', 'scenario_id']):
        # Total energy should be positive
        total_electric = group['electric_kwh'].sum()
        total_heating = group['heating_kwh'].sum()
        total_cooling = group['cooling_kwh'].sum()
        
        if total_electric <= 0:
            errors.append(f"Building {building_id} / {scenario_id}: total electric energy <= 0")
        
        if total_heating + total_cooling <= 0:
            errors.append(f"Building {building_id} / {scenario_id}: total heating + cooling energy <= 0")
        
        # Electric should generally be >= heating + cooling (simplified check)
        # This is a very simplified energy balance - in reality it's more complex
        # Note: This assumes electric is primarily for HVAC equipment, not heating loads
        # The 0.2 threshold may not hold for all building types (e.g., buildings with 
        # electric heating or data centers with high plug loads). Consider making this
        # configurable for production use.
        if total_electric < 0.2 * (total_heating + total_cooling):
            errors.append(
                f"Building {building_id} / {scenario_id}: electric energy seems too low "
                f"relative to heating+cooling (electric: {total_electric:.0f}, "
                f"heating+cooling: {total_heating + total_cooling:.0f})"
            )
    
    return len(errors) == 0, errors


def validate_all(schema: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
    """
    Run all validation checks on the schema.
    
    Args:
        schema: Dictionary containing dimension and fact tables
        
    Returns:
        Dictionary with validation results
    """
    results = {
        'is_valid': True,
        'checks': {}
    }
    
    # Define expected schema
    expected_schema = {
        'dim_building': ['building_id', 'building_name', 'location', 'floor_area_m2'],
        'dim_scenario': ['scenario_id', 'description'],
        'dim_zone': ['zone_key', 'building_id', 'zone_id', 'zone_name'],
        'dim_ahu': ['ahu_key', 'building_id', 'ahu_id'],
        'dim_time': ['time_key', 'timestamp', 'year', 'month', 'day', 'hour', 'dow', 'is_weekend'],
        'fact_zone_conditions': ['time_key', 'zone_key', 'scenario_id', 'air_temp_C', 'setpoint_C', 'co2_ppm', 'rh_pct'],
        'fact_hvac': ['time_key', 'ahu_key', 'scenario_id', 'power_kw', 'cooling_kw', 'heating_kw', 'cop_proxy'],
        'fact_meters': ['time_key', 'building_id', 'scenario_id', 'electric_kwh', 'heating_kwh', 'cooling_kwh'],
        'fact_weather': ['time_key', 'building_id', 'drybulb_C', 'relhum_pct', 'ghi_W_m2']
    }
    
    # Run schema validation
    is_valid, errors = validate_schema(schema, expected_schema)
    results['checks']['schema'] = {
        'valid': is_valid,
        'errors': errors
    }
    if not is_valid:
        results['is_valid'] = False
    
    # Run value range validation
    is_valid, errors = validate_value_ranges(schema)
    results['checks']['value_ranges'] = {
        'valid': is_valid,
        'errors': errors
    }
    if not is_valid:
        results['is_valid'] = False
    
    # Run temporal coverage validation
    is_valid, errors = validate_temporal_coverage(schema)
    results['checks']['temporal_coverage'] = {
        'valid': is_valid,
        'errors': errors
    }
    if not is_valid:
        results['is_valid'] = False
    
    # Run energy plausibility validation
    is_valid, errors = validate_energy_plausibility(schema)
    results['checks']['energy_plausibility'] = {
        'valid': is_valid,
        'errors': errors
    }
    if not is_valid:
        results['is_valid'] = False
    
    return results


def print_validation_results(results: Dict[str, Any]) -> None:
    """
    Print validation results in a readable format.
    
    Args:
        results: Dictionary with validation results from validate_all()
    """
    print("=" * 60)
    print("VALIDATION RESULTS")
    print("=" * 60)
    
    overall_status = "PASSED" if results['is_valid'] else "FAILED"
    print(f"\nOverall Status: {overall_status}\n")
    
    for check_name, check_result in results['checks'].items():
        status = "✓ PASS" if check_result['valid'] else "✗ FAIL"
        print(f"{check_name.upper()}: {status}")
        
        if check_result['errors']:
            for error in check_result['errors']:
                print(f"  - {error}")
        print()
    
    print("=" * 60)

"""
Data transformation module for IDA ICE simulation data.

This module provides functions for cleaning, normalizing, and transforming
simulation data into standardized star schema formats.
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, List, Tuple


def create_dim_building(runs: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Create building dimension table from simulation runs.
    
    Args:
        runs: List of extracted run dictionaries
        
    Returns:
        DataFrame with columns: building_id, building_name, location, floor_area_m2
    """
    if not runs:
        return pd.DataFrame(columns=['building_id', 'building_name', 'location', 'floor_area_m2'])
    
    buildings = []
    seen = set()
    
    for run in runs:
        metadata = run['metadata']
        building_id = metadata['building_id']
        
        if building_id not in seen:
            buildings.append({
                'building_id': building_id,
                'building_name': metadata['building_name'],
                'location': metadata['location'],
                'floor_area_m2': metadata['floor_area_m2']
            })
            seen.add(building_id)
    
    return pd.DataFrame(buildings).sort_values('building_id').reset_index(drop=True)


def create_dim_scenario(runs: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Create scenario dimension table from simulation runs.
    
    Args:
        runs: List of extracted run dictionaries
        
    Returns:
        DataFrame with columns: scenario_id, description
    """
    if not runs:
        return pd.DataFrame(columns=['scenario_id', 'description'])
    
    scenarios = []
    seen = set()
    
    for run in runs:
        metadata = run['metadata']
        scenario_id = metadata['scenario_id']
        
        if scenario_id not in seen:
            scenarios.append({
                'scenario_id': scenario_id,
                'description': metadata.get('description', f'Scenario {scenario_id}')
            })
            seen.add(scenario_id)
    
    return pd.DataFrame(scenarios).sort_values('scenario_id').reset_index(drop=True)


def create_dim_zone(runs: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Create zone dimension table from simulation runs.
    
    Args:
        runs: List of extracted run dictionaries
        
    Returns:
        DataFrame with columns: zone_key, building_id, zone_id, zone_name
    """
    if not runs:
        return pd.DataFrame(columns=['zone_key', 'building_id', 'zone_id', 'zone_name'])
    
    zones = []
    
    for run in runs:
        metadata = run['metadata']
        building_id = metadata['building_id']
        zones_df = run['zones']
        
        # Get unique zones for this building
        unique_zones = zones_df[['zone_id', 'zone_name']].drop_duplicates()
        
        for _, zone in unique_zones.iterrows():
            zones.append({
                'building_id': building_id,
                'zone_id': zone['zone_id'],
                'zone_name': zone['zone_name']
            })
    
    # Remove duplicates and add zone_key
    zones_df = pd.DataFrame(zones).drop_duplicates().sort_values(['building_id', 'zone_id']).reset_index(drop=True)
    zones_df['zone_key'] = range(1, len(zones_df) + 1)
    
    return zones_df[['zone_key', 'building_id', 'zone_id', 'zone_name']]


def create_dim_ahu(runs: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Create AHU (Air Handling Unit) dimension table from simulation runs.
    
    Args:
        runs: List of extracted run dictionaries
        
    Returns:
        DataFrame with columns: ahu_key, building_id, ahu_id
    """
    if not runs:
        return pd.DataFrame(columns=['ahu_key', 'building_id', 'ahu_id'])
    
    ahus = []
    
    for run in runs:
        metadata = run['metadata']
        building_id = metadata['building_id']
        hvac_df = run['hvac']
        
        # Get unique AHUs for this building
        unique_ahus = hvac_df[['ahu_id']].drop_duplicates()
        
        for _, ahu in unique_ahus.iterrows():
            ahus.append({
                'building_id': building_id,
                'ahu_id': ahu['ahu_id']
            })
    
    # Remove duplicates and add ahu_key
    ahus_df = pd.DataFrame(ahus).drop_duplicates().sort_values(['building_id', 'ahu_id']).reset_index(drop=True)
    ahus_df['ahu_key'] = range(1, len(ahus_df) + 1)
    
    return ahus_df[['ahu_key', 'building_id', 'ahu_id']]


def create_dim_time(runs: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Create time dimension table from simulation runs.
    
    Args:
        runs: List of extracted run dictionaries
        
    Returns:
        DataFrame with columns: time_key, timestamp, year, month, day, hour, dow, is_weekend
    """
    if not runs:
        return pd.DataFrame(columns=['time_key', 'timestamp', 'year', 'month', 'day', 'hour', 'dow', 'is_weekend'])
    
    # Collect all unique timestamps
    all_timestamps = set()
    
    for run in runs:
        # Get timestamps from any of the dataframes (they should all be aligned)
        timestamps = run['zones']['timestamp'].unique()
        all_timestamps.update(timestamps)
    
    # Create time dimension
    timestamps = sorted(list(all_timestamps))
    time_data = []
    
    for i, ts in enumerate(timestamps, start=1):
        ts = pd.Timestamp(ts)
        time_data.append({
            'time_key': i,
            'timestamp': ts,
            'year': ts.year,
            'month': ts.month,
            'day': ts.day,
            'hour': ts.hour,
            'dow': ts.dayofweek,  # Monday=0, Sunday=6
            'is_weekend': ts.dayofweek >= 5
        })
    
    return pd.DataFrame(time_data)


def create_fact_zone_conditions(runs: List[Dict[str, Any]], 
                                  dim_time: pd.DataFrame,
                                  dim_zone: pd.DataFrame,
                                  dim_scenario: pd.DataFrame) -> pd.DataFrame:
    """
    Create fact table for zone conditions.
    
    Args:
        runs: List of extracted run dictionaries
        dim_time: Time dimension table
        dim_zone: Zone dimension table
        dim_scenario: Scenario dimension table
        
    Returns:
        DataFrame with grain: one row per (timestamp, zone, scenario)
    """
    facts = []
    
    for run in runs:
        metadata = run['metadata']
        scenario_id = metadata['scenario_id']
        zones_df = run['zones'].copy()
        
        # Join with dimensions to get keys
        zones_df = zones_df.merge(
            dim_time[['time_key', 'timestamp']], 
            on='timestamp', 
            how='left'
        )
        
        zones_df = zones_df.merge(
            dim_zone[['zone_key', 'building_id', 'zone_id']], 
            on=['building_id', 'zone_id'], 
            how='left'
        )
        
        zones_df['scenario_id'] = scenario_id
        
        # Select fact columns
        fact_cols = [
            'time_key', 'zone_key', 'scenario_id',
            'air_temp_C', 'setpoint_C', 'co2_ppm', 'rh_pct'
        ]
        
        facts.append(zones_df[fact_cols])
    
    if not facts:
        return pd.DataFrame(columns=[
            'time_key', 'zone_key', 'scenario_id',
            'air_temp_C', 'setpoint_C', 'co2_ppm', 'rh_pct'
        ])
    
    return pd.concat(facts, ignore_index=True)


def create_fact_hvac(runs: List[Dict[str, Any]], 
                      dim_time: pd.DataFrame,
                      dim_ahu: pd.DataFrame,
                      dim_scenario: pd.DataFrame) -> pd.DataFrame:
    """
    Create fact table for HVAC system data with derived COP proxy metric.
    
    Args:
        runs: List of extracted run dictionaries
        dim_time: Time dimension table
        dim_ahu: AHU dimension table
        dim_scenario: Scenario dimension table
        
    Returns:
        DataFrame with grain: one row per (timestamp, ahu, scenario)
    """
    facts = []
    
    for run in runs:
        metadata = run['metadata']
        scenario_id = metadata['scenario_id']
        hvac_df = run['hvac'].copy()
        
        # Calculate COP proxy, set to NaN when power_kw < 1.0 kW
        # This avoids artificially high COP values when power is very low
        cop_threshold = 1.0
        hvac_df['cop_proxy'] = np.where(
            hvac_df['power_kw'] >= cop_threshold,
            (hvac_df['heating_kw'] + hvac_df['cooling_kw']) / hvac_df['power_kw'],
            np.nan
        )
        
        # Join with dimensions to get keys
        hvac_df = hvac_df.merge(
            dim_time[['time_key', 'timestamp']], 
            on='timestamp', 
            how='left'
        )
        
        hvac_df = hvac_df.merge(
            dim_ahu[['ahu_key', 'building_id', 'ahu_id']], 
            on=['building_id', 'ahu_id'], 
            how='left'
        )
        
        hvac_df['scenario_id'] = scenario_id
        
        # Select fact columns
        fact_cols = [
            'time_key', 'ahu_key', 'scenario_id',
            'supply_temp_C', 'return_temp_C', 
            'power_kw', 'cooling_kw', 'heating_kw', 'cop_proxy'
        ]
        
        facts.append(hvac_df[fact_cols])
    
    if not facts:
        return pd.DataFrame(columns=[
            'time_key', 'ahu_key', 'scenario_id',
            'supply_temp_C', 'return_temp_C',
            'power_kw', 'cooling_kw', 'heating_kw', 'cop_proxy'
        ])
    
    return pd.concat(facts, ignore_index=True)


def create_fact_meters(runs: List[Dict[str, Any]], 
                        dim_time: pd.DataFrame,
                        dim_building: pd.DataFrame,
                        dim_scenario: pd.DataFrame) -> pd.DataFrame:
    """
    Create fact table for building meters.
    
    Args:
        runs: List of extracted run dictionaries
        dim_time: Time dimension table
        dim_building: Building dimension table
        dim_scenario: Scenario dimension table
        
    Returns:
        DataFrame with grain: one row per (timestamp, building, scenario)
    """
    facts = []
    
    for run in runs:
        metadata = run['metadata']
        scenario_id = metadata['scenario_id']
        meters_df = run['meters'].copy()
        
        # Join with dimensions to get keys
        meters_df = meters_df.merge(
            dim_time[['time_key', 'timestamp']], 
            on='timestamp', 
            how='left'
        )
        
        meters_df['scenario_id'] = scenario_id
        
        # Select fact columns
        fact_cols = [
            'time_key', 'building_id', 'scenario_id',
            'electric_kwh', 'heating_kwh', 'cooling_kwh'
        ]
        
        facts.append(meters_df[fact_cols])
    
    if not facts:
        return pd.DataFrame(columns=[
            'time_key', 'building_id', 'scenario_id',
            'electric_kwh', 'heating_kwh', 'cooling_kwh'
        ])
    
    return pd.concat(facts, ignore_index=True)


def create_fact_weather(runs: List[Dict[str, Any]], 
                         dim_time: pd.DataFrame,
                         dim_building: pd.DataFrame) -> pd.DataFrame:
    """
    Create fact table for weather data.
    
    Args:
        runs: List of extracted run dictionaries
        dim_time: Time dimension table
        dim_building: Building dimension table
        
    Returns:
        DataFrame with grain: one row per (timestamp, building)
    """
    # Weather is the same for all runs with same timestamp
    # Generate once and associate with all buildings
    if not runs:
        return pd.DataFrame(columns=[
            'time_key', 'building_id',
            'drybulb_C', 'relhum_pct', 'ghi_W_m2'
        ])
    
    # Get weather data from first run (it's the same for all)
    weather_df = runs[0]['weather'].copy()
    
    # Join with time dimension once
    weather_df = weather_df.merge(
        dim_time[['time_key', 'timestamp']], 
        on='timestamp', 
        how='left'
    )
    
    # Create records for each building
    facts = []
    fact_cols = [
        'time_key', 'building_id',
        'drybulb_C', 'relhum_pct', 'ghi_W_m2'
    ]
    
    for building_id in dim_building['building_id']:
        building_weather = weather_df.copy()
        building_weather['building_id'] = building_id
        facts.append(building_weather[fact_cols])
    
    if not facts:
        return pd.DataFrame(columns=[
            'time_key', 'building_id',
            'drybulb_C', 'relhum_pct', 'ghi_W_m2'
        ])
    
    return pd.concat(facts, ignore_index=True)


def transform_all(runs: List[Dict[str, Any]]) -> Dict[str, pd.DataFrame]:
    """
    Transform all extracted runs into star schema.
    
    Args:
        runs: List of extracted run dictionaries
        
    Returns:
        Dictionary containing all dimension and fact tables
    """
    # Create dimension tables
    dim_building = create_dim_building(runs)
    dim_scenario = create_dim_scenario(runs)
    dim_zone = create_dim_zone(runs)
    dim_ahu = create_dim_ahu(runs)
    dim_time = create_dim_time(runs)
    
    # Create fact tables
    fact_zone_conditions = create_fact_zone_conditions(runs, dim_time, dim_zone, dim_scenario)
    fact_hvac = create_fact_hvac(runs, dim_time, dim_ahu, dim_scenario)
    fact_meters = create_fact_meters(runs, dim_time, dim_building, dim_scenario)
    fact_weather = create_fact_weather(runs, dim_time, dim_building)
    
    return {
        'dim_building': dim_building,
        'dim_scenario': dim_scenario,
        'dim_zone': dim_zone,
        'dim_ahu': dim_ahu,
        'dim_time': dim_time,
        'fact_zone_conditions': fact_zone_conditions,
        'fact_hvac': fact_hvac,
        'fact_meters': fact_meters,
        'fact_weather': fact_weather
    }

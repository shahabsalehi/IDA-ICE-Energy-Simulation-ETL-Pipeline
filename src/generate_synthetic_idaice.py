import json
import random
from datetime import datetime, timedelta

def generate_simulation_data(num_buildings=5):
    simulations = []
    building_types = ["office", "residential", "commercial", "industrial"]
    for i in range(num_buildings):
        sim_date = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 180))
        heating_kwh = random.uniform(50000, 200000)
        cooling_kwh = random.uniform(30000, 150000)
        simulations.append({
            "simulation_id": f"SIM-{i+1:04d}",
            "building_type": random.choice(building_types),
            "floor_area_m2": random.randint(500, 5000),
            "simulation_date": sim_date.isoformat(),
            "heating_energy_kwh": round(heating_kwh, 2),
            "cooling_energy_kwh": round(cooling_kwh, 2),
            "total_energy_kwh": round(heating_kwh + cooling_kwh, 2)
        })
    return simulations

if __name__ == "__main__":
    data = generate_simulation_data()
    with open("data/simulations.json", "w") as f:
        json.dump(data, f, indent=2)
    print(f"Generated {len(data)} simulations")

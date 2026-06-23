import asyncio
import json
import random
import uuid
from datetime import datetime, timezone
from pydantic import BaseModel, Field

# 1. Define the Telemetry Schema using Pydantic
class VehicleTelemetry(BaseModel):
    vehicle_id: str
    timestamp: str
    engine_temperature: float = Field(..., ge=50.0, le=150.0)
    vibration_amplitude: float = Field(..., ge=0.0, le=10.0)
    fuel_flow_rate: float
    gps_coordinates: dict

# 2. Simulate an Individual Truck Device
async def simulate_truck(vehicle_id: str, is_anomaly_vehicle: bool = False):
    """Simulates a continuous stream of IoT readings from a specific truck."""
    print(f"[START] Initializing telemetry stream for Vehicle: {vehicle_id}")
    
    iteration = 0
    while True:
        iteration += 1
        
        # Base normal operating parameters
        temp = random.uniform(85.0, 98.0)
        vibration = random.uniform(1.2, 2.5)
        
        # Inject an anomaly if this vehicle is designated to fail over time
        if is_anomaly_vehicle and iteration > 10:
            # Gradually spike temperature and vibration to simulate failure
            temp += random.uniform(5.0, 15.0) * (iteration - 10)
            vibration += random.uniform(0.5, 1.8) * (iteration - 10)
            print(f"⚠️ [ANOMALY INJECTED] Vehicle {vehicle_id} exhibiting critical wear profiles.")

        # Construct payload mapping to the strict schema
        payload = VehicleTelemetry(
            vehicle_id=vehicle_id,
            timestamp=datetime.now(timezone.utc).isoformat(),
            engine_temperature=round(temp, 2),
            vibration_amplitude=round(vibration, 2),
            fuel_flow_rate=round(random.uniform(12.0, 18.5), 2),
            gps_coordinates={
                "latitude": round(random.uniform(9.0, 9.1), 5),  # Simulated local zone
                "longitude": round(random.uniform(7.4, 7.5), 5)
            }
        )
        
        # Output the serialized JSON payload
        print(f"📡 [SENDING] {payload.model_dump_json()}")
        
        # Wait 2 seconds before sending the next telemetry interval
        await asyncio.sleep(2.0)

# 3. Main Orchestrator to Run Concurrent Tasks
async def main():
    # Generate unique UUIDs for 5 distinct vehicles to start small
    fleet_ids = [str(uuid.uuid4())[:8] for _ in range(5)]
    
    # Designate the first vehicle as our anomaly generator
    tasks = []
    for idx, vehicle_id in enumerate(fleet_ids):
        is_anomaly = (idx == 0) 
        tasks.append(simulate_truck(vehicle_id, is_anomaly_vehicle=is_anomaly))
    
    # Run all vehicle simulations concurrently
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nSimulation terminated by user.")
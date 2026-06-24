from ingestion.producer import send_telemetry_to_kafka, producer
import asyncio
import json
import random
import uuid
from datetime import datetime, timezone
from pydantic import BaseModel, Field

# 1. Define the Telemetry Schema with realistic extreme limits
class VehicleTelemetry(BaseModel):
    vehicle_id: str
    timestamp: str
    # Enforce that it's a valid positive number, but remove the maximum cap entirely
    engine_temperature: float = Field(..., ge=0.0) 
    vibration_amplitude: float = Field(..., ge=0.0)
    fuel_flow_rate: float
    gps_coordinates: dict

# 2. Simulate an Individual Truck Device
async def simulate_truck(vehicle_id: str, is_anomaly_vehicle: bool = False):
    print(f"[START] Initializing telemetry stream for Vehicle: {vehicle_id}")
    
    iteration = 0
    while True:
        iteration += 1
        temp = random.uniform(85.0, 98.0)
        vibration = random.uniform(1.2, 2.5)
        
        if is_anomaly_vehicle and iteration > 10:
            severity_factor = min(iteration - 10, 8)
            temp += random.uniform(8.0, 12.0) * severity_factor
            vibration += random.uniform(0.4, 0.8) * severity_factor
            print(f"⚠️ [CRITICAL ALERT] Vehicle {vehicle_id} operating in extreme failure zone.")

        payload = VehicleTelemetry(
            vehicle_id=vehicle_id,
            timestamp=datetime.now(timezone.utc).isoformat(),
            engine_temperature=round(temp, 2),
            vibration_amplitude=round(vibration, 2),
            fuel_flow_rate=round(random.uniform(12.0, 18.5), 2),
            gps_coordinates={
                "latitude": round(random.uniform(9.0, 9.1), 5),
                "longitude": round(random.uniform(7.4, 7.5), 5)
            }
        )
        
                # Instead of just printing, we grab the raw dictionary and pass it to Kafka
        data_to_send = payload.model_dump()
        
        send_telemetry_to_kafka(
            topic="fleet_telemetry",
            key=payload.vehicle_id, # Partitioning Key
            payload_dict=data_to_send
        )
        # ---------------------------
        
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
        print("\nSimulation terminated by user. Flushing producer buffers...")
        producer.flush() # Forces outstanding messages to be delivered before shutting down

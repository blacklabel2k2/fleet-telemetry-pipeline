import json
from confluent_kafka import Producer

# 1. Configuration dictionary pointing to your local Docker Kafka container
kafka_config = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'fleet-telemetry-producer',
    # Enable high-throughput/reliability settings
    'acks': 'all',                  # Ensure data is fully acknowledged by the broker
    'retries': 5,                   # Retry if transmission drops temporarily
}

# Initialize the global Kafka producer instance
producer = Producer(kafka_config)

# 2. Callback function to verify message transmission success/failure
def delivery_report(err, msg):
    """Triggered once for each message to report delivery result."""
    if err is not None:
        print(f"❌ Message delivery failed: {err}")
    else:
        print(f"✅ Partition Bound: {msg.partition()} | Offset: {msg.offset()}")

# 3. Dedicated transmission function
def send_telemetry_to_kafka(topic: str, key: str, payload_dict: dict):
    """Serializes and sends a dictionary payload to a specific Kafka topic."""
    try:
        # Serialize the dictionary back to a JSON string
        value_bytes = json.dumps(payload_dict).encode('utf-8')
        
        # Produce message asynchronously
        # CRITICAL: We use 'key=key' (vehicle_id). This guarantees that logs
        # from the exact same truck always land in the same partition!
        producer.produce(
            topic=topic,
            key=key.encode('utf-8'),
            value=value_bytes,
            callback=delivery_report
        )
        
        # Serve delivery callbacks from the background queue
        producer.poll(0)
    except Exception as e:
        print(f"Internal Producer Exception: {e}")
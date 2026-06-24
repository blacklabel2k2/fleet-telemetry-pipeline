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
    try:
        # Convert key securely to a string and strip any invisible whitespaces
        string_key = str(key).strip()
        
        # DEBUG PRINT: Verify what key is being passed
        print(f"DEBUG: Sending message with Key: '{string_key}'")
        
        value_bytes = json.dumps(payload_dict).encode('utf-8')
        
        producer.produce(
            topic=topic,
            key=string_key.encode('utf-8'), # Explicitly passing encoded string bytes
            value=value_bytes,
            callback=delivery_report
        )
        producer.poll(0)
    except Exception as e:
        print(f"Internal Producer Exception: {e}")
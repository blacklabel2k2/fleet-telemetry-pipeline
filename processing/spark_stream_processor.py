from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import from_json, col
import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# Define the precise schema layout matching the simulator's JSON payload
# Define the precise schema layout matching the simulator's actual JSON payload
TELEMETRY_SCHEMA = StructType([
    StructField("vehicle_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("engine_temperature", DoubleType(), True),
    StructField("vibration_amplitude", DoubleType(), True), # Fixed name
    StructField("fuel_flow_rate", DoubleType(), True),       # Added
    StructField("gps_coordinates", StringType(), True)       # Read dict as raw string for now
])

# This explicitly instructs the underlying PySpark submission process to load the exact jar drivers required
os.environ['PYSPARK_SUBMIT_ARGS'] = (
    '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,'
    'io.delta:delta-spark_2.12:3.2.0 pyspark-shell'
)    
def create_spark_session() -> SparkSession:
    """Initializes a local Spark Session pre-configured to connect to Kafka and write to Delta Lake."""
    
    # Define the required coordinate packages for Kafka and Delta Lake matching Spark 3.5.0
    # These packages tell Spark how to talk to Kafka and how to write Delta storage format files
    packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "io.delta:delta-spark_2.12:3.2.0"
    ]
    
    builder = (
        SparkSession.builder
        .appName("FleetTelemetryProcessor")
        # Run Spark locally utilizing all available CPU cores on your machine
        .master("local[*]") 
        .config("spark.jars.packages", ",".join(packages))
        # Configure Spark SQL extensions to support Delta open-source ACID transactions
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Optimize memory configurations for local execution
        .config("spark.sql.shuffle.partitions", "4") # Match our Kafka partition count
    )
    
    # Wrap the builder configuration with Delta capabilities
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    
    # Adjust log levels so your terminal isn't overwhelmed with diagnostic info
    spark.sparkContext.setLogLevel("WARN")
    
    print("🚀 Apache Spark Session initialized successfully with Kafka and Delta extensions!")
    return spark

from pyspark.sql.functions import col, expr

def read_kafka_stream(spark: SparkSession, topic: str) -> any:
    """Opens a continuous streaming connection to the local Kafka broker."""
    print(f"📡 Establishing connection to Kafka topic: '{topic}'...")
    
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", topic)
        # Start reading from the earliest available message in the topic
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )
def write_to_bronze(df, checkpoint_path: str, storage_path: str):
    """Writes the raw streaming data into an append-only Delta Lake table."""
    print(f"💾 Initializing Bronze Layer stream writer at: {storage_path}")
    
    return (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .start(storage_path)
    )

def transform_bronze_to_silver(bronze_df):
    """Parses raw JSON, casts data types, applies watermarking, and removes duplicate entries."""
    print("✨ Applying Silver Layer Transformations (Parsing, Watermarking, Deduplication)...")
    
    return (
        bronze_df
        # 1. Convert the raw binary 'value' column from Kafka into a readable string
        .withColumn("json_string", col("value").cast(StringType()))
        
        # 2. Parse the JSON string into individual columns using our schema blueprint
        .withColumn("parsed_data", from_json(col("json_string"), TELEMETRY_SCHEMA))
        
        # 3. Pull the nested columns out to the top level and cast timestamps correctly
        .select(
            col("parsed_data.vehicle_id").alias("vehicle_id"),
            col("parsed_data.timestamp").cast(TimestampType()).alias("event_timestamp"),
            col("parsed_data.engine_temperature").cast(DoubleType()).alias("engine_temperature"),
            col("parsed_data.vibration_amplitude").cast(DoubleType()).alias("vibration_amplitude"),
            col("parsed_data.fuel_flow_rate").cast(DoubleType()).alias("fuel_flow_rate"),
            col("parsed_data.gps_coordinates").alias("gps_coordinates"),
            col("timestamp").alias("kafka_ingest_timestamp") 
        )
        
        # 4. Set a 10-minute watermark on our event time column
        .withWatermark("event_timestamp", "10 minutes")
        
        # 5. Deduplicate records matching on the same truck and exact timestamp
        .dropDuplicates(["vehicle_id", "event_timestamp"])
    )
def write_to_silver(silver_df, checkpoint_path: str, storage_path: str):
    """Writes the transformed silver stream into an independent Delta Lake table."""
    print(f"💾 Initializing Silver Layer stream writer at: {storage_path}")
    
    return (
        silver_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .start(storage_path)
    )

if __name__ == "__main__":
    # 1. Initialize the computing engine
    spark_session = create_spark_session()
    
    # Define local project paths for our Lakehouse storage layers
    base_storage = "storage"
    
    # Bronze Configuration
    bronze_path = f"{base_storage}/bronze_fleet_telemetry"
    bronze_checkpoint = f"{base_storage}/checkpoints/bronze"
    
    # Silver Configuration
    silver_path = f"{base_storage}/silver_fleet_telemetry"
    silver_checkpoint = f"{base_storage}/checkpoints/silver"
    
    try:
        # 2. Start consuming the live Kafka stream
        raw_telemetry_stream = read_kafka_stream(spark_session, "fleet_telemetry")
        
        # 3. Direct the stream to save out continuously into the Bronze Delta folder
        bronze_query = write_to_bronze(
            df=raw_telemetry_stream,
            checkpoint_path=bronze_checkpoint,
            storage_path=bronze_path
        )
        
        # 4. Pass the raw stream through our transformation filters
        transformed_silver_df = transform_bronze_to_silver(raw_telemetry_stream)
        
        # 5. Direct the transformed stream to save out to the Silver Delta folder
        silver_query = write_to_silver(
            silver_df=transformed_silver_df,
            checkpoint_path=silver_checkpoint,
            storage_path=silver_path
        )
        
        # 6. Keep all background queries active together until manually stopped
        print("⚡ Dual-Stream Active: Processing Bronze (Raw) & Silver (Cleaned) concurrently...")
        spark_session.streams.awaitAnyTermination()
        
    except KeyboardInterrupt:
        print("\nStopping Spark Streaming queries cleanly...")
    finally:
        spark_session.stop()
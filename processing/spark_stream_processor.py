import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

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
        .option("startingOffsets", "earliest") 
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

if __name__ == "__main__":
    # 1. Initialize the computing engine
    spark_session = create_spark_session()
    
    # Define local project paths for our Lakehouse storage layers
    base_storage = "storage"
    bronze_path = f"{base_storage}/bronze_fleet_telemetry"
    bronze_checkpoint = f"{base_storage}/checkpoints/bronze"
    
    try:
        # 2. Start consuming the live Kafka stream
        raw_telemetry_stream = read_kafka_stream(spark_session, "fleet_telemetry")
        
        # 3. Direct the stream to save out continuously into the Bronze Delta folder
        bronze_query = write_to_bronze(
            df=raw_telemetry_stream,
            checkpoint_path=bronze_checkpoint,
            storage_path=bronze_path
        )
        
        # 4. Keep the streaming query active until the user manually stops it
        print("⚡ Stream processing active. Awaiting real-time telemetry inputs...")
        bronze_query.awaitTermination()
        
    except KeyboardInterrupt:
        print("\nStopping Spark Streaming queries cleanly...")
    finally:
        spark_session.stop()
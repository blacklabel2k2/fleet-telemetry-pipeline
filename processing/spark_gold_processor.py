import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, avg, stddev, max as spark_max
from pyspark.sql.types import StringType
from delta import configure_spark_with_delta_pip

# Load the exact jar drivers required for Spark to handle Delta Lake locally
os.environ['PYSPARK_SUBMIT_ARGS'] = (
    '--packages io.delta:delta-spark_2.12:3.2.0 pyspark-shell'
)

def create_spark_session() -> SparkSession:
    """Initializes a local Spark Session configured to read and write Delta Lake tables."""
    builder = (
        SparkSession.builder
        .appName("FleetGoldFeatureAggregator")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4") 
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print("🚀 Spark Session initialized for Gold Aggregations!")
    return spark

if __name__ == "__main__":
    spark = create_spark_session()
    
    # 1. Read the live, streaming Silver Delta table as a source stream
    print("📖 Reading clean stream from Silver Medallion Layer...")
    silver_stream = (
        spark.readStream
        .format("delta")
        .load("storage/silver_fleet_telemetry")
    )
    
    # 2. Apply a 10-minute watermark and aggregate data into 5-minute Tumbling Windows
    print("📊 Constructing 5-minute tumbling windows for predictive maintenance features...")
    gold_features_df = (
        silver_stream
        .withWatermark("event_timestamp", "10 minutes")
        .groupBy(
            window(col("event_timestamp"), "5 minutes"),
            col("vehicle_id")
        )
        .agg(
            avg("engine_temperature").alias("avg_engine_temp"),
            stddev("vibration_amplitude").alias("vibration_anomaly_score"),
            avg("fuel_flow_rate").alias("avg_fuel_flow_rate"),
            spark_max("vibration_amplitude").alias("peak_vibration")
        )
        # Flatten out the window struct columns for easy downstream AI querying
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("vehicle_id"),
            col("avg_engine_temp"),
            col("vibration_anomaly_score"),
            col("avg_fuel_flow_rate"),
            col("peak_vibration")
        )
    )
    
    # 3. Write the live aggregates out using Complete Output Mode
    print("💾 Initializing Gold Layer stream writer at: storage/gold_fault_features")
    query = (
        gold_features_df.writeStream
        .format("delta")
        .outputMode("complete") # Complete mode rewrites the aggregated summaries continuously
        .option("checkpointLocation", "storage/checkpoints/gold")
        .start("storage/gold_fault_features")
    )
    
    try:
        print("⚡ Gold Aggregation Layer Active. Generating rolling asset features...")
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\nStopping Gold Streaming query cleanly...")
    finally:
        spark.stop()
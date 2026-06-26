from processing.spark_stream_processor import create_spark_session

# 1. Start a fresh, static session to inspect the files
spark = create_spark_session()

# 2. Read from our newly generated Silver Delta directory
silver_df = spark.read.format("delta").load("storage/silver_fleet_telemetry")

# 3. Print the schema to verify data types are correctly cast
print("\n📋 --- SILVER TABLE SCHEMA ---")
silver_df.printSchema()

# 4. Print the first 5 rows to see your data cleanly formatted
print("\n🚗 --- SILVER TABLE SAMPLE RECORDS ---")
silver_df.show(5, truncate=False)

spark.stop()
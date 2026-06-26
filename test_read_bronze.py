from processing.spark_stream_processor import create_spark_session

# 1. Start a fresh session to read the static storage files
spark = create_spark_session()

# 2. Read from our newly generated Bronze Delta directory
df = spark.read.format("delta").load("storage/bronze_fleet_telemetry")

# 3. Print the schema and the first 5 records to confirm data integrity
df.printSchema()
df.show(5, truncate=False)

spark.stop()
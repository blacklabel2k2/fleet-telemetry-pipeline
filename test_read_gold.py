from processing.spark_gold_processor import create_spark_session

spark = create_spark_session()
gold_df = spark.read.format("delta").load("storage/gold_fault_features")

print("\n🏆 --- GOLD TABLE FEATURES SCHEMA ---")
gold_df.printSchema()

print("\n🎯 --- LIVE ROLLING AI FEATURES ---")
gold_df.orderBy("window_start", ascending=False).show(10, truncate=False)

spark.stop()
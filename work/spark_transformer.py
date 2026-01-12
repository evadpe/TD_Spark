import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, to_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# ------------------ SPARK SESSION --------------------
spark = SparkSession.builder \
    .appName("WeatherTransformationWithLocation") \
    .master("local[*]") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ------------------ KAFKA INPUT SCHEMA --------------------
# Schema enrichi avec city/country pour l'exercice 6+
schema = StructType([
    StructField("temperature", DoubleType()),
    StructField("windspeed", DoubleType()),
    StructField("winddirection", DoubleType()),
    StructField("weathercode", DoubleType()),
    StructField("time", StringType()),
    StructField("city", StringType()),
    StructField("country", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType())
])

# ------------------ READ FROM KAFKA --------------------
weather_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "weather_stream") \
    .option("startingOffsets", "latest") \
    .load()

weather_df = weather_stream.selectExpr("CAST(value AS STRING)")

# Parse JSON
weather_json = weather_df.select(from_json(col("value"), schema).alias("data")).select("data.*")

# ------------------ ALERT RULES --------------------

# Wind Alerts
wind_alert = when(col("windspeed") < 10, lit("level_0")) \
    .when((col("windspeed") >= 10) & (col("windspeed") < 20), lit("level_1")) \
    .otherwise(lit("level_2"))

# Heat Alerts
heat_alert = when(col("temperature") < 25, lit("level_0")) \
    .when((col("temperature") >= 25) & (col("temperature") < 35), lit("level_1")) \
    .otherwise(lit("level_2"))

# Final transformation
transformed_weather = weather_json \
    .withColumn("event_time", to_timestamp(col("time"))) \
    .withColumn("wind_alert_level", wind_alert) \
    .withColumn("heat_alert_level", heat_alert)

# ------------------ WRITE TO KAFKA --------------------
output_df = transformed_weather.selectExpr("to_json(struct(*)) AS value")

query = output_df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "weather_transformed") \
    .option("checkpointLocation", "/tmp/spark_weather_checkpoint_v4/") \
    .outputMode("append") \
    .start()

# Console output pour debug (optionnel)
query_console = transformed_weather.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", "false") \
    .start()

print(" Spark Streaming started. Waiting for data...")
spark.streams.awaitAnyTermination()
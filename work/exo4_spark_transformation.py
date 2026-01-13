from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_json, struct, when, current_timestamp,
    get_json_object, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    IntegerType, TimestampType
)

def create_spark_session():
    """Crée une session Spark avec les packages Kafka"""
    return SparkSession.builder \
        .appName("WeatherAlertDetection") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1") \
        .getOrCreate()

def define_weather_schema():
    """Définit le schéma des messages météo"""
    return StructType([
        StructField("timestamp", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("windspeed", DoubleType(), True),
        StructField("wind_direction", DoubleType(), True),
        StructField("weather_code", IntegerType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("precipitation", DoubleType(), True)
    ])

def classify_wind_alert(windspeed):
    """Classifie le niveau d'alerte vent"""
    return when(col(windspeed) < 10, "level_0") \
        .when((col(windspeed) >= 10) & (col(windspeed) <= 20), "level_1") \
        .when(col(windspeed) > 20, "level_2") \
        .otherwise("unknown")

def classify_heat_alert(temperature):
    """Classifie le niveau d'alerte chaleur"""
    return when(col(temperature) < 25, "level_0") \
        .when((col(temperature) >= 25) & (col(temperature) <= 35), "level_1") \
        .when(col(temperature) > 35, "level_2") \
        .otherwise("unknown")

def process_weather_stream(spark):
    """Traite le flux weather_stream et produit weather_transformed"""
    
    print("=== Exercice 4: Transformation Spark Streaming ===")
    print("Lecture depuis: weather_stream")
    print("Écriture vers: weather_transformed")
    print("Détection d'alertes vent et chaleur en cours...\n")
    
    # Schéma des données météo
    weather_schema = define_weather_schema()
    
    # Lecture du stream Kafka
    df_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "weather_stream") \
        .option("startingOffsets", "latest") \
        .load()
    
    # Conversion de la valeur JSON
    df_parsed = df_stream.select(
        from_json(col("value").cast("string"), weather_schema).alias("data"),
        col("timestamp").alias("kafka_timestamp")
    ).select("data.*", "kafka_timestamp")
    
    # Transformation et ajout des alertes
    df_transformed = df_parsed.select(
        current_timestamp().alias("event_time"),
        col("timestamp").alias("original_timestamp"),
        col("city"),
        col("country"),
        col("latitude"),
        col("longitude"),
        col("temperature"),
        col("windspeed"),
        col("wind_direction"),
        col("weather_code"),
        col("humidity"),
        col("precipitation"),
        classify_wind_alert("windspeed").alias("wind_alert_level"),
        classify_heat_alert("temperature").alias("heat_alert_level")
    )
    
    # Conversion en JSON pour Kafka
    df_output = df_transformed.select(
        to_json(struct("*")).alias("value")
    )
    
    # Écriture vers Kafka
    query = df_output \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("topic", "weather_transformed") \
        .option("checkpointLocation", "/tmp/checkpoint_weather_transformed") \
        .outputMode("append") \
        .start()
    
    # Affichage en console pour debug
    query_console = df_transformed \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .start()
    
    return query, query_console

if __name__ == "__main__":
    # Créer la session Spark
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Lancer le traitement
        query_kafka, query_console = process_weather_stream(spark)
        
        print("✓ Streaming démarré avec succès!")
        print("Les transformations sont appliquées en temps réel...")
        print("Appuyez sur Ctrl+C pour arrêter\n")
        
        # Attendre l'arrêt
        query_kafka.awaitTermination()
        
    except KeyboardInterrupt:
        print("\n\nArrêt du streaming...")
    except Exception as e:
        print(f"Erreur: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()
        print("Session Spark fermée.")
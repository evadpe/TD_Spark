from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, count, avg, min, max, sum,
    when, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    IntegerType, TimestampType
)

def create_spark_session():
    """Crée une session Spark avec les packages Kafka"""
    return SparkSession.builder \
        .appName("WeatherAggregates") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint_aggregates") \
        .getOrCreate()

def define_transformed_schema():
    """Définit le schéma des messages weather_transformed"""
    return StructType([
        StructField("event_time", StringType(), True),
        StructField("original_timestamp", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("windspeed", DoubleType(), True),
        StructField("wind_direction", DoubleType(), True),
        StructField("weather_code", IntegerType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("precipitation", DoubleType(), True),
        StructField("wind_alert_level", StringType(), True),
        StructField("heat_alert_level", StringType(), True)
    ])

def calculate_aggregates(spark, window_duration="5 minutes", slide_duration="1 minute"):
    """
    Calcule des agrégats sur fenêtres glissantes
    
    Args:
        spark: SparkSession
        window_duration: Taille de la fenêtre (ex: "5 minutes", "1 minute")
        slide_duration: Intervalle de glissement (ex: "1 minute")
    """
    
    print(f"=== Exercice 5: Agrégats en Temps Réel ===")
    print(f"Fenêtre: {window_duration}")
    print(f"Glissement: {slide_duration}")
    print("Lecture depuis: weather_transformed")
    print("Calcul d'agrégats en cours...\n")
    
    # Schéma des données
    schema = define_transformed_schema()
    
    # Lecture du stream Kafka
    df_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "weather_transformed") \
        .option("startingOffsets", "latest") \
        .load()
    
    # Parse JSON et conversion du timestamp
    df_parsed = df_stream.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    
    # Conversion de event_time en timestamp
    df_with_timestamp = df_parsed.withColumn(
        "event_timestamp",
        col("event_time").cast(TimestampType())
    )
    
    # === Agrégat 1: Statistiques globales par fenêtre ===
    df_global_stats = df_with_timestamp \
        .withWatermark("event_timestamp", "10 minutes") \
        .groupBy(
            window(col("event_timestamp"), window_duration, slide_duration)
        ) \
        .agg(
            count("*").alias("total_messages"),
            avg("temperature").alias("avg_temperature"),
            min("temperature").alias("min_temperature"),
            max("temperature").alias("max_temperature"),
            avg("windspeed").alias("avg_windspeed"),
            max("windspeed").alias("max_windspeed")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("total_messages"),
            col("avg_temperature"),
            col("min_temperature"),
            col("max_temperature"),
            col("avg_windspeed"),
            col("max_windspeed")
        )
    
    # === Agrégat 2: Nombre d'alertes par niveau et type ===
    df_alerts = df_with_timestamp \
        .withWatermark("event_timestamp", "10 minutes") \
        .groupBy(
            window(col("event_timestamp"), window_duration, slide_duration),
            col("wind_alert_level"),
            col("heat_alert_level")
        ) \
        .agg(
            count("*").alias("count")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("wind_alert_level"),
            col("heat_alert_level"),
            col("count")
        )
    
    # === Agrégat 3: Statistiques par ville ===
    df_city_stats = df_with_timestamp \
        .withWatermark("event_timestamp", "10 minutes") \
        .groupBy(
            window(col("event_timestamp"), window_duration, slide_duration),
            col("city"),
            col("country")
        ) \
        .agg(
            count("*").alias("message_count"),
            avg("temperature").alias("avg_temp"),
            avg("windspeed").alias("avg_wind"),
            sum(when(col("heat_alert_level") != "level_0", 1).otherwise(0)).alias("heat_alerts"),
            sum(when(col("wind_alert_level") != "level_0", 1).otherwise(0)).alias("wind_alerts")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("city"),
            col("country"),
            col("message_count"),
            col("avg_temp"),
            col("avg_wind"),
            col("heat_alerts"),
            col("wind_alerts")
        )
    
    # === Agrégat 4: Alertes critiques (level_1 et level_2) ===
    df_critical_alerts = df_with_timestamp \
        .filter(
            (col("wind_alert_level").isin(["level_1", "level_2"])) |
            (col("heat_alert_level").isin(["level_1", "level_2"]))
        ) \
        .withWatermark("event_timestamp", "10 minutes") \
        .groupBy(
            window(col("event_timestamp"), window_duration, slide_duration),
            col("city"),
            col("country")
        ) \
        .agg(
            sum(when(col("wind_alert_level") == "level_1", 1).otherwise(0)).alias("wind_level_1"),
            sum(when(col("wind_alert_level") == "level_2", 1).otherwise(0)).alias("wind_level_2"),
            sum(when(col("heat_alert_level") == "level_1", 1).otherwise(0)).alias("heat_level_1"),
            sum(when(col("heat_alert_level") == "level_2", 1).otherwise(0)).alias("heat_level_2")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("city"),
            col("country"),
            col("wind_level_1"),
            col("wind_level_2"),
            col("heat_level_1"),
            col("heat_level_2")
        )
    
    return df_global_stats, df_alerts, df_city_stats, df_critical_alerts

def start_queries(df_global_stats, df_alerts, df_city_stats, df_critical_alerts):
    """Démarre les requêtes de streaming avec affichage console"""
    
    # Query 1: Statistiques globales
    query_global = df_global_stats \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .queryName("GlobalStats") \
        .start()
    
    # Query 2: Distribution des alertes
    query_alerts = df_alerts \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .queryName("AlertDistribution") \
        .start()
    
    # Query 3: Statistiques par ville
    query_city = df_city_stats \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .queryName("CityStats") \
        .start()
    
    # Query 4: Alertes critiques
    query_critical = df_critical_alerts \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .queryName("CriticalAlerts") \
        .start()
    
    return query_global, query_alerts, query_city, query_critical

if __name__ == "__main__":
    import sys
    
    # Paramètres de fenêtre
    window_duration = "5 minutes"  # Taille de la fenêtre
    slide_duration = "1 minute"    # Intervalle de glissement
    
    # Parser les arguments optionnels
    if len(sys.argv) > 1:
        window_duration = sys.argv[1]
    if len(sys.argv) > 2:
        slide_duration = sys.argv[2]
    
    # Créer la session Spark
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Calculer les agrégats
        df_global, df_alerts, df_city, df_critical = calculate_aggregates(
            spark, window_duration, slide_duration
        )
        
        # Démarrer les requêtes
        queries = start_queries(df_global, df_alerts, df_city, df_critical)
        
        print("✓ Agrégations démarrées avec succès!")
        print(f"Fenêtre: {window_duration}, Glissement: {slide_duration}")
        print("\nAffichage des 4 types d'agrégats:")
        print("1. Statistiques globales (température, vent)")
        print("2. Distribution des alertes par niveau")
        print("3. Statistiques par ville")
        print("4. Alertes critiques (level_1 et level_2)")
        print("\nAppuyez sur Ctrl+C pour arrêter\n")
        
        # Attendre l'arrêt
        for query in queries:
            query.awaitTermination()
        
    except KeyboardInterrupt:
        print("\n\nArrêt des agrégations...")
    except Exception as e:
        print(f"Erreur: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()
        print("Session Spark fermée.")
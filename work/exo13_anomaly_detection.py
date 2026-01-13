from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_json, struct, when, current_timestamp, 
    month, lit, abs as spark_abs
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    IntegerType, TimestampType
)
import json

def create_spark_session():
    """Crée une session Spark avec Kafka"""
    return SparkSession.builder \
        .appName("AnomalyDetection") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1") \
        .config("spark.sql.streaming.checkpointLocation", 
                "/tmp/checkpoint_anomalies") \
        .getOrCreate()

def load_seasonal_profiles(city, country):
    """Charge les profils saisonniers enrichis"""
    
    file_path = f"/home/jovyan/work/hdfs-data/{country}/{city}/seasonal_profile_enriched/profile.json"
    
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        # Créer un dictionnaire mois -> profil
        profiles = {}
        for profile in data['profiles']:
            profiles[profile['month']] = profile
        
        return profiles
    except Exception as e:
        print(f"Erreur chargement profil pour {city}: {e}")
        return None

def define_transformed_schema():
    """Schéma des messages weather_transformed"""
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

def detect_anomalies_stream(spark):
    """
    Détecte les anomalies en temps réel en comparant avec les profils historiques
    """
    
    print("=== Exercice 13: Détection d'Anomalies Climatiques ===")
    print("Lecture depuis: weather_transformed")
    print("Publication vers: weather_anomalies")
    print("Lambda Architecture: Speed Layer (temps réel) vs Batch Layer (historique)")
    print("\n⚙️  Chargement des profils saisonniers...")
    
    # Charger les profils pour toutes les villes
    # On va créer un broadcast pour les profils
    cities_profiles = {
        'Paris': load_seasonal_profiles('Paris', 'France'),
        'Marseille': load_seasonal_profiles('Marseille', 'France'),
        'Lyon': load_seasonal_profiles('Lyon', 'France')
    }
    
    # Filtrer les villes avec profils valides
    valid_cities = {k: v for k, v in cities_profiles.items() if v is not None}
    
    if not valid_cities:
        print("  Aucun profil saisonnier trouvé. Exécutez d'abord les exercices 11 et 12.")
        return None
    
    print(f"✓ {len(valid_cities)} profils chargés: {', '.join(valid_cities.keys())}")
    
    # Broadcast des profils
    profiles_broadcast = spark.sparkContext.broadcast(valid_cities)
    
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
    
    # Parse JSON
    df_parsed = df_stream.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    
    # Conversion timestamp et extraction du mois
    df_with_time = df_parsed.withColumn(
        "event_timestamp",
        col("event_time").cast(TimestampType())
    )
    
    df_with_month = df_with_time.withColumn(
        "month",
        month(col("event_timestamp"))
    )
    
    # UDF pour détecter les anomalies
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StructType, StructField, BooleanType, StringType, DoubleType
    
    anomaly_schema = StructType([
        StructField("is_anomaly", BooleanType(), True),
        StructField("anomaly_type", StringType(), True),
        StructField("variable", StringType(), True),
        StructField("observed_value", DoubleType(), True),
        StructField("expected_value", DoubleType(), True),
        StructField("deviation", DoubleType(), True),
        StructField("threshold", DoubleType(), True)
    ])
    
    def detect_anomaly_func(city, month_num, temperature, windspeed):
        """Détecte si les valeurs sont anormales"""
        
        profiles = profiles_broadcast.value
        
        if city not in profiles:
            return {
                'is_anomaly': False,
                'anomaly_type': None,
                'variable': None,
                'observed_value': None,
                'expected_value': None,
                'deviation': None,
                'threshold': None
            }
        
        profile = profiles[city].get(month_num)
        
        if not profile:
            return {
                'is_anomaly': False,
                'anomaly_type': None,
                'variable': None,
                'observed_value': None,
                'expected_value': None,
                'deviation': None,
                'threshold': None
            }
        
        # Récupérer les statistiques historiques
        temp_mean = profile['temperature']['mean']
        temp_stddev = profile['temperature']['stddev']
        wind_mean = profile['wind']['mean']
        wind_stddev = profile['wind']['stddev']
        
        # Seuils d'anomalie : ±2 écarts-types (95% des valeurs normales)
        temp_threshold = 2 * temp_stddev
        wind_threshold = 2 * wind_stddev
        
        # Déviations
        temp_deviation = abs(temperature - temp_mean)
        wind_deviation = abs(windspeed - wind_mean)
        
        # Détection anomalie température
        if temp_deviation > temp_threshold:
            anomaly_type = "heat_wave" if temperature > temp_mean else "cold_wave"
            return {
                'is_anomaly': True,
                'anomaly_type': anomaly_type,
                'variable': 'temperature',
                'observed_value': float(temperature),
                'expected_value': float(temp_mean),
                'deviation': float(temp_deviation),
                'threshold': float(temp_threshold)
            }
        
        # Détection anomalie vent
        if wind_deviation > wind_threshold:
            return {
                'is_anomaly': True,
                'anomaly_type': 'wind_storm',
                'variable': 'windspeed',
                'observed_value': float(windspeed),
                'expected_value': float(wind_mean),
                'deviation': float(wind_deviation),
                'threshold': float(wind_threshold)
            }
        
        return {
            'is_anomaly': False,
            'anomaly_type': None,
            'variable': None,
            'observed_value': None,
            'expected_value': None,
            'deviation': None,
            'threshold': None
        }
    
    detect_anomaly_udf = udf(detect_anomaly_func, anomaly_schema)
    
    # Appliquer la détection
    df_with_anomaly = df_with_month.withColumn(
        "anomaly_info",
        detect_anomaly_udf(col("city"), col("month"), col("temperature"), col("windspeed"))
    )
    
    # Extraire les champs d'anomalie
    df_anomalies = df_with_anomaly.select(
        col("event_timestamp"),
        col("city"),
        col("country"),
        col("month"),
        col("temperature"),
        col("windspeed"),
        col("anomaly_info.is_anomaly").alias("is_anomaly"),
        col("anomaly_info.anomaly_type").alias("anomaly_type"),
        col("anomaly_info.variable").alias("variable"),
        col("anomaly_info.observed_value").alias("observed_value"),
        col("anomaly_info.expected_value").alias("expected_value"),
        col("anomaly_info.deviation").alias("deviation"),
        col("anomaly_info.threshold").alias("threshold")
    )
    
    # Filtrer uniquement les anomalies
    df_anomalies_only = df_anomalies.filter(col("is_anomaly") == True)
    
    # Conversion en JSON pour Kafka
    df_output = df_anomalies_only.select(
        to_json(struct("*")).alias("value")
    )
    
    return df_anomalies_only, df_output

def start_anomaly_detection(spark):
    """Lance la détection d'anomalies"""
    
    df_anomalies, df_output = detect_anomalies_stream(spark)
    
    if df_anomalies is None:
        return None, None
    
    # Query 1: Publication vers Kafka
    query_kafka = df_output \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("topic", "weather_anomalies") \
        .option("checkpointLocation", "/tmp/checkpoint_anomalies_kafka") \
        .outputMode("append") \
        .start()
    
    # Query 2: Affichage console
    query_console = df_anomalies \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .start()
    
    return query_kafka, query_console

if __name__ == "__main__":
    # Créer la session Spark
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Lancer la détection
        query_kafka, query_console = start_anomaly_detection(spark)
        
        if query_kafka and query_console:
            print("\n Détection d'anomalies démarrée avec succès!")
            print("\n Les anomalies détectées s'afficheront ci-dessous...")
            print(" Anomalie = valeur s'écartant de >2σ de la moyenne historique")
            print("\nAppuyez sur Ctrl+C pour arrêter\n")
            
            # Attendre
            query_kafka.awaitTermination()
        
    except KeyboardInterrupt:
        print("\n\n  Arrêt de la détection d'anomalies...")
    except Exception as e:
        print(f"  Erreur: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()
        print("Session Spark fermée.")
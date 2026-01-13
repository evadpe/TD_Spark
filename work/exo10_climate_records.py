from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max, min as spark_min, sum as spark_sum, count, lit, struct, to_json, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, IntegerType
from kafka import KafkaProducer
import json
from datetime import datetime

def create_spark_session():
    """Crée une session Spark"""
    return SparkSession.builder \
        .appName("ClimateRecords") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

def load_historical_data(spark, city, country):
    """Charge les données historiques depuis le fichier local"""
    
    file_path = f"/home/jovyan/work/hdfs-data/{country}/{city}/weather_history_raw.json"
    
    schema = StructType([
        StructField("date", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("temp_max", DoubleType(), True),
        StructField("temp_min", DoubleType(), True),
        StructField("temp_mean", DoubleType(), True),
        StructField("precipitation", DoubleType(), True),
        StructField("rain", DoubleType(), True),
        StructField("snowfall", DoubleType(), True),
        StructField("windspeed_max", DoubleType(), True),
        StructField("windgusts_max", DoubleType(), True),
        StructField("weather_code", IntegerType(), True)
    ])
    
    df = spark.read.schema(schema).json(file_path)
    df = df.withColumn("date", col("date").cast(DateType()))
    
    return df

def find_records(df):
    """Trouve les records climatiques"""
    
    # 1. Jour le plus chaud
    hottest = df.orderBy(col("temp_max").desc()).first()
    
    # 2. Jour le plus froid
    coldest = df.orderBy(col("temp_min").asc()).first()
    
    # 3. Rafale de vent la plus forte
    windiest = df.orderBy(col("windgusts_max").desc()).first()
    
    # 4. Période la plus pluvieuse (fenêtre glissante de 30 jours)
    from pyspark.sql.window import Window
    
    # Créer une fenêtre glissante de 30 jours
    windowSpec = Window.orderBy("date").rowsBetween(-29, 0)
    
    df_rain = df.withColumn("rain_30days", spark_sum("precipitation").over(windowSpec))
    rainiest = df_rain.orderBy(col("rain_30days").desc()).first()
    
    # 5. Journée la plus neigeuse
    snowiest = df.orderBy(col("snowfall").desc()).first()
    
    # 6. Statistiques globales
    stats = df.agg(
        spark_max("temp_max").alias("absolute_max_temp"),
        spark_min("temp_min").alias("absolute_min_temp"),
        spark_max("windgusts_max").alias("absolute_max_wind"),
        spark_sum("precipitation").alias("total_precipitation"),
        spark_sum("snowfall").alias("total_snowfall"),
        count("*").alias("total_days")
    ).first()
    
    return {
        'hottest_day': {
            'date': str(hottest['date']),
            'temp_max': float(hottest['temp_max']),
            'temp_min': float(hottest['temp_min']),
            'description': f"Jour le plus chaud: {hottest['temp_max']}°C"
        },
        'coldest_day': {
            'date': str(coldest['date']),
            'temp_max': float(coldest['temp_max']),
            'temp_min': float(coldest['temp_min']),
            'description': f"Jour le plus froid: {coldest['temp_min']}°C"
        },
        'windiest_day': {
            'date': str(windiest['date']),
            'windgusts_max': float(windiest['windgusts_max']),
            'windspeed_max': float(windiest['windspeed_max']),
            'description': f"Rafale la plus forte: {windiest['windgusts_max']} m/s"
        },
        'rainiest_period': {
            'date': str(rainiest['date']),
            'rain_30days': float(rainiest['rain_30days']),
            'description': f"Période la plus pluvieuse (30 jours): {rainiest['rain_30days']:.1f} mm"
        },
        'snowiest_day': {
            'date': str(snowiest['date']),
            'snowfall': float(snowiest['snowfall']),
            'description': f"Jour le plus neigeux: {snowiest['snowfall']} cm"
        },
        'statistics': {
            'absolute_max_temp': float(stats['absolute_max_temp']),
            'absolute_min_temp': float(stats['absolute_min_temp']),
            'absolute_max_wind': float(stats['absolute_max_wind']),
            'total_precipitation': float(stats['total_precipitation']),
            'total_snowfall': float(stats['total_snowfall']),
            'total_days': int(stats['total_days'])
        }
    }

def save_records_to_kafka(records, city, country, producer, topic='weather_records'):
    """Envoie les records vers Kafka"""
    
    message = {
        'timestamp': datetime.now().isoformat(),
        'city': city,
        'country': country,
        'records': records
    }
    
    producer.send(topic, value=message)
    producer.flush()
    
    print(f"  ✓ Records envoyés vers Kafka (topic: {topic})")

def save_records_locally(records, city, country):
    """Sauvegarde les records localement"""
    
    import os
    
    dir_path = f"/home/jovyan/work/hdfs-data/{country}/{city}"
    os.makedirs(dir_path, exist_ok=True)
    
    file_path = f"{dir_path}/weather_records.json"
    
    with open(file_path, 'w') as f:
        json.dump(records, f, indent=2)
    
    print(f"  ✓ Records sauvegardés: {file_path}")

def print_records(records, city, country):
    """Affiche les records de manière lisible"""
    
    print(f"\n{'='*80}")
    print(f"🏆 RECORDS CLIMATIQUES - {city}, {country}")
    print('='*80)
    
    print(f"\n🔥 {records['hottest_day']['description']}")
    print(f"   Date: {records['hottest_day']['date']}")
    print(f"   Température: {records['hottest_day']['temp_min']}°C à {records['hottest_day']['temp_max']}°C")
    
    print(f"\n❄️  {records['coldest_day']['description']}")
    print(f"   Date: {records['coldest_day']['date']}")
    print(f"   Température: {records['coldest_day']['temp_min']}°C à {records['coldest_day']['temp_max']}°C")
    
    print(f"\n💨 {records['windiest_day']['description']}")
    print(f"   Date: {records['windiest_day']['date']}")
    print(f"   Vent moyen max: {records['windiest_day']['windspeed_max']} m/s")
    
    print(f"\n🌧️  {records['rainiest_period']['description']}")
    print(f"   Date de fin de période: {records['rainiest_period']['date']}")
    
    print(f"\n❄️  {records['snowiest_day']['description']}")
    print(f"   Date: {records['snowiest_day']['date']}")
    
    print(f"\n📊 STATISTIQUES GLOBALES")
    print(f"   Période: {records['statistics']['total_days']} jours")
    print(f"   Température max absolue: {records['statistics']['absolute_max_temp']}°C")
    print(f"   Température min absolue: {records['statistics']['absolute_min_temp']}°C")
    print(f"   Rafale max absolue: {records['statistics']['absolute_max_wind']} m/s")
    print(f"   Précipitations totales: {records['statistics']['total_precipitation']:.1f} mm")
    print(f"   Chutes de neige totales: {records['statistics']['total_snowfall']:.1f} cm")
    
    print('='*80)

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 3:
        print("Usage: python exo10_climate_records.py <city> <country>")
        print("Exemple: python exo10_climate_records.py Paris France")
        sys.exit(1)
    
    city = sys.argv[1]
    country = sys.argv[2]
    
    print(f"=== Exercice 10: Détection des Records Climatiques ===")
    print(f"Ville: {city}, {country}")
    print(f"Analyse en cours...\n")
    
    # Créer la session Spark
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    
    try:
        # Charger les données
        print(" Chargement des données historiques...")
        df = load_historical_data(spark, city, country)
        
        total_records = df.count()
        print(f"  ✓ {total_records} jours chargés")
        
        # Trouver les records
        print("\n Recherche des records climatiques...")
        records = find_records(df)
        
        # Afficher les résultats
        print_records(records, city, country)
        
        # Sauvegarder localement
        print("\n Sauvegarde des résultats...")
        save_records_locally(records, city, country)
        
        # Envoyer vers Kafka
        producer = KafkaProducer(
            bootstrap_servers=['kafka:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        save_records_to_kafka(records, city, country, producer)
        producer.close()
        
        print("\n Analyse terminée avec succès!")
        
    except Exception as e:
        print(f"Erreur: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        spark.stop()
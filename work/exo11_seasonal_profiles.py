from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, month, count, when, stddev, min as spark_min, max as spark_max
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, IntegerType
import json
from datetime import datetime

def create_spark_session():
    """Crée une session Spark"""
    return SparkSession.builder \
        .appName("SeasonalProfiles") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

def load_historical_data(spark, city, country):
    """Charge les données historiques"""
    
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
    df = df.withColumn("month", month(col("date")))
    
    return df

def classify_alerts(temp_mean, windspeed_max):
    """Classifie les alertes selon les critères de l'exercice 4"""
    
    # Alerte chaleur
    heat_alert = when(temp_mean < 25, 0) \
        .when((temp_mean >= 25) & (temp_mean <= 35), 1) \
        .when(temp_mean > 35, 2) \
        .otherwise(0)
    
    # Alerte vent
    wind_alert = when(windspeed_max < 10, 0) \
        .when((windspeed_max >= 10) & (windspeed_max <= 20), 1) \
        .when(windspeed_max > 20, 2) \
        .otherwise(0)
    
    return heat_alert, wind_alert

def calculate_seasonal_profile(df):
    """
    Calcule le profil saisonnier : agrégation par mois
    
    Returns:
        DataFrame avec colonnes : month, temp_mean, wind_mean, heat_alert_prob, wind_alert_prob, etc.
    """
    
    # Ajouter les colonnes d'alertes
    heat_alert, wind_alert = classify_alerts(col("temp_mean"), col("windspeed_max"))
    
    df = df.withColumn("heat_alert_level", heat_alert)
    df = df.withColumn("wind_alert_level", wind_alert)
    
    # Agrégation par mois
    seasonal = df.groupBy("month").agg(
        # Température
        avg("temp_mean").alias("temp_mean"),
        avg("temp_max").alias("temp_max"),
        avg("temp_min").alias("temp_min"),
        stddev("temp_mean").alias("temp_stddev"),
        spark_min("temp_min").alias("temp_abs_min"),
        spark_max("temp_max").alias("temp_abs_max"),
        
        # Vent
        avg("windspeed_max").alias("wind_mean"),
        avg("windgusts_max").alias("windgust_mean"),
        stddev("windspeed_max").alias("wind_stddev"),
        spark_max("windgusts_max").alias("wind_abs_max"),
        
        # Précipitations
        avg("precipitation").alias("precipitation_mean"),
        avg("rain").alias("rain_mean"),
        avg("snowfall").alias("snowfall_mean"),
        
        # Probabilités d'alertes (proportion de jours avec alerte level_1 ou level_2)
        (count(when(col("heat_alert_level") >= 1, True)) / count("*")).alias("heat_alert_probability"),
        (count(when(col("wind_alert_level") >= 1, True)) / count("*")).alias("wind_alert_probability"),
        
        # Nombre de jours par mois
        count("*").alias("days_count")
    )
    
    # Trier par mois
    seasonal = seasonal.orderBy("month")
    
    return seasonal

def format_profile_for_save(seasonal_df, city, country):
    """Convertit le profil en format JSON pour sauvegarde"""
    
    profiles = []
    
    month_names = {
        1: "Janvier", 2: "Février", 3: "Mars", 4: "Avril",
        5: "Mai", 6: "Juin", 7: "Juillet", 8: "Août",
        9: "Septembre", 10: "Octobre", 11: "Novembre", 12: "Décembre"
    }
    
    for row in seasonal_df.collect():
        profile = {
            'month': int(row['month']),
            'month_name': month_names[int(row['month'])],
            'temperature': {
                'mean': round(float(row['temp_mean']), 2),
                'max': round(float(row['temp_max']), 2),
                'min': round(float(row['temp_min']), 2),
                'stddev': round(float(row['temp_stddev']), 2),
                'absolute_min': round(float(row['temp_abs_min']), 2),
                'absolute_max': round(float(row['temp_abs_max']), 2)
            },
            'wind': {
                'mean': round(float(row['wind_mean']), 2),
                'gust_mean': round(float(row['windgust_mean']), 2),
                'stddev': round(float(row['wind_stddev']), 2),
                'absolute_max': round(float(row['wind_abs_max']), 2)
            },
            'precipitation': {
                'mean': round(float(row['precipitation_mean']), 2),
                'rain_mean': round(float(row['rain_mean']), 2),
                'snowfall_mean': round(float(row['snowfall_mean']), 2)
            },
            'alerts': {
                'heat_probability': round(float(row['heat_alert_probability']), 4),
                'wind_probability': round(float(row['wind_alert_probability']), 4)
            },
            'days_count': int(row['days_count'])
        }
        profiles.append(profile)
    
    return {
        'city': city,
        'country': country,
        'generated_at': datetime.now().isoformat(),
        'profiles': profiles
    }

def save_profile_locally(profile_data, city, country):
    """Sauvegarde le profil saisonnier localement"""
    
    import os
    
    dir_path = f"/home/jovyan/work/hdfs-data/{country}/{city}/seasonal_profile"
    os.makedirs(dir_path, exist_ok=True)
    
    file_path = f"{dir_path}/profile.json"
    
    with open(file_path, 'w') as f:
        json.dump(profile_data, f, indent=2)
    
    print(f"  ✓ Profil sauvegardé: {file_path}")

def print_seasonal_profile(profile_data):
    """Affiche le profil saisonnier de manière lisible"""
    
    city = profile_data['city']
    country = profile_data['country']
    
    print(f"\n{'='*100}")
    print(f"PROFIL SAISONNIER - {city}, {country}")
    print('='*100)
    
    print(f"\n{'Mois':<12} {'Temp Moy':<10} {'Temp Min':<10} {'Temp Max':<10} {'Vent Moy':<10} {'P(Alerte Chaleur)':<20} {'P(Alerte Vent)':<15}")
    print('-'*100)
    
    for profile in profile_data['profiles']:
        month_name = profile['month_name']
        temp_mean = profile['temperature']['mean']
        temp_min = profile['temperature']['min']
        temp_max = profile['temperature']['max']
        wind_mean = profile['wind']['mean']
        heat_prob = profile['alerts']['heat_probability']
        wind_prob = profile['alerts']['wind_probability']
        
        print(f"{month_name:<12} {temp_mean:>6.1f}°C   {temp_min:>6.1f}°C   {temp_max:>6.1f}°C   {wind_mean:>6.1f} m/s  {heat_prob:>7.2%}              {wind_prob:>7.2%}")
    
    print('='*100)
    
    # Résumé annuel
    temps = [p['temperature']['mean'] for p in profile_data['profiles']]
    winds = [p['wind']['mean'] for p in profile_data['profiles']]
    
    print(f"\n  RÉSUMÉ ANNUEL")
    print(f"   Température annuelle moyenne: {sum(temps) / len(temps):.1f}°C")
    print(f"   Vent annuel moyen: {sum(winds) / len(winds):.1f} m/s")
    print(f"   Mois le plus chaud: {profile_data['profiles'][temps.index(max(temps))]['month_name']} ({max(temps):.1f}°C)")
    print(f"   Mois le plus froid: {profile_data['profiles'][temps.index(min(temps))]['month_name']} ({min(temps):.1f}°C)")
    print(f"   Mois le plus venteux: {profile_data['profiles'][winds.index(max(winds))]['month_name']} ({max(winds):.1f} m/s)")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 3:
        print("Usage: spark-submit exo11_seasonal_profiles.py <city> <country>")
        print("Exemple: spark-submit exo11_seasonal_profiles.py Paris France")
        sys.exit(1)
    
    city = sys.argv[1]
    country = sys.argv[2]
    
    print(f"=== Exercice 11: Profils Saisonniers ===")
    print(f"Ville: {city}, {country}")
    print(f"Calcul en cours...\n")
    
    # Créer la session Spark
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    
    try:
        # Charger les données
        print("Chargement des données historiques...")
        df = load_historical_data(spark, city, country)
        
        total_records = df.count()
        print(f"  ✓ {total_records} jours chargés")
        
        # Calculer le profil saisonnier
        print("\n  Calcul du profil saisonnier...")
        seasonal_df = calculate_seasonal_profile(df)
        
        # Formater pour sauvegarde
        profile_data = format_profile_for_save(seasonal_df, city, country)
        
        # Afficher les résultats
        print_seasonal_profile(profile_data)
        
        # Sauvegarder
        print("\n Sauvegarde du profil...")
        save_profile_locally(profile_data, city, country)
        
        print("\n Profil saisonnier créé avec succès!")
        
    except Exception as e:
        print(f"Erreur: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        spark.stop()
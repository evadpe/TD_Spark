from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, month, count, when, stddev, min as spark_min, max as spark_max, percentile_approx
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, IntegerType
import json
from datetime import datetime

def create_spark_session():
    """Crée une session Spark"""
    return SparkSession.builder \
        .appName("ProfileValidation") \
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

def validate_profile(df):
    """
    Valide le profil saisonnier
    
    Vérifications:
    1. Profil complet sur 12 mois
    2. Pas de valeurs manquantes
    3. Valeurs réalistes (température entre -50°C et +60°C, vent entre 0 et 60 m/s)
    """
    
    validation_report = {
        'is_valid': True,
        'errors': [],
        'warnings': []
    }
    
    # 1. Vérifier 12 mois
    months_present = df.select("month").distinct().count()
    
    if months_present < 12:
        validation_report['is_valid'] = False
        validation_report['errors'].append(f"Profil incomplet: seulement {months_present}/12 mois présents")
    
    # 2. Vérifier valeurs manquantes par mois
    monthly_nulls = df.groupBy("month").agg(
        count(when(col("temp_mean").isNull(), 1)).alias("null_temp"),
        count(when(col("windspeed_max").isNull(), 1)).alias("null_wind")
    ).collect()
    
    for row in monthly_nulls:
        month_num = row['month']
        if row['null_temp'] > 0:
            validation_report['warnings'].append(f"Mois {month_num}: {row['null_temp']} valeurs de température manquantes")
        if row['null_wind'] > 0:
            validation_report['warnings'].append(f"Mois {month_num}: {row['null_wind']} valeurs de vent manquantes")
    
    # 3. Vérifier valeurs réalistes
    unrealistic = df.filter(
        (col("temp_mean") < -50) | (col("temp_mean") > 60) |
        (col("windspeed_max") < 0) | (col("windspeed_max") > 60)
    ).count()
    
    if unrealistic > 0:
        validation_report['warnings'].append(f"{unrealistic} valeurs non réalistes détectées")
    
    return validation_report

def calculate_enriched_profile(df):
    """
    Calcule un profil enrichi avec statistiques de dispersion et quantiles
    """
    
    # Agrégation par mois avec statistiques enrichies
    enriched = df.groupBy("month").agg(
        # Température - statistiques de base
        avg("temp_mean").alias("temp_mean"),
        avg("temp_max").alias("temp_max"),
        avg("temp_min").alias("temp_min"),
        
        # Température - dispersion
        stddev("temp_mean").alias("temp_stddev"),
        spark_min("temp_min").alias("temp_abs_min"),
        spark_max("temp_max").alias("temp_abs_max"),
        
        # Température - médiane et quantiles
        percentile_approx("temp_mean", 0.5).alias("temp_median"),
        percentile_approx("temp_mean", 0.25).alias("temp_q25"),
        percentile_approx("temp_mean", 0.75).alias("temp_q75"),
        
        # Vent - statistiques de base
        avg("windspeed_max").alias("wind_mean"),
        avg("windgusts_max").alias("windgust_mean"),
        
        # Vent - dispersion
        stddev("windspeed_max").alias("wind_stddev"),
        spark_min("windspeed_max").alias("wind_abs_min"),
        spark_max("windgusts_max").alias("wind_abs_max"),
        
        # Vent - médiane et quantiles
        percentile_approx("windspeed_max", 0.5).alias("wind_median"),
        percentile_approx("windspeed_max", 0.25).alias("wind_q25"),
        percentile_approx("windspeed_max", 0.75).alias("wind_q75"),
        
        # Précipitations
        avg("precipitation").alias("precipitation_mean"),
        stddev("precipitation").alias("precipitation_stddev"),
        spark_max("precipitation").alias("precipitation_max"),
        
        # Nombre de jours
        count("*").alias("days_count")
    )
    
    enriched = enriched.orderBy("month")
    
    return enriched

def format_enriched_profile(enriched_df, city, country, validation_report, year=None):
    """Convertit le profil enrichi en format JSON"""
    
    profiles = []
    
    month_names = {
        1: "Janvier", 2: "Février", 3: "Mars", 4: "Avril",
        5: "Mai", 6: "Juin", 7: "Juillet", 8: "Août",
        9: "Septembre", 10: "Octobre", 11: "Novembre", 12: "Décembre"
    }
    
    for row in enriched_df.collect():
        profile = {
            'month': int(row['month']),
            'month_name': month_names[int(row['month'])],
            'temperature': {
                'mean': round(float(row['temp_mean']), 2),
                'median': round(float(row['temp_median']), 2),
                'stddev': round(float(row['temp_stddev']), 2),
                'min': round(float(row['temp_min']), 2),
                'max': round(float(row['temp_max']), 2),
                'absolute_min': round(float(row['temp_abs_min']), 2),
                'absolute_max': round(float(row['temp_abs_max']), 2),
                'q25': round(float(row['temp_q25']), 2),
                'q75': round(float(row['temp_q75']), 2),
                'iqr': round(float(row['temp_q75']) - float(row['temp_q25']), 2)
            },
            'wind': {
                'mean': round(float(row['wind_mean']), 2),
                'median': round(float(row['wind_median']), 2),
                'stddev': round(float(row['wind_stddev']), 2),
                'gust_mean': round(float(row['windgust_mean']), 2),
                'absolute_min': round(float(row['wind_abs_min']), 2),
                'absolute_max': round(float(row['wind_abs_max']), 2),
                'q25': round(float(row['wind_q25']), 2),
                'q75': round(float(row['wind_q75']), 2),
                'iqr': round(float(row['wind_q75']) - float(row['wind_q25']), 2)
            },
            'precipitation': {
                'mean': round(float(row['precipitation_mean']), 2),
                'stddev': round(float(row['precipitation_stddev']), 2),
                'max': round(float(row['precipitation_max']), 2)
            },
            'days_count': int(row['days_count'])
        }
        profiles.append(profile)
    
    return {
        'city': city,
        'country': country,
        'year': year,
        'generated_at': datetime.now().isoformat(),
        'validation': validation_report,
        'profiles': profiles
    }

def save_enriched_profile(profile_data, city, country, year=None):
    """Sauvegarde le profil enrichi"""
    
    import os
    
    if year:
        dir_path = f"/home/jovyan/work/hdfs-data/{country}/{city}/seasonal_profile_enriched/{year}"
    else:
        dir_path = f"/home/jovyan/work/hdfs-data/{country}/{city}/seasonal_profile_enriched"
    
    os.makedirs(dir_path, exist_ok=True)
    
    file_path = f"{dir_path}/profile.json"
    
    with open(file_path, 'w') as f:
        json.dump(profile_data, f, indent=2)
    
    print(f"  ✓ Profil enrichi sauvegardé: {file_path}")

def print_validation_report(validation_report):
    """Affiche le rapport de validation"""
    
    print(f"\n{'='*80}")
    print("  RAPPORT DE VALIDATION")
    print('='*80)
    
    if validation_report['is_valid']:
        print(" Profil valide")
    else:
        print(" Profil invalide")
    
    if validation_report['errors']:
        print("\n ERREURS:")
        for error in validation_report['errors']:
            print(f"   - {error}")
    
    if validation_report['warnings']:
        print("\n  AVERTISSEMENTS:")
        for warning in validation_report['warnings']:
            print(f"   - {warning}")
    
    if not validation_report['errors'] and not validation_report['warnings']:
        print("\n Aucun problème détecté")

def print_enriched_profile(profile_data):
    """Affiche le profil enrichi"""
    
    city = profile_data['city']
    country = profile_data['country']
    
    print(f"\n{'='*120}")
    print(f"  PROFIL SAISONNIER ENRICHI - {city}, {country}")
    print('='*120)
    
    print(f"\n{'Mois':<12} {'Temp Moy':<10} {'Temp Med':<10} {'Temp σ':<10} {'IQR Temp':<10} {'Vent Moy':<10} {'Vent Med':<10} {'Vent σ':<10} {'IQR Vent':<10}")
    print('-'*120)
    
    for profile in profile_data['profiles']:
        month = profile['month_name']
        temp_mean = profile['temperature']['mean']
        temp_median = profile['temperature']['median']
        temp_std = profile['temperature']['stddev']
        temp_iqr = profile['temperature']['iqr']
        wind_mean = profile['wind']['mean']
        wind_median = profile['wind']['median']
        wind_std = profile['wind']['stddev']
        wind_iqr = profile['wind']['iqr']
        
        print(f"{month:<12} {temp_mean:>6.1f}°C   {temp_median:>6.1f}°C   {temp_std:>6.1f}°C   {temp_iqr:>6.1f}°C   {wind_mean:>6.1f} m/s {wind_median:>6.1f} m/s {wind_std:>6.1f} m/s {wind_iqr:>6.1f} m/s")
    
    print('='*120)

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 3:
        print("Usage: spark-submit exo12_profile_validation.py <city> <country> [year]")
        print("Exemple: spark-submit exo12_profile_validation.py Paris France")
        print("Exemple: spark-submit exo12_profile_validation.py Paris France 2024")
        sys.exit(1)
    
    city = sys.argv[1]
    country = sys.argv[2]
    year = sys.argv[3] if len(sys.argv) > 3 else None
    
    print(f"=== Exercice 12: Validation et Enrichissement des Profils ===")
    print(f"Ville: {city}, {country}")
    if year:
        print(f"Année: {year}")
    print(f"Traitement en cours...\n")
    
    # Créer la session Spark
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    
    try:
        # Charger les données
        print("  Chargement des données historiques...")
        df = load_historical_data(spark, city, country)
        
        total_records = df.count()
        print(f" {total_records} jours chargés")
        
        # Validation
        print("\n  Validation du profil...")
        validation_report = validate_profile(df)
        print_validation_report(validation_report)
        
        # Calcul du profil enrichi
        print("\n  Calcul du profil enrichi avec statistiques de dispersion...")
        enriched_df = calculate_enriched_profile(df)
        
        # Formater
        profile_data = format_enriched_profile(enriched_df, city, country, validation_report, year)
        
        # Afficher
        print_enriched_profile(profile_data)
        
        # Sauvegarder
        print("\n  Sauvegarde du profil enrichi...")
        save_enriched_profile(profile_data, city, country, year)
        
        print("\n Validation et enrichissement terminés avec succès!")
        
    except Exception as e:
        print(f" Erreur: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        spark.stop()
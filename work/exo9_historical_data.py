import requests
import json
from datetime import datetime, timedelta
from kafka import KafkaProducer
import time
import argparse

def create_producer():
    """Crée un producteur Kafka"""
    return KafkaProducer(
        bootstrap_servers=['kafka:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def fetch_historical_weather(latitude, longitude, start_date, end_date):
    """
    Récupère les données météo historiques depuis l'API Open-Meteo Archive
    
    Args:
        latitude: Latitude
        longitude: Longitude
        start_date: Date de début (YYYY-MM-DD)
        end_date: Date de fin (YYYY-MM-DD)
    
    Returns:
        dict: Données météo historiques
    """
    url = "https://archive-api.open-meteo.com/v1/archive"
    
    params = {
        'latitude': latitude,
        'longitude': longitude,
        'start_date': start_date,
        'end_date': end_date,
        'daily': [
            'temperature_2m_max',
            'temperature_2m_min',
            'temperature_2m_mean',
            'precipitation_sum',
            'rain_sum',
            'snowfall_sum',
            'windspeed_10m_max',
            'windgusts_10m_max',
            'weathercode'
        ],
        'timezone': 'auto'
    }
    
    try:
        print(f" Requête API: {start_date} à {end_date}")
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"  Erreur API: {e}")
        return None

def fetch_10_years_data(city, country, latitude, longitude, end_year=2024):
    """
    Récupère 10 ans de données historiques par morceaux d'1 an
    
    Args:
        city: Nom de la ville
        country: Nom du pays
        latitude: Latitude
        longitude: Longitude
        end_year: Année de fin (par défaut 2024)
    
    Returns:
        list: Liste de tous les enregistrements journaliers
    """
    start_year = end_year - 10
    all_records = []
    
    print(f"\n{'='*80}")
    print(f"Récupération de 10 ans de données pour {city}, {country}")
    print(f"Période: {start_year} - {end_year}")
    print('='*80)
    
    # Récupérer année par année pour éviter les timeouts
    for year in range(start_year, end_year + 1):
        start_date = f"{year}-01-01"
        end_date = f"{year}-12-31"
        
        print(f"\n Année {year}:")
        data = fetch_historical_weather(latitude, longitude, start_date, end_date)
        
        if data and 'daily' in data:
            daily = data['daily']
            dates = daily['time']
            
            # Créer un enregistrement pour chaque jour
            for i in range(len(dates)):
                record = {
                    'date': dates[i],
                    'city': city,
                    'country': country,
                    'latitude': latitude,
                    'longitude': longitude,
                    'temp_max': daily['temperature_2m_max'][i],
                    'temp_min': daily['temperature_2m_min'][i],
                    'temp_mean': daily['temperature_2m_mean'][i],
                    'precipitation': daily['precipitation_sum'][i],
                    'rain': daily['rain_sum'][i],
                    'snowfall': daily['snowfall_sum'][i],
                    'windspeed_max': daily['windspeed_10m_max'][i],
                    'windgusts_max': daily['windgusts_10m_max'][i],
                    'weather_code': daily['weathercode'][i]
                }
                all_records.append(record)
            
            print(f"  ✓ {len(dates)} jours récupérés")
        else:
            print(f"  Échec pour {year}")
        
        # Pause pour éviter de surcharger l'API
        time.sleep(1)
    
    print(f"\n Total: {len(all_records)} jours de données récupérés")
    return all_records

def save_to_kafka_and_local(producer, records, city, country, topic='weather_history_raw'):
    """
    Sauvegarde les données dans Kafka et localement
    
    Args:
        producer: Producteur Kafka
        records: Liste des enregistrements
        city: Nom de la ville
        country: Nom du pays
        topic: Topic Kafka
    """
    print(f"\n Sauvegarde des données...")
    
    # Sauvegarder localement
    local_dir = f"/home/jovyan/work/hdfs-data/{country}/{city}"
    import os
    os.makedirs(local_dir, exist_ok=True)
    
    local_file = f"{local_dir}/weather_history_raw.json"
    
    with open(local_file, 'w') as f:
        for record in records:
            f.write(json.dumps(record) + '\n')
    
    print(f"  ✓ Sauvegardé localement: {local_file}")
    
    # Envoyer vers Kafka par batch de 100
    batch_size = 100
    sent_count = 0
    
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        
        for record in batch:
            producer.send(topic, value=record)
            sent_count += 1
        
        producer.flush()
        
        if (i + batch_size) % 1000 == 0:
            print(f"  {sent_count}/{len(records)} envoyés vers Kafka...")
    
    print(f"  ✓ {sent_count} enregistrements envoyés vers Kafka (topic: {topic})")

def save_to_hdfs(records, city, country):
    """Copie les données vers HDFS"""
    import subprocess
    
    local_file = f"/home/jovyan/work/hdfs-data/{country}/{city}/weather_history_raw.json"
    hdfs_path = f"/hdfs-data/{country}/{city}/weather_history_raw.json"
    
    try:
        # Créer le répertoire HDFS
        subprocess.run(
            ['docker', 'exec', 'namenode', 'hdfs', 'dfs', '-mkdir', '-p', 
             f"/hdfs-data/{country}/{city}"],
            capture_output=True
        )
        
        # Copier le fichier dans namenode
        temp_container_path = f"/tmp/weather_history_{city}.json"
        subprocess.run(
            ['docker', 'cp', local_file, f"namenode:{temp_container_path}"],
            check=True
        )
        
        # Copier vers HDFS
        subprocess.run(
            ['docker', 'exec', 'namenode', 'hdfs', 'dfs', '-put', 
             '-f', temp_container_path, hdfs_path],
            check=True
        )
        
        # Nettoyer
        subprocess.run(
            ['docker', 'exec', 'namenode', 'rm', temp_container_path],
            capture_output=True
        )
        
        print(f"  ✓ Données copiées vers HDFS: {hdfs_path}")
        return True
        
    except Exception as e:
        print(f"  Erreur copie HDFS: {e}")
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Récupère 10 ans de données météo historiques'
    )
    parser.add_argument('city', type=str, help='Nom de la ville')
    parser.add_argument('country', type=str, help='Nom du pays')
    parser.add_argument('latitude', type=float, help='Latitude')
    parser.add_argument('longitude', type=float, help='Longitude')
    parser.add_argument('--end-year', type=int, default=2024, 
                       help='Année de fin (défaut: 2024)')
    
    args = parser.parse_args()
    
    print("=== Exercice 9: Récupération de Données Historiques ===")
    
    # Récupérer les données
    records = fetch_10_years_data(
        args.city, 
        args.country, 
        args.latitude, 
        args.longitude,
        args.end_year
    )
    
    if records:
        # Créer le producteur Kafka
        producer = create_producer()
        
        try:
            # Sauvegarder
            save_to_kafka_and_local(producer, records, args.city, args.country)
            
            # Copier vers HDFS
            save_to_hdfs(records, args.city, args.country)
            
            print(f"\n{'='*80}")
            print(" Récupération et sauvegarde terminées avec succès!")
            print(f"   Ville: {args.city}, {args.country}")
            print(f"   Enregistrements: {len(records)}")
            print(f"   Période: {records[0]['date']} à {records[-1]['date']}")
            print('='*80)
            
        finally:
            producer.close()
    else:
        print(" Échec de la récupération des données")
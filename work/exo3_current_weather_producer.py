from kafka import KafkaProducer
import json
import time
import requests
import argparse
from datetime import datetime

def create_producer():
    """Crée et retourne un producteur Kafka"""
    return KafkaProducer(
        bootstrap_servers=['kafka:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def get_current_weather(latitude, longitude):
    """
    Récupère les données météo actuelles depuis l'API Open-Meteo
    
    Args:
        latitude: Latitude de la localisation
        longitude: Longitude de la localisation
    
    Returns:
        dict: Données météo au format JSON
    """
    url = "https://api.open-meteo.com/v1/forecast"
    
    params = {
        'latitude': latitude,
        'longitude': longitude,
        'current': [
            'temperature_2m',
            'relative_humidity_2m',
            'precipitation',
            'weather_code',
            'wind_speed_10m',
            'wind_direction_10m'
        ],
        'timezone': 'auto'
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Erreur lors de la requête API: {e}")
        return None

def produce_weather_data(producer, latitude, longitude, topic='weather_stream', interval=None):
    """
    Produit des données météo vers Kafka
    
    Args:
        producer: Instance du producteur Kafka
        latitude: Latitude
        longitude: Longitude
        topic: Nom du topic Kafka
        interval: Intervalle en secondes entre chaque envoi (None = une seule fois)
    """
    print(f"=== Exercice 3: Producteur Météo en Direct ===")
    print(f"Localisation: Lat={latitude}, Lon={longitude}")
    print(f"Topic: {topic}")
    
    if interval:
        print(f"Mode streaming: envoi toutes les {interval} secondes (Ctrl+C pour arrêter)")
    else:
        print("Mode single: envoi unique")
    print()
    
    message_count = 0
    
    try:
        while True:
            # Récupérer les données météo
            weather_data = get_current_weather(latitude, longitude)
            
            if weather_data:
                message_count += 1
                timestamp = datetime.now().isoformat()
                
                # Préparer le message
                message = {
                    'timestamp': timestamp,
                    'latitude': latitude,
                    'longitude': longitude,
                    'weather_data': weather_data
                }
                
                # Envoyer vers Kafka
                future = producer.send(topic, value=message)
                record_metadata = future.get(timeout=10)
                
                print(f"[{timestamp}] Message #{message_count} envoyé avec succès")
                print(f"  Température: {weather_data['current']['temperature_2m']} {weather_data['current_units']['temperature_2m']}")
                print(f"  Vitesse du vent: {weather_data['current']['wind_speed_10m']} {weather_data['current_units']['wind_speed_10m']}")
                print(f"  Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")
                print()
            else:
                print("Erreur lors de la récupération des données météo")
            
            # Si pas de streaming, on sort de la boucle
            if interval is None:
                break
            
            # Attendre avant le prochain envoi
            time.sleep(interval)
            
    except KeyboardInterrupt:
        print(f"\n\nArrêt du producteur...")
        print(f"Total de messages envoyés: {message_count}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Producteur Kafka pour données météo en temps réel'
    )
    parser.add_argument('latitude', type=float, 
                       help='Latitude de la localisation')
    parser.add_argument('longitude', type=float,
                       help='Longitude de la localisation')
    parser.add_argument('--topic', type=str, default='weather_stream',
                       help='Nom du topic Kafka (défaut: weather_stream)')
    parser.add_argument('--interval', type=int, default=None,
                       help='Intervalle en secondes pour le streaming continu (défaut: envoi unique)')
    
    args = parser.parse_args()
    
    # Créer le producteur
    producer = create_producer()
    
    try:
        # Produire les données
        produce_weather_data(
            producer,
            args.latitude,
            args.longitude,
            topic=args.topic,
            interval=args.interval
        )
    finally:
        producer.close()
        print("Producteur fermé.")
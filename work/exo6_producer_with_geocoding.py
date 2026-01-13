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

def geocode_city(city, country=None):
    """
    Utilise l'API Geocoding d'Open-Meteo pour obtenir les coordonnées d'une ville
    
    Args:
        city: Nom de la ville
        country: Code pays optionnel (ex: "FR", "UK", "US")
    
    Returns:
        dict: {'name', 'country', 'latitude', 'longitude', 'country_code'} ou None
    """
    url = "https://geocoding-api.open-meteo.com/v1/search"
    
    params = {
        'name': city,
        'count': 1,  # Retourner seulement le meilleur résultat
        'language': 'fr',
        'format': 'json'
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if 'results' in data and len(data['results']) > 0:
            result = data['results'][0]
            
            # Vérifier le pays si spécifié
            if country:
                country_upper = country.upper()
                if result.get('country_code') != country_upper:
                    # Chercher dans tous les résultats
                    for r in data['results']:
                        if r.get('country_code') == country_upper:
                            result = r
                            break
            
            return {
                'name': result.get('name'),
                'country': result.get('country'),
                'country_code': result.get('country_code'),
                'latitude': result.get('latitude'),
                'longitude': result.get('longitude'),
                'admin1': result.get('admin1', ''),  # Région
                'population': result.get('population', 0)
            }
        else:
            print(f"Aucun résultat trouvé pour '{city}'")
            return None
            
    except Exception as e:
        print(f"Erreur geocoding pour '{city}': {e}")
        return None

def get_current_weather(latitude, longitude):
    """
    Récupère les données météo actuelles depuis l'API Open-Meteo
    """
    url = "https://api.open-meteo.com/v1/forecast"
    
    params = {
        'latitude': latitude,
        'longitude': longitude,
        'current': [
            'temperature_2m',
            'wind_speed_10m', 
            'wind_direction_10m',
            'weather_code',
            'relative_humidity_2m',
            'precipitation'
        ],
        'timezone': 'auto'
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Erreur API météo: {e}")
        return None

def produce_weather_for_city(producer, city, country=None, topic='weather_stream', interval=None):
    """
    Produit des données météo pour une ville donnée
    
    Args:
        producer: Instance du producteur Kafka
        city: Nom de la ville
        country: Code pays (optionnel)
        topic: Nom du topic Kafka
        interval: Intervalle en secondes (None = une seule fois)
    """
    print(f"=== Exercice 6: Producteur avec Geocoding ===")
    print(f"Ville: {city}")
    if country:
        print(f"Pays: {country}")
    print(f"Topic: {topic}")
    print("\n1. Recherche des coordonnées...")
    
    # Geocoding
    geo_data = geocode_city(city, country)
    
    if not geo_data:
        print(f"❌ Impossible de géocoder '{city}'")
        return
    
    print(f"✓ Ville trouvée: {geo_data['name']}, {geo_data['country']} ({geo_data['country_code']})")
    print(f"  Coordonnées: {geo_data['latitude']}, {geo_data['longitude']}")
    if geo_data['admin1']:
        print(f"  Région: {geo_data['admin1']}")
    if geo_data['population'] > 0:
        print(f"  Population: {geo_data['population']:,}")
    
    if interval:
        print(f"\n2. Mode streaming: envoi toutes les {interval} secondes (Ctrl+C pour arrêter)")
    else:
        print(f"\n2. Mode single: envoi unique")
    print()
    
    message_count = 0
    
    try:
        while True:
            # Récupérer les données météo
            weather_data = get_current_weather(geo_data['latitude'], geo_data['longitude'])
            
            if weather_data:
                message_count += 1
                timestamp = datetime.now().isoformat()
                
                # Préparer le message avec métadonnées géographiques
                message = {
                    'timestamp': timestamp,
                    'city': geo_data['name'],
                    'country': geo_data['country'],
                    'country_code': geo_data['country_code'],
                    'region': geo_data['admin1'],
                    'latitude': geo_data['latitude'],
                    'longitude': geo_data['longitude'],
                    'temperature': weather_data['current']['temperature_2m'],
                    'windspeed': weather_data['current']['wind_speed_10m'],
                    'wind_direction': weather_data['current']['wind_direction_10m'],
                    'weather_code': weather_data['current']['weather_code'],
                    'humidity': weather_data['current']['relative_humidity_2m'],
                    'precipitation': weather_data['current']['precipitation'],
                    'units': weather_data['current_units']
                }
                
                # Envoyer vers Kafka
                future = producer.send(topic, value=message)
                record_metadata = future.get(timeout=10)
                
                print(f"[{timestamp}] Message #{message_count} envoyé")
                print(f"  {geo_data['name']}, {geo_data['country']}")
                print(f"  Température: {message['temperature']}°C")
                print(f"  Vent: {message['windspeed']} m/s")
                print(f"  Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")
                print()
            else:
                print("❌ Erreur lors de la récupération des données météo")
            
            # Si pas de streaming, on sort
            if interval is None:
                break
            
            # Attendre
            time.sleep(interval)
            
    except KeyboardInterrupt:
        print(f"\n\n🛑 Arrêt du producteur")
        print(f"Total de messages envoyés: {message_count}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Producteur Kafka avec geocoding automatique'
    )
    parser.add_argument('city', type=str, 
                       help='Nom de la ville')
    parser.add_argument('--country', type=str, default=None,
                       help='Code pays (ex: FR, UK, US, DE) - optionnel')
    parser.add_argument('--topic', type=str, default='weather_stream',
                       help='Nom du topic Kafka (défaut: weather_stream)')
    parser.add_argument('--interval', type=int, default=None,
                       help='Intervalle en secondes pour le streaming continu')
    
    args = parser.parse_args()
    
    # Créer le producteur
    producer = create_producer()
    
    try:
        # Produire les données
        produce_weather_for_city(
            producer,
            args.city,
            country=args.country,
            topic=args.topic,
            interval=args.interval
        )
    finally:
        producer.close()
        print("Producteur fermé.")
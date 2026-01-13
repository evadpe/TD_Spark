from kafka import KafkaProducer
import json
import time
import requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

def create_producer():
    return KafkaProducer(
        bootstrap_servers=['kafka:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def geocode_city(city, country=None):
    """Geocode une ville"""
    url = "https://geocoding-api.open-meteo.com/v1/search"
    params = {'name': city, 'count': 1, 'language': 'fr', 'format': 'json'}
    
    try:
        response = requests.get(url, params=params, timeout=10)
        data = response.json()
        
        if 'results' in data and len(data['results']) > 0:
            result = data['results'][0]
            
            # Vérifier le pays si spécifié
            if country:
                for r in data['results']:
                    if r.get('country_code') == country.upper():
                        result = r
                        break
            
            return {
                'name': result.get('name'),
                'country': result.get('country'),
                'country_code': result.get('country_code'),
                'latitude': result.get('latitude'),
                'longitude': result.get('longitude'),
                'admin1': result.get('admin1', '')
            }
    except Exception as e:
        print(f"Erreur geocoding '{city}': {e}")
    return None

def get_current_weather(latitude, longitude):
    """Récupère la météo actuelle"""
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        'latitude': latitude,
        'longitude': longitude,
        'current': ['temperature_2m', 'wind_speed_10m', 'wind_direction_10m',
                   'weather_code', 'relative_humidity_2m', 'precipitation'],
        'timezone': 'auto'
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        return response.json()
    except:
        return None

def produce_city_weather(producer, city_name, country_code, geo_cache, topic='weather_stream'):
    """Produit les données météo pour une ville"""
    
    # Utiliser le cache
    if city_name not in geo_cache:
        geo_data = geocode_city(city_name, country_code)
        if not geo_data:
            return None
        geo_cache[city_name] = geo_data
    else:
        geo_data = geo_cache[city_name]
    
    # Récupérer la météo
    weather_data = get_current_weather(geo_data['latitude'], geo_data['longitude'])
    
    if weather_data and 'current' in weather_data:  # ← Vérifier que 'current' existe
        try:
            message = {
                'timestamp': datetime.now().isoformat(),
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
            
            future = producer.send(topic, value=message)
            record_metadata = future.get(timeout=10)
            
            return {
                'city': geo_data['name'],
                'country': geo_data['country'],
                'temp': message['temperature'],
                'wind': message['windspeed'],
                'offset': record_metadata.offset
            }
        except Exception as e:
            print(f"Erreur traitement {city_name}: {e}")
            return None
    else:
        print(f" Données météo incomplètes pour {city_name}")
        return None
if __name__ == "__main__":
    # Liste de villes avec codes pays
    cities = [
        {'name': 'Paris', 'country': 'FR'},
        {'name': 'Marseille', 'country': 'FR'},
        {'name': 'Lyon', 'country': 'FR'},
        {'name': 'London', 'country': 'GB'},
        {'name': 'Berlin', 'country': 'DE'},
        {'name': 'Madrid', 'country': 'ES'},
        {'name': 'Rome', 'country': 'IT'},
        {'name': 'Amsterdam', 'country': 'NL'},
        {'name': 'Brussels', 'country': 'BE'}
    ]
    
    producer = create_producer()
    geo_cache = {}  # Cache pour éviter de géocoder à chaque fois
    
    print("=== Exercice 6: Producteur Multi-Villes avec Geocoding ===")
    print(f"Initialisation: géocodage de {len(cities)} villes...")
    
    # Geocoder toutes les villes au démarrage
    for city in cities:
        geo_data = geocode_city(city['name'], city['country'])
        if geo_data:
            geo_cache[city['name']] = geo_data
            print(f"  ✓ {geo_data['name']}, {geo_data['country']} ({geo_data['latitude']}, {geo_data['longitude']})")
        else:
            print(f"  Échec pour {city['name']}")
    
    print(f"\n{len(geo_cache)} villes géocodées avec succès!")
    print("Démarrage du streaming (Ctrl+C pour arrêter)\n")
    
    iteration = 0
    
    try:
        while True:
            iteration += 1
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print(f"\n{'='*80}")
            print(f"Itération #{iteration} - {timestamp}")
            print('='*80)
            
            results = []
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = []
                for city in cities:
                    future = executor.submit(
                        produce_city_weather,
                        producer, city['name'], city['country'], geo_cache
                    )
                    futures.append(future)
                
                for future in futures:
                    result = future.result()
                    if result:
                        results.append(result)
            
            # Affichage des résultats
            if results:
                print(f"\n{'Ville':<20} {'Pays':<15} {'Température':<15} {'Vent':<15}")
                print('-'*80)
                for r in sorted(results, key=lambda x: x['temp'], reverse=True):
                    print(f"{r['city']:<20} {r['country']:<15} {r['temp']:>6.1f}°C         {r['wind']:>6.1f} m/s")
                
                avg_temp = sum(r['temp'] for r in results) / len(results)
                avg_wind = sum(r['wind'] for r in results) / len(results)
                print('-'*80)
                print(f"{'MOYENNES':<20} {'Europe':<15} {avg_temp:>6.1f}°C         {avg_wind:>6.1f} m/s")
                print(f"\nMessages envoyés: {len(results)}/{len(cities)}")
            
            producer.flush()
            print(f"\n Prochaine mise à jour dans 30 secondes...")
            time.sleep(30)
            
    except KeyboardInterrupt:
        print("\n\nArrêt du producteur")
        print(f"Total d'itérations: {iteration}")
    finally:
        producer.close()
        print("Producteur fermé.")
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

def get_current_weather(latitude, longitude):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        'latitude': latitude,
        'longitude': longitude,
        'current': ['temperature_2m', 'wind_speed_10m', 'weather_code'],
        'timezone': 'auto'
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Erreur API: {e}")
        return None

def produce_city_weather(producer, city_name, latitude, longitude, topic='weather_stream'):
    weather_data = get_current_weather(latitude, longitude)
    
    if weather_data:
        message = {
            'timestamp': datetime.now().isoformat(),
            'city': city_name,
            'latitude': latitude,
            'longitude': longitude,
            'weather_data': weather_data
        }
        
        producer.send(topic, value=message)
        print(f"[{city_name}] Température: {weather_data['current']['temperature_2m']}°C, "
              f"Vent: {weather_data['current']['wind_speed_10m']} m/s")

if __name__ == "__main__":
    cities = {
        'Paris': (48.8566, 2.3522),
        'Marseille': (43.2965, 5.3698),
        'Lyon': (45.7640, 4.8357),
        'Londres': (51.5074, -0.1278),
        'Berlin': (52.5200, 13.4050)
    }
    
    producer = create_producer()
    
    print("=== Producteur Multi-Villes ===")
    print(f"Streaming pour {len(cities)} villes (Ctrl+C pour arrêter)\n")
    
    try:
        while True:
            print(f"\n--- {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")
            
            with ThreadPoolExecutor(max_workers=5) as executor:
                for city, (lat, lon) in cities.items():
                    executor.submit(produce_city_weather, producer, city, lat, lon)
            
            producer.flush()
            print("\nEn attente de 30 secondes...")
            time.sleep(30)
            
    except KeyboardInterrupt:
        print("\n\nArrêt du producteur multi-villes")
    finally:
        producer.close()
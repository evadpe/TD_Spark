import requests
import json
import time
import argparse
from kafka import KafkaProducer

GEOCODING_API = "https://geocoding-api.open-meteo.com/v1/search"
WEATHER_API = "https://api.open-meteo.com/v1/forecast"

def get_coordinates(city, country):
    """Récupère lat/lon via l'API Geocoding"""
    params = {
        "name": city,
        "count": 5,
        "language": "en",
        "format": "json"
    }
    
    resp = requests.get(GEOCODING_API, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    
    if not data.get("results"):
        raise ValueError(f"City '{city}' not found")
    
    # Filtrer par pays si spécifié
    results = data["results"]
    if country:
        results = [r for r in results if country.lower() in r.get("country", "").lower()]
        if not results:
            raise ValueError(f"City '{city}' not found in country '{country}'")
    
    location = results[0]
    return {
        "latitude": location["latitude"],
        "longitude": location["longitude"],
        "city": location["name"],
        "country": location["country"]
    }

def fetch_weather(lat, lon):
    """Récupère la météo actuelle"""
    params = {
        "latitude": lat,
        "longitude": lon,
        "current_weather": "true"
    }
    resp = requests.get(WEATHER_API, params=params, timeout=10)
    resp.raise_for_status()
    return resp.json().get("current_weather", {})

def main():
    parser = argparse.ArgumentParser(description="Weather producer with city/country")
    parser.add_argument("--city", required=True, help="City name (e.g., Paris)")
    parser.add_argument("--country", required=True, help="Country name (e.g., France)")
    parser.add_argument("--interval", type=int, default=60, help="Polling interval in seconds")
    parser.add_argument("--topic", default="weather_stream", help="Kafka topic name")
    args = parser.parse_args()
    
    # Récupération des coordonnées
    print(f" Looking up coordinates for {args.city}, {args.country}...")
    location = get_coordinates(args.city, args.country)
    print(f" Found: {location['city']}, {location['country']} ({location['latitude']}, {location['longitude']})")
    
    # Kafka producer
    producer = KafkaProducer(
        bootstrap_servers="kafka:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    
    print(f" Starting weather producer (topic={args.topic}, interval={args.interval}s)...")
    
    try:
        while True:
            weather = fetch_weather(location["latitude"], location["longitude"])
            
            if weather:
                # Enrichissement avec city/country
                enriched_message = {
                    **weather,
                    "city": location["city"],
                    "country": location["country"],
                    "latitude": location["latitude"],
                    "longitude": location["longitude"]
                }
                
                producer.send(args.topic, enriched_message)
                print(f" Sent to {args.topic}: {enriched_message}")
            
            time.sleep(args.interval)
    
    except KeyboardInterrupt:
        print("\n  Producer stopped by user")
    except Exception as e:
        print(f"  Error: {e}")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
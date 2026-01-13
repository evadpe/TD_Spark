from kafka import KafkaConsumer, KafkaProducer
import json
from datetime import datetime

def classify_wind_alert(windspeed):
    if windspeed < 10:
        return "level_0"
    elif 10 <= windspeed <= 20:
        return "level_1"
    else:
        return "level_2"

def classify_heat_alert(temperature):
    if temperature < 25:
        return "level_0"
    elif 25 <= temperature <= 35:
        return "level_1"
    else:
        return "level_2"

def transform_weather_data():
    # Consumer
    consumer = KafkaConsumer(
        'weather_stream',
        bootstrap_servers=['kafka:9092'],
        auto_offset_reset='latest',
        group_id='weather-transformer',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    # Producer
    producer = KafkaProducer(
        bootstrap_servers=['kafka:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    print("=== Transformation Simple (Python) ===")
    print("Lecture: weather_stream")
    print("Écriture: weather_transformed")
    print("En attente de messages...\n")
    
    message_count = 0
    
    try:
        for message in consumer:
            data = message.value
            message_count += 1
            
            # Extraction des données
            temperature = data.get('temperature', 0)
            windspeed = data.get('windspeed', 0)
            
            # Transformation
            transformed = {
                'event_time': datetime.now().isoformat(),
                'original_timestamp': data.get('timestamp'),
                'city': data.get('city'),
                'country': data.get('country'),
                'latitude': data.get('latitude'),
                'longitude': data.get('longitude'),
                'temperature': temperature,
                'windspeed': windspeed,
                'wind_direction': data.get('wind_direction'),
                'weather_code': data.get('weather_code'),
                'humidity': data.get('humidity'),
                'precipitation': data.get('precipitation'),
                'wind_alert_level': classify_wind_alert(windspeed),
                'heat_alert_level': classify_heat_alert(temperature)
            }
            
            # Envoi vers weather_transformed
            producer.send('weather_transformed', value=transformed)
            
            print(f"[{message_count}] {data.get('city')} - "
                  f"Temp: {temperature:.1f}°C ({transformed['heat_alert_level']}), "
                  f"Vent: {windspeed:.1f}m/s ({transformed['wind_alert_level']})")
            
    except KeyboardInterrupt:
        print(f"\n\nArrêt de la transformation")
        print(f"Messages transformés: {message_count}")
    finally:
        consumer.close()
        producer.close()

if __name__ == "__main__":
    transform_weather_data()
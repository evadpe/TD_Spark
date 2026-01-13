from kafka import KafkaConsumer
import json
from datetime import datetime

def monitor_anomalies():
    """Monitore et affiche les anomalies climatiques"""
    
    consumer = KafkaConsumer(
        'weather_anomalies',
        bootstrap_servers=['kafka:9092'],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='anomaly-monitor',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    print("===  MONITORING DES ANOMALIES CLIMATIQUES ===")
    print("Écoute du topic: weather_anomalies")
    print("En attente d'anomalies... (Ctrl+C pour arrêter)\n")
    
    anomaly_count = 0
    
    try:
        for message in consumer:
            anomaly_count += 1
            data = message.value
            
            timestamp = datetime.fromisoformat(data['event_timestamp'].replace('Z', '+00:00'))
            city = data['city']
            country = data['country']
            anomaly_type = data['anomaly_type']
            variable = data['variable']
            observed = data['observed_value']
            expected = data['expected_value']
            deviation = data['deviation']
            threshold = data['threshold']
            
            # Emoji selon le type
            emoji_map = {
                'heat_wave': '🔥',
                'cold_wave': '❄️',
                'wind_storm': '💨'
            }
            emoji = emoji_map.get(anomaly_type, '⚠️')
            
            print(f"\n{'='*80}")
            print(f"{emoji} ANOMALIE #{anomaly_count} DÉTECTÉE {emoji}")
            print('='*80)
            print(f"Heure      : {timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"Lieu       : {city}, {country}")
            print(f"Type       : {anomaly_type.upper().replace('_', ' ')}")
            print(f"Variable   : {variable}")
            print(f"Valeur obs : {observed:.1f} {'°C' if variable == 'temperature' else 'm/s'}")
            print(f"Valeur att : {expected:.1f} {'°C' if variable == 'temperature' else 'm/s'}")
            print(f"Déviation  : {deviation:.1f} (seuil: {threshold:.1f})")
            print(f"Écart      : {((observed - expected) / expected * 100):.1f}% par rapport à la normale")
            print('='*80)
            
    except KeyboardInterrupt:
        print(f"\n\n Arrêt du monitoring")
        print(f"Total d'anomalies détectées: {anomaly_count}")
    finally:
        consumer.close()

if __name__ == "__main__":
    monitor_anomalies()
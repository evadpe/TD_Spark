from kafka import KafkaConsumer
import json
from datetime import datetime
import os

def consume_and_store():
    """Consomme weather_transformed et stocke localement"""
    
    # Répertoire de base (monté en volume, accessible depuis Windows)
    base_dir = "/home/jovyan/work/hdfs-data"
    os.makedirs(base_dir, exist_ok=True)
    
    consumer = KafkaConsumer(
        'weather_transformed',
        bootstrap_servers=['kafka:9092'],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='local-storage-consumer',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    print("=== Exercice 7: Stockage Local ===")
    print("Lecture depuis: weather_transformed")
    print(f"Stockage vers: {base_dir}/{{country}}/{{city}}/alerts.json")
    print("(Les fichiers seront copiés vers HDFS manuellement)")
    print("En attente de messages... (Ctrl+C pour arrêter)\n")
    
    message_count = 0
    saved_count = 0
    stats = {}
    batch_data = {}
    
    try:
        for message in consumer:
            message_count += 1
            data = message.value
            
            # Nettoyer les noms
            city = data.get('city', 'Unknown').replace(' ', '_').replace('/', '_').replace("'", '')
            country = data.get('country', 'Unknown').replace(' ', '_').replace('/', '_').replace("'", '')
            location = f"{country}/{city}"
            
            # Préparer les données
            alert_data = {
                'event_time': data.get('event_time'),
                'city': data.get('city'),
                'country': data.get('country'),
                'country_code': data.get('country_code'),
                'temperature': data.get('temperature'),
                'windspeed': data.get('windspeed'),
                'wind_alert_level': data.get('wind_alert_level'),
                'heat_alert_level': data.get('heat_alert_level'),
                'weather_code': data.get('weather_code'),
                'humidity': data.get('humidity'),
                'precipitation': data.get('precipitation')
            }
            
            # Créer les répertoires
            dir_path = os.path.join(base_dir, country, city)
            os.makedirs(dir_path, exist_ok=True)
            
            file_path = os.path.join(dir_path, "alerts.json")
            
            # Écrire (append)
            try:
                with open(file_path, 'a') as f:
                    f.write(json.dumps(alert_data) + '\n')
                
                saved_count += 1
                stats[location] = stats.get(location, 0) + 1
                
                print(f"[{message_count}] ✓ Sauvegardé: {country}/{city}")
                print(f"    Temp: {alert_data['temperature']}°C ({alert_data['heat_alert_level']}), "
                      f"Vent: {alert_data['windspeed']} m/s ({alert_data['wind_alert_level']})")
                
            except Exception as e:
                print(f"[{message_count}] ❌ Erreur: {e}")
            
            # Stats tous les 20 messages
            if message_count % 20 == 0:
                print(f"\n{'='*70}")
                print(f"Stats - Messages: {message_count}, Sauvegardés: {saved_count}")
                print('-'*70)
                for loc, count in sorted(stats.items()):
                    print(f"  {loc}: {count} alertes")
                print('='*70)
                print()
    
    except KeyboardInterrupt:
        print("\n\n Arrêt du stockage")
        print(f"\nRésumé:")
        print(f"  Messages reçus: {message_count}")
        print(f"  ✓ Sauvegardés: {saved_count}")
        print(f"\nRépartition par localisation:")
        for location, count in sorted(stats.items()):
            print(f"  {location}: {count} alertes")
    
    finally:
        consumer.close()
        print("\nConsommateur fermé.")

if __name__ == "__main__":
    consume_and_store()
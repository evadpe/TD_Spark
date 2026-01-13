from kafka import KafkaConsumer
import json
from datetime import datetime
import subprocess
import os

def create_hdfs_directory(path):
    """Crée un répertoire HDFS s'il n'existe pas"""
    try:
        # Vérifier si le répertoire existe
        result = subprocess.run(
            ['hdfs', 'dfs', '-test', '-d', path],
            capture_output=True
        )
        
        # Si n'existe pas (code retour != 0), créer
        if result.returncode != 0:
            subprocess.run(
                ['hdfs', 'dfs', '-mkdir', '-p', path],
                check=True,
                capture_output=True
            )
            print(f"  ✓ Répertoire créé: {path}")
        return True
    except Exception as e:
        print(f"  Erreur création répertoire {path}: {e}")
        return False

def save_to_hdfs(data, hdfs_path):
    """Sauvegarde des données JSON dans HDFS"""
    try:
        # Créer un fichier temporaire local
        temp_file = f"/tmp/alert_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}.json"
        
        with open(temp_file, 'w') as f:
            json.dump(data, f, indent=2)
        
        # Copier vers HDFS (append mode)
        result = subprocess.run(
            ['hdfs', 'dfs', '-appendToFile', temp_file, hdfs_path],
            capture_output=True,
            text=True
        )
        
        # Si le fichier n'existe pas, utiliser -put
        if result.returncode != 0:
            subprocess.run(
                ['hdfs', 'dfs', '-put', temp_file, hdfs_path],
                check=True,
                capture_output=True
            )
        
        # Supprimer le fichier temporaire
        os.remove(temp_file)
        return True
        
    except Exception as e:
        print(f"  Erreur sauvegarde HDFS: {e}")
        if os.path.exists(temp_file):
            os.remove(temp_file)
        return False

def consume_and_store():
    """Consomme weather_transformed et stocke dans HDFS"""
    
    consumer = KafkaConsumer(
        'weather_transformed',
        bootstrap_servers=['kafka:9092'],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='hdfs-storage-consumer',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    print("=== Exercice 7: Stockage HDFS Organisé ===")
    print("Lecture depuis: weather_transformed")
    print("Stockage vers: HDFS /hdfs-data/{country}/{city}/alerts.json")
    print("En attente de messages... (Ctrl+C pour arrêter)\n")
    
    message_count = 0
    saved_count = 0
    
    # Statistiques par pays/ville
    stats = {}
    
    try:
        for message in consumer:
            message_count += 1
            data = message.value
            
            # Extraire les métadonnées
            city = data.get('city', 'Unknown').replace(' ', '_').replace('/', '_')
            country = data.get('country', 'Unknown').replace(' ', '_').replace('/', '_')
            
            # Structure HDFS
            hdfs_dir = f"/hdfs-data/{country}/{city}"
            hdfs_file = f"{hdfs_dir}/alerts.json"
            
            # Créer le répertoire si nécessaire
            if f"{country}/{city}" not in stats:
                create_hdfs_directory(hdfs_dir)
                stats[f"{country}/{city}"] = 0
            
            # Préparer les données à sauvegarder
            alert_data = {
                'event_time': data.get('event_time'),
                'city': data.get('city'),
                'country': data.get('country'),
                'temperature': data.get('temperature'),
                'windspeed': data.get('windspeed'),
                'wind_alert_level': data.get('wind_alert_level'),
                'heat_alert_level': data.get('heat_alert_level'),
                'weather_code': data.get('weather_code'),
                'humidity': data.get('humidity'),
                'precipitation': data.get('precipitation')
            }
            
            # Sauvegarder dans HDFS
            if save_to_hdfs(alert_data, hdfs_file):
                saved_count += 1
                stats[f"{country}/{city}"] += 1
                
                print(f"[{message_count}] ✓ Sauvegardé: {country}/{city}")
                print(f"    Température: {alert_data['temperature']}°C ({alert_data['heat_alert_level']})")
                print(f"    Vent: {alert_data['windspeed']} m/s ({alert_data['wind_alert_level']})")
                print(f"    Fichier: {hdfs_file}")
                print()
            
            # Afficher les stats tous les 10 messages
            if message_count % 10 == 0:
                print(f"\n{'='*70}")
                print(f"Statistiques - Messages: {message_count}, Sauvegardés: {saved_count}")
                print('-'*70)
                for location, count in sorted(stats.items()):
                    print(f"  {location}: {count} alertes")
                print('='*70)
                print()
    
    except KeyboardInterrupt:
        print("\n\nArrêt du stockage HDFS")
        print(f"\nRésumé:")
        print(f"  Messages reçus: {message_count}")
        print(f"  Alertes sauvegardées: {saved_count}")
        print(f"\nRépartition par localisation:")
        for location, count in sorted(stats.items()):
            print(f"  {location}: {count} alertes")
    
    finally:
        consumer.close()
        print("\nConsommateur fermé.")

if __name__ == "__main__":
    consume_and_store()
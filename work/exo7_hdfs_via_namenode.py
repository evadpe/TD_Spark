from kafka import KafkaConsumer
import json
import os
import subprocess

def copy_to_hdfs(local_path, hdfs_path):
    """Copie un fichier local vers HDFS via le conteneur namenode"""
    try:
        # Créer le répertoire HDFS
        hdfs_dir = os.path.dirname(hdfs_path)
        subprocess.run(
            ['docker', 'exec', 'namenode', 'hdfs', 'dfs', '-mkdir', '-p', hdfs_dir],
            capture_output=True
        )
        
        # Copier le fichier dans le conteneur namenode
        temp_container_path = f"/tmp/{os.path.basename(local_path)}"
        subprocess.run(
            ['docker', 'cp', local_path, f"namenode:{temp_container_path}"],
            check=True,
            capture_output=True
        )
        
        # Copier vers HDFS depuis namenode
        result = subprocess.run(
            ['docker', 'exec', 'namenode', 'hdfs', 'dfs', '-appendToFile', temp_container_path, hdfs_path],
            capture_output=True
        )
        
        # Si le fichier n'existe pas, utiliser -put
        if result.returncode != 0:
            subprocess.run(
                ['docker', 'exec', 'namenode', 'hdfs', 'dfs', '-put', temp_container_path, hdfs_path],
                check=True,
                capture_output=True
            )
        
        # Nettoyer le fichier temporaire
        subprocess.run(
            ['docker', 'exec', 'namenode', 'rm', temp_container_path],
            capture_output=True
        )
        
        return True
    except Exception as e:
        print(f"  Erreur HDFS: {e}")
        return False

def consume_and_store():
    """Consomme weather_transformed et stocke dans HDFS"""
    
    # Répertoire temporaire local
    temp_dir = "/home/jovyan/work/temp_hdfs"
    os.makedirs(temp_dir, exist_ok=True)
    
    consumer = KafkaConsumer(
        'weather_transformed',
        bootstrap_servers=['kafka:9092'],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='hdfs-storage-consumer',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    print("=== Exercice 7: Stockage HDFS (via namenode) ===")
    print("Lecture depuis: weather_transformed")
    print("Stockage vers: HDFS /hdfs-data/{country}/{city}/alerts.json")
    print("En attente de messages... (Ctrl+C pour arrêter)\n")
    
    message_count = 0
    saved_count = 0
    stats = {}
    batch_data = {}  # Batch par localisation
    
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
            
            # Ajouter au batch
            if location not in batch_data:
                batch_data[location] = []
            batch_data[location].append(alert_data)
            
            print(f"[{message_count}] Reçu: {country}/{city} - "
                  f"Temp: {alert_data['temperature']}°C, Vent: {alert_data['windspeed']} m/s")
            
            # Flush vers HDFS tous les 5 messages par localisation
            if len(batch_data[location]) >= 5:
                local_file = os.path.join(temp_dir, f"{country}_{city}_{message_count}.json")
                hdfs_path = f"/hdfs-data/{country}/{city}/alerts.json"
                
                # Écrire localement
                with open(local_file, 'w') as f:
                    for alert in batch_data[location]:
                        f.write(json.dumps(alert) + '\n')
                
                # Copier vers HDFS
                if copy_to_hdfs(local_file, hdfs_path):
                    saved_count += len(batch_data[location])
                    stats[location] = stats.get(location, 0) + len(batch_data[location])
                    print(f"  ✓ {len(batch_data[location])} alertes copiées vers HDFS: {hdfs_path}")
                else:
                    print(f"  ❌ Échec copie HDFS pour {location}")
                
                # Nettoyer
                os.remove(local_file)
                batch_data[location] = []
            
            # Stats tous les 20 messages
            if message_count % 20 == 0:
                print(f"\n{'='*70}")
                print(f"Stats - Messages: {message_count}, Sauvegardés dans HDFS: {saved_count}")
                print('-'*70)
                for loc, count in sorted(stats.items()):
                    print(f"  {loc}: {count} alertes")
                print('='*70)
                print()
    
    except KeyboardInterrupt:
        print("\n\n🛑 Arrêt - Flush des données restantes...")
        
        # Flush tous les batchs restants
        for location, alerts in batch_data.items():
            if alerts:
                country, city = location.split('/')
                local_file = os.path.join(temp_dir, f"{country}_{city}_final.json")
                hdfs_path = f"/hdfs-data/{country}/{city}/alerts.json"
                
                with open(local_file, 'w') as f:
                    for alert in alerts:
                        f.write(json.dumps(alert) + '\n')
                
                if copy_to_hdfs(local_file, hdfs_path):
                    saved_count += len(alerts)
                    stats[location] = stats.get(location, 0) + len(alerts)
                    print(f"  ✓ {len(alerts)} alertes finales copiées: {location}")
                
                os.remove(local_file)
        
        print(f"\nRésumé:")
        print(f"  Messages reçus: {message_count}")
        print(f"  ✓ Sauvegardés dans HDFS: {saved_count}")
        print(f"\nRépartition par localisation:")
        for location, count in sorted(stats.items()):
            print(f"  {location}: {count} alertes")
    
    finally:
        consumer.close()
        print("\nConsommateur fermé.")

if __name__ == "__main__":
    consume_and_store()
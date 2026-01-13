from kafka import KafkaConsumer
import json
from datetime import datetime
import requests

class HDFSClient:
    """Client amélioré pour WebHDFS"""
    
    def __init__(self, namenode_host='namenode', namenode_port=9870, user='root'):
        self.base_url = f"http://{namenode_host}:{namenode_port}/webhdfs/v1"
        self.user = user
    
    def mkdir(self, path):
        """Crée un répertoire HDFS"""
        url = f"{self.base_url}{path}?op=MKDIRS&user.name={self.user}"
        try:
            response = requests.put(url, timeout=10)
            return response.status_code == 200
        except Exception as e:
            return False
    
    def file_exists(self, path):
        """Vérifie si un fichier existe"""
        url = f"{self.base_url}{path}?op=GETFILESTATUS&user.name={self.user}"
        try:
            response = requests.get(url, timeout=10)
            return response.status_code == 200
        except:
            return False
    
    def create_or_append(self, path, data):
        """Crée un fichier ou ajoute des données"""
        try:
            if not self.file_exists(path):
                return self.create(path, data)
            else:
                return self.append(path, data)
        except Exception as e:
            print(f"  Erreur create_or_append: {e}")
            return False
    
    def create(self, path, data):
        """Crée un nouveau fichier HDFS"""
        try:
            url = f"{self.base_url}{path}?op=CREATE&overwrite=false&user.name={self.user}"
            response = requests.put(url, allow_redirects=False, timeout=10)
            
            if response.status_code == 307:
                redirect_url = response.headers['Location']
                data_response = requests.put(redirect_url, data=data, timeout=30)
                return data_response.status_code == 201
            
            return False
        except Exception as e:
            print(f"  Erreur create: {e}")
            return False
    
    def append(self, path, data):
        """Ajoute des données à un fichier existant"""
        try:
            url = f"{self.base_url}{path}?op=APPEND&user.name={self.user}"
            response = requests.post(url, allow_redirects=False, timeout=10)
            
            if response.status_code == 307:
                redirect_url = response.headers['Location']
                data_response = requests.post(redirect_url, data=data, timeout=30)
                return data_response.status_code == 200
            
            return False
        except Exception as e:
            print(f"  Erreur append: {e}")
            return False

def consume_and_store():
    """Consomme weather_transformed et stocke dans HDFS"""
    
    hdfs = HDFSClient()
    
    consumer = KafkaConsumer(
        'weather_transformed',
        bootstrap_servers=['kafka:9092'],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='hdfs-storage-consumer',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    print("=== Exercice 7: Stockage HDFS Organisé (WebHDFS v2) ===")
    print("Lecture depuis: weather_transformed")
    print("Stockage vers: HDFS /hdfs-data/{country}/{city}/alerts.json")
    print(f"Connexion HDFS: http://namenode:9870")
    print("En attente de messages... (Ctrl+C pour arrêter)\n")
    
    message_count = 0
    saved_count = 0
    failed_count = 0
    
    created_dirs = set()
    stats = {}
    
    try:
        for message in consumer:
            message_count += 1
            data = message.value
            
            # Nettoyer les noms
            city = data.get('city', 'Unknown')
            city = city.replace(' ', '_').replace('/', '_').replace("'", '').replace('-', '_')
            
            country = data.get('country', 'Unknown')
            country = country.replace(' ', '_').replace('/', '_').replace("'", '').replace('-', '_')
            
            hdfs_dir = f"/hdfs-data/{country}/{city}"
            hdfs_file = f"{hdfs_dir}/alerts.json"
            
            # Créer le répertoire une seule fois
            if hdfs_dir not in created_dirs:
                if hdfs.mkdir(hdfs_dir):
                    print(f"  ✓ Répertoire créé: {hdfs_dir}")
                created_dirs.add(hdfs_dir)
            
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
            
            json_line = json.dumps(alert_data) + '\n'
            
            # Sauvegarder
            if hdfs.create_or_append(hdfs_file, json_line.encode('utf-8')):
                saved_count += 1
                location = f"{country}/{city}"
                stats[location] = stats.get(location, 0) + 1
                
                print(f"[{message_count}] ✓ Sauvegardé: {country}/{city}")
                print(f"    Temp: {alert_data['temperature']}°C ({alert_data['heat_alert_level']}), "
                      f"Vent: {alert_data['windspeed']} m/s ({alert_data['wind_alert_level']})")
            else:
                failed_count += 1
                print(f"[{message_count}] ❌ Échec: {country}/{city}")
            
            # Stats tous les 10 messages
            if message_count % 10 == 0:
                success_rate = (saved_count / message_count * 100) if message_count > 0 else 0
                print(f"\n{'='*70}")
                print(f"Stats - Messages: {message_count}, ✓ {saved_count}, ❌ {failed_count} ({success_rate:.1f}% succès)")
                print('-'*70)
                for location, count in sorted(stats.items()):
                    print(f"  {location}: {count} alertes")
                print('='*70)
                print()
    
    except KeyboardInterrupt:
        print("\n\n🛑 Arrêt du stockage HDFS")
        success_rate = (saved_count / message_count * 100) if message_count > 0 else 0
        print(f"\nRésumé:")
        print(f"  Messages reçus: {message_count}")
        print(f"  ✓ Sauvegardés: {saved_count}")
        print(f"  ❌ Échecs: {failed_count}")
        print(f"  Taux de succès: {success_rate:.1f}%")
        print(f"\nRépartition par localisation:")
        for location, count in sorted(stats.items()):
            print(f"  {location}: {count} alertes")
    
    finally:
        consumer.close()
        print("\nConsommateur fermé.")

if __name__ == "__main__":
    consume_and_store()
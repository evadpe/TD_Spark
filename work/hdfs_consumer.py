from kafka import KafkaConsumer
import json
from hdfs import InsecureClient
from datetime import datetime
import os
import argparse

# Configuration
KAFKA_TOPIC = "weather_transformed"
KAFKA_BROKER = "kafka:9092"
HDFS_BASE_DIR = "/hdfs-data"

class HDFSWeatherConsumer:
    def __init__(self, hdfs_url="http://namenode:9870", hdfs_user="root", batch_size=10):
        """
        Consommateur Kafka qui sauvegarde dans HDFS
        
        Args:
            hdfs_url: URL du NameNode HDFS
            hdfs_user: Utilisateur HDFS
            batch_size: Nombre de messages avant écriture dans HDFS
        """
        self.hdfs_client = InsecureClient(hdfs_url, user=hdfs_user)
        self.batch_size = batch_size
        self.message_buffer = {}  # {(country, city): [messages]}
        
        # Vérifier la connexion HDFS
        try:
            self.hdfs_client.status('/')
            print(f"✅ Connected to HDFS at {hdfs_url}")
        except Exception as e:
            print(f"❌ HDFS connection failed: {e}")
            raise
    
    def get_hdfs_path(self, country, city):
        """Génère le chemin HDFS pour un pays/ville"""
        # Nettoyer les noms (remplacer espaces et caractères spéciaux)
        country_clean = country.replace(" ", "_").replace("/", "_")
        city_clean = city.replace(" ", "_").replace("/", "_")
        return f"{HDFS_BASE_DIR}/{country_clean}/{city_clean}/alerts.json"
    
    def buffer_message(self, message):
        """Ajoute un message au buffer"""
        country = message.get("country", "Unknown")
        city = message.get("city", "Unknown")
        
        key = (country, city)
        if key not in self.message_buffer:
            self.message_buffer[key] = []
        
        self.message_buffer[key].append(message)
        
        # Si le batch est atteint, flush
        if len(self.message_buffer[key]) >= self.batch_size:
            self.flush_to_hdfs(country, city)
    
    def flush_to_hdfs(self, country, city):
        """Écrit le buffer dans HDFS"""
        key = (country, city)
        messages = self.message_buffer.get(key, [])
        
        if not messages:
            return
        
        hdfs_path = self.get_hdfs_path(country, city)
        
        try:
            # Créer le répertoire si nécessaire
            hdfs_dir = os.path.dirname(hdfs_path)
            self.hdfs_client.makedirs(hdfs_dir)
            
            # Lire le contenu existant
            existing_data = []
            try:
                with self.hdfs_client.read(hdfs_path, encoding='utf-8') as reader:
                    for line in reader:
                        if line.strip():
                            existing_data.append(json.loads(line))
            except Exception:
                # Fichier n'existe pas encore
                pass
            
            # Ajouter les nouveaux messages
            all_data = existing_data + messages
            
            # Écrire dans HDFS (format JSONL - une ligne par objet)
            with self.hdfs_client.write(hdfs_path, overwrite=True, encoding='utf-8') as writer:
                for msg in all_data:
                    writer.write(json.dumps(msg) + '\n')
            
            print(f"✅ Saved {len(messages)} messages to {hdfs_path} (total: {len(all_data)})")
            
            # Vider le buffer
            self.message_buffer[key] = []
            
        except Exception as e:
            print(f"❌ Error writing to HDFS {hdfs_path}: {e}")
    
    def flush_all(self):
        """Flush tous les buffers"""
        for (country, city) in list(self.message_buffer.keys()):
            self.flush_to_hdfs(country, city)
    
    def consume(self):
        """Démarre la consommation Kafka"""
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BROKER,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8"))
        )
        
        print(f"🚀 Started consuming from topic '{KAFKA_TOPIC}'...")
        print(f"📊 Batch size: {self.batch_size}")
        print(f"💾 Base HDFS directory: {HDFS_BASE_DIR}")
        print("-" * 80)
        
        message_count = 0
        
        try:
            for msg in consumer:
                record = msg.value
                message_count += 1
                
                # Afficher le message
                city = record.get("city", "Unknown")
                country = record.get("country", "Unknown")
                temp = record.get("temperature", "N/A")
                wind_alert = record.get("wind_alert_level", "N/A")
                heat_alert = record.get("heat_alert_level", "N/A")
                
                print(f"📥 [{message_count}] {city}, {country} | "
                      f"Temp: {temp}°C | Wind: {wind_alert} | Heat: {heat_alert}")
                
                # Buffer le message
                self.buffer_message(record)
        
        except KeyboardInterrupt:
            print("\n⛔ Stopping consumer...")
            self.flush_all()
            print(f"✅ Total messages processed: {message_count}")
        
        except Exception as e:
            print(f"❌ Error: {e}")
            self.flush_all()


def main():
    parser = argparse.ArgumentParser(description="HDFS Weather Consumer")
    parser.add_argument("--batch-size", type=int, default=10, 
                        help="Number of messages before writing to HDFS")
    parser.add_argument("--hdfs-url", default="http://namenode:9870",
                        help="HDFS NameNode URL")
    parser.add_argument("--hdfs-user", default="root",
                        help="HDFS user")
    
    args = parser.parse_args()
    
    consumer = HDFSWeatherConsumer(
        hdfs_url=args.hdfs_url,
        hdfs_user=args.hdfs_user,
        batch_size=args.batch_size
    )
    
    consumer.consume()


if __name__ == "__main__":
    main()

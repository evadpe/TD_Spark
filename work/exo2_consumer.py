from kafka import KafkaConsumer
import json
import sys
import argparse

def create_consumer(topic, group_id='weather-consumer-group'):
    """Crée et retourne un consommateur Kafka"""
    return KafkaConsumer(
        topic,
        bootstrap_servers=['kafka:9092'],
        auto_offset_reset='earliest',  # Lire depuis le début
        enable_auto_commit=True,
        group_id=group_id,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

def consume_messages(topic):
    """Consomme et affiche les messages d'un topic Kafka en temps réel"""
    print(f"=== Exercice 2: Consommateur Kafka ===")
    print(f"Écoute du topic: '{topic}'")
    print(f"En attente de messages... (Ctrl+C pour arrêter)\n")
    
    consumer = create_consumer(topic)
    
    try:
        message_count = 0
        for message in consumer:
            message_count += 1
            
            print(f"--- Message #{message_count} ---")
            print(f"Topic     : {message.topic}")
            print(f"Partition : {message.partition}")
            print(f"Offset    : {message.offset}")
            print(f"Timestamp : {message.timestamp}")
            print(f"Contenu   : {message.value}")
            print()
            
    except KeyboardInterrupt:
        print(f"\n\nArrêt du consommateur...")
        print(f"Total de messages reçus: {message_count}")
    
    finally:
        consumer.close()
        print("Consommateur fermé.")

if __name__ == "__main__":
    # Parser les arguments
    parser = argparse.ArgumentParser(description='Consommateur Kafka pour lire les messages d\'un topic')
    parser.add_argument('topic', type=str, help='Nom du topic Kafka à consommer')
    parser.add_argument('--group', type=str, default='weather-consumer-group', 
                       help='Nom du consumer group (défaut: weather-consumer-group)')
    
    args = parser.parse_args()
    
    # Consommer les messages
    consume_messages(args.topic)
from kafka import KafkaProducer
import json
import time

def create_producer():
    """Crée et retourne un producteur Kafka"""
    return KafkaProducer(
        bootstrap_servers=['kafka:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def send_static_message(producer, topic='weather_stream'):
    """Envoie un message statique au topic Kafka"""
    message = {"msg": "Hello Kafka"}
    
    print(f"Envoi du message vers le topic '{topic}'...")
    future = producer.send(topic, value=message)
    
    # Attendre la confirmation
    record_metadata = future.get(timeout=10)
    
    print(f"Message envoyé avec succès !")
    print(f"  Topic: {record_metadata.topic}")
    print(f"  Partition: {record_metadata.partition}")
    print(f"  Offset: {record_metadata.offset}")
    
    return record_metadata

if __name__ == "__main__":
    
    # Créer le producteur
    producer = create_producer()
    
    try:
        # Envoyer le message
        send_static_message(producer, topic='weather_stream')
        
    except Exception as e:
        print(f"Erreur lors de l'envoi: {e}")
    
    finally:
        # Fermer le producteur proprement
        producer.close()
        print("\nProducteur fermé.")
from kafka import KafkaConsumer
import json
import argparse

def main():
    parser = argparse.ArgumentParser(description="Kafka consumer")
    parser.add_argument("--topic", default="weather_transformed", help="Kafka topic to consume")
    args = parser.parse_args()
    
    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers="kafka:9092",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )
    
    print(f" Consumer started on topic '{args.topic}'...")
    print("Waiting for messages...\n")
    
    for msg in consumer:
        data = msg.value
        print("=" * 80)
        print(f" Message received:")
        print(f"   City: {data.get('city', 'N/A')}")
        print(f"   Country: {data.get('country', 'N/A')}")
        print(f"   Temperature: {data.get('temperature', 'N/A')}°C")
        print(f"   Wind Speed: {data.get('windspeed', 'N/A')} m/s")
        print(f"   Wind Alert: {data.get('wind_alert_level', 'N/A')}")
        print(f"   Heat Alert: {data.get('heat_alert_level', 'N/A')}")
        print(f"   Event Time: {data.get('event_time', 'N/A')}")
        print("=" * 80)
        print()

if __name__ == "__main__":
    main()
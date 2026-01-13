from kafka import KafkaConsumer
import json
from datetime import datetime

def format_alert_level(level):
    """Format l'affichage du niveau d'alerte"""
    colors = {
        'level_0': '🟢',
        'level_1': '🟡',
        'level_2': '🔴'
    }
    return colors.get(level, '❓')

def safe_parse_timestamp(timestamp_str):
    """Parse le timestamp de manière sécurisée"""
    if not timestamp_str:
        return datetime.now()
    
    try:
        # Essayer plusieurs formats
        if 'T' in timestamp_str:
            # Format ISO
            timestamp_str = timestamp_str.replace('Z', '+00:00')
            return datetime.fromisoformat(timestamp_str)
        else:
            # Format classique
            return datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
    except Exception as e:
        print(f"Erreur de parsing date: {timestamp_str} - {e}")
        return datetime.now()

def monitor_alerts():
    """Monitore et affiche les alertes de manière lisible"""
    consumer = KafkaConsumer(
        'weather_transformed',
        bootstrap_servers=['kafka:9092'],
        auto_offset_reset='earliest',  # Lire depuis le début
        enable_auto_commit=True,
        group_id='alert-monitor',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    print("=== Monitoring des Alertes Météo ===")
    print("En temps réel depuis weather_transformed\n")
    print(f"{'Heure':<20} {'Ville':<15} {'Pays':<10} {'Temp':<8} {'Vent':<8} {'Alerte Chaleur':<20} {'Alerte Vent':<20}")
    print("="*120)
    
    message_count = 0
    
    try:
        for message in consumer:
            message_count += 1
            
            try:
                data = message.value
                
                # Parse le timestamp
                timestamp = safe_parse_timestamp(data.get('event_time'))
                
                # Extraction des données
                city = data.get('city', 'Unknown')
                country = data.get('country', 'Unknown')
                temp = data.get('temperature', 0)
                wind = data.get('windspeed', 0)
                heat_alert = data.get('heat_alert_level', 'unknown')
                wind_alert = data.get('wind_alert_level', 'unknown')
                
                # Coloration des alertes
                heat_display = f"{format_alert_level(heat_alert)} {heat_alert}"
                wind_display = f"{format_alert_level(wind_alert)} {wind_alert}"
                
                print(f"{timestamp.strftime('%Y-%m-%d %H:%M:%S'):<20} "
                      f"{city:<15} {country:<10} "
                      f"{temp:>6.1f}°C {wind:>6.1f}m/s "
                      f"{heat_display:<20} {wind_display:<20}")
                
                # Alerte spéciale pour les niveaux 2
                if heat_alert == 'level_2' or wind_alert == 'level_2':
                    print(f"  ⚠️  ALERTE NIVEAU 2 détectée pour {city}!")
                
            except Exception as e:
                print(f"Erreur traitement message #{message_count}: {e}")
                print(f"Contenu brut: {message.value}")
            
    except KeyboardInterrupt:
        print(f"\n\nArrêt du monitoring")
        print(f"Messages traités: {message_count}")
    finally:
        consumer.close()

if __name__ == "__main__":
    monitor_alerts()
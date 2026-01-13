import subprocess
import json

def list_hdfs_directory(path):
    """Liste le contenu d'un répertoire HDFS"""
    try:
        result = subprocess.run(
            ['hdfs', 'dfs', '-ls', '-R', path],
            capture_output=True,
            text=True,
            check=True
        )
        return result.stdout
    except Exception as e:
        return f"Erreur: {e}"

def read_hdfs_file(path, lines=10):
    """Lit les premières lignes d'un fichier HDFS"""
    try:
        result = subprocess.run(
            ['hdfs', 'dfs', '-cat', path],
            capture_output=True,
            text=True,
            check=True
        )
        
        # Parser le JSON (chaque ligne est un objet JSON)
        data = []
        for line in result.stdout.strip().split('\n')[:lines]:
            if line:
                try:
                    data.append(json.loads(line))
                except:
                    pass
        return data
    except Exception as e:
        return f"Erreur: {e}"

def count_alerts_by_location():
    """Compte le nombre d'alertes par localisation"""
    try:
        # Lister tous les fichiers alerts.json
        result = subprocess.run(
            ['hdfs', 'dfs', '-ls', '-R', '/hdfs-data'],
            capture_output=True,
            text=True
        )
        
        stats = {}
        for line in result.stdout.split('\n'):
            if 'alerts.json' in line:
                parts = line.split()
                if len(parts) >= 8:
                    path = parts[-1]
                    # Extraire country/city du path
                    path_parts = path.split('/')
                    if len(path_parts) >= 4:
                        country = path_parts[2]
                        city = path_parts[3]
                        
                        # Compter les lignes (alertes)
                        count_result = subprocess.run(
                            ['hdfs', 'dfs', '-cat', path],
                            capture_output=True,
                            text=True
                        )
                        count = len([l for l in count_result.stdout.split('\n') if l.strip()])
                        stats[f"{country}/{city}"] = count
        
        return stats
    except Exception as e:
        return f"Erreur: {e}"

if __name__ == "__main__":
    print("=== Vérification HDFS - Exercice 7 ===\n")
    
    print("1. Structure des répertoires:")
    print("-" * 80)
    listing = list_hdfs_directory('/hdfs-data')
    print(listing)
    
    print("\n2. Statistiques par localisation:")
    print("-" * 80)
    stats = count_alerts_by_location()
    if isinstance(stats, dict):
        total = 0
        for location, count in sorted(stats.items()):
            print(f"  {location}: {count} alertes")
            total += count
        print("-" * 80)
        print(f"  TOTAL: {total} alertes")
    else:
        print(stats)
    
    print("\n3. Exemple d'alertes (première localisation):")
    print("-" * 80)
    if isinstance(stats, dict) and stats:
        first_location = list(stats.keys())[0]
        country, city = first_location.split('/')
        file_path = f"/hdfs-data/{country}/{city}/alerts.json"
        
        print(f"Fichier: {file_path}")
        print(f"Premières 5 alertes:\n")
        
        alerts = read_hdfs_file(file_path, lines=5)
        if isinstance(alerts, list):
            for i, alert in enumerate(alerts, 1):
                print(f"Alerte #{i}:")
                print(f"  Ville: {alert.get('city')}, {alert.get('country')}")
                print(f"  Heure: {alert.get('event_time')}")
                print(f"  Température: {alert.get('temperature')}°C ({alert.get('heat_alert_level')})")
                print(f"  Vent: {alert.get('windspeed')} m/s ({alert.get('wind_alert_level')})")
                print()
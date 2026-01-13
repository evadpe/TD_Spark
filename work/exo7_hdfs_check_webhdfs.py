import requests
import json

class HDFSClient:
    def __init__(self, namenode_host='namenode', namenode_port=9870, user='root'):
        self.base_url = f"http://{namenode_host}:{namenode_port}/webhdfs/v1"
        self.user = user
    
    def list_directory(self, path, recursive=False):
        """Liste le contenu d'un répertoire"""
        url = f"{self.base_url}{path}?op=LISTSTATUS&user.name={self.user}"
        try:
            response = requests.get(url)
            if response.status_code == 200:
                return response.json().get('FileStatuses', {}).get('FileStatus', [])
            return []
        except Exception as e:
            print(f"Erreur: {e}")
            return []
    
    def read_file(self, path, lines=10):
        """Lit un fichier HDFS"""
        url = f"{self.base_url}{path}?op=OPEN&user.name={self.user}"
        try:
            response = requests.get(url, allow_redirects=True)
            if response.status_code == 200:
                content = response.text
                # Retourner les N premières lignes
                all_lines = content.strip().split('\n')
                return all_lines[:lines]
            return []
        except Exception as e:
            print(f"Erreur: {e}")
            return []

def print_tree(hdfs, path, prefix="", is_last=True):
    """Affiche l'arborescence HDFS"""
    files = hdfs.list_directory(path)
    
    for i, file_info in enumerate(files):
        is_last_item = (i == len(files) - 1)
        connector = "└── " if is_last_item else "├── "
        
        file_path = file_info['pathSuffix']
        file_type = file_info['type']
        
        print(f"{prefix}{connector}{file_path}")
        
        if file_type == 'DIRECTORY':
            extension = "    " if is_last_item else "│   "
            new_path = f"{path}/{file_path}" if not path.endswith('/') else f"{path}{file_path}"
            print_tree(hdfs, new_path, prefix + extension, is_last_item)

def count_alerts(hdfs, path="/hdfs-data"):
    """Compte les alertes par localisation"""
    stats = {}
    
    def scan_directory(dir_path, level=0):
        files = hdfs.list_directory(dir_path)
        
        for file_info in files:
            file_name = file_info['pathSuffix']
            file_type = file_info['type']
            full_path = f"{dir_path}/{file_name}"
            
            if file_type == 'DIRECTORY':
                scan_directory(full_path, level + 1)
            elif file_name == 'alerts.json':
                # Compter les lignes
                lines = hdfs.read_file(full_path, lines=999999)
                count = len([l for l in lines if l.strip()])
                
                # Extraire country/city du path
                parts = dir_path.split('/')
                if len(parts) >= 4:
                    country = parts[2]
                    city = parts[3]
                    stats[f"{country}/{city}"] = count
    
    scan_directory(path)
    return stats

if __name__ == "__main__":
    hdfs = HDFSClient()
    
    print("=== Vérification HDFS - Exercice 7 (WebHDFS) ===\n")
    
    print("1. Structure des répertoires:")
    print("-" * 80)
    print("/hdfs-data")
    print_tree(hdfs, "/hdfs-data")
    
    print("\n2. Statistiques par localisation:")
    print("-" * 80)
    stats = count_alerts(hdfs)
    
    if stats:
        total = 0
        for location, count in sorted(stats.items()):
            print(f"  {location}: {count} alertes")
            total += count
        print("-" * 80)
        print(f"  TOTAL: {total} alertes")
    else:
        print("  Aucune donnée trouvée")
    
    print("\n3. Exemple d'alertes (première localisation):")
    print("-" * 80)
    
    if stats:
        first_location = list(stats.keys())[0]
        country, city = first_location.split('/')
        file_path = f"/hdfs-data/{country}/{city}/alerts.json"
        
        print(f"Fichier: {file_path}")
        print(f"Premières 5 alertes:\n")
        
        lines = hdfs.read_file(file_path, lines=5)
        for i, line in enumerate(lines, 1):
            if line.strip():
                try:
                    alert = json.loads(line)
                    print(f"Alerte #{i}:")
                    print(f"  Ville: {alert.get('city')}, {alert.get('country')}")
                    print(f"  Heure: {alert.get('event_time')}")
                    print(f"  Température: {alert.get('temperature')}°C ({alert.get('heat_alert_level')})")
                    print(f"  Vent: {alert.get('windspeed')} m/s ({alert.get('wind_alert_level')})")
                    print()
                except:
                    pass
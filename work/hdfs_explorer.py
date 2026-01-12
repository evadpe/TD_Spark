from hdfs import InsecureClient
import json
import argparse

class HDFSExplorer:
    def __init__(self, hdfs_url="http://namenode:9870", hdfs_user="root"):
        self.hdfs_client = InsecureClient(hdfs_url, user=hdfs_user)
        print(f"✅ Connected to HDFS at {hdfs_url}\n")
    
    def list_directory(self, path="/hdfs-data"):
        """Liste le contenu d'un répertoire HDFS"""
        try:
            print(f"📂 Directory: {path}")
            print("-" * 80)
            
            items = self.hdfs_client.list(path, status=True)
            
            if not items:
                print("   (empty)")
                return
            
            for item_name, status in items:
                item_type = "📁" if status['type'] == 'DIRECTORY' else "📄"
                size = status.get('length', 0)
                size_str = self.format_size(size) if status['type'] == 'FILE' else "-"
                
                print(f"   {item_type} {item_name:<30} {size_str:>10}")
            
            print()
        
        except Exception as e:
            print(f"❌ Error listing directory: {e}\n")
    
    def tree_view(self, path="/hdfs-data", prefix="", max_depth=3, current_depth=0):
        """Affiche une vue arborescente"""
        if current_depth >= max_depth:
            return
        
        try:
            items = self.hdfs_client.list(path, status=True)
            
            for i, (item_name, status) in enumerate(items):
                is_last = (i == len(items) - 1)
                connector = "└── " if is_last else "├── "
                
                if status['type'] == 'DIRECTORY':
                    print(f"{prefix}{connector}📁 {item_name}/")
                    
                    # Récursion pour les sous-répertoires
                    extension = "    " if is_last else "│   "
                    self.tree_view(
                        f"{path}/{item_name}",
                        prefix + extension,
                        max_depth,
                        current_depth + 1
                    )
                else:
                    size = self.format_size(status.get('length', 0))
                    print(f"{prefix}{connector}📄 {item_name} ({size})")
        
        except Exception as e:
            print(f"{prefix}   ❌ Error: {e}")
    
    def read_file(self, path):
        """Lit et affiche le contenu d'un fichier JSON"""
        try:
            print(f"📄 File: {path}")
            print("-" * 80)
            
            with self.hdfs_client.read(path, encoding='utf-8') as reader:
                lines = reader.read().strip().split('\n')
                
                print(f"Total messages: {len(lines)}\n")
                
                for i, line in enumerate(lines[-5:], 1):  # Afficher les 5 derniers
                    if line.strip():
                        data = json.loads(line)
                        print(f"Message {len(lines) - 5 + i}:")
                        print(f"  City: {data.get('city', 'N/A')}")
                        print(f"  Country: {data.get('country', 'N/A')}")
                        print(f"  Temperature: {data.get('temperature', 'N/A')}°C")
                        print(f"  Wind Speed: {data.get('windspeed', 'N/A')} m/s")
                        print(f"  Wind Alert: {data.get('wind_alert_level', 'N/A')}")
                        print(f"  Heat Alert: {data.get('heat_alert_level', 'N/A')}")
                        print(f"  Event Time: {data.get('event_time', 'N/A')}")
                        print()
        
        except Exception as e:
            print(f"❌ Error reading file: {e}")
    
    def stats(self, path="/hdfs-data"):
        """Affiche des statistiques sur les données"""
        print(f"📊 Statistics for {path}")
        print("-" * 80)
        
        stats = {
            'countries': set(),
            'cities': set(),
            'total_files': 0,
            'total_messages': 0,
            'total_size': 0
        }
        
        try:
            self._collect_stats(path, stats)
            
            print(f"Countries: {len(stats['countries'])}")
            for country in sorted(stats['countries']):
                print(f"  - {country}")
            
            print(f"\nCities: {len(stats['cities'])}")
            for city in sorted(stats['cities']):
                print(f"  - {city}")
            
            print(f"\nFiles: {stats['total_files']}")
            print(f"Messages: {stats['total_messages']}")
            print(f"Total size: {self.format_size(stats['total_size'])}")
        
        except Exception as e:
            print(f"❌ Error collecting stats: {e}")
    
    def _collect_stats(self, path, stats):
        """Collecte les statistiques récursivement"""
        try:
            items = self.hdfs_client.list(path, status=True)
            
            for item_name, status in items:
                item_path = f"{path}/{item_name}"
                
                if status['type'] == 'DIRECTORY':
                    # Extraire country/city de la structure
                    parts = item_path.replace('/hdfs-data/', '').split('/')
                    if len(parts) >= 1:
                        stats['countries'].add(parts[0])
                    if len(parts) >= 2:
                        stats['cities'].add(f"{parts[0]}/{parts[1]}")
                    
                    self._collect_stats(item_path, stats)
                else:
                    stats['total_files'] += 1
                    stats['total_size'] += status.get('length', 0)
                    
                    # Compter les messages dans le fichier
                    if item_name.endswith('.json'):
                        try:
                            with self.hdfs_client.read(item_path, encoding='utf-8') as reader:
                                lines = reader.read().strip().split('\n')
                                stats['total_messages'] += len([l for l in lines if l.strip()])
                        except:
                            pass
        
        except Exception:
            pass
    
    @staticmethod
    def format_size(size_bytes):
        """Formate la taille en octets"""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.1f} TB"


def main():
    parser = argparse.ArgumentParser(description="HDFS Explorer for Weather Data")
    parser.add_argument("--hdfs-url", default="http://namenode:9870",
                        help="HDFS NameNode URL")
    parser.add_argument("--hdfs-user", default="root",
                        help="HDFS user")
    
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # Command: list
    list_parser = subparsers.add_parser('list', help='List directory contents')
    list_parser.add_argument('path', nargs='?', default='/hdfs-data',
                             help='Path to list')
    
    # Command: tree
    tree_parser = subparsers.add_parser('tree', help='Show tree view')
    tree_parser.add_argument('path', nargs='?', default='/hdfs-data',
                             help='Root path for tree')
    tree_parser.add_argument('--depth', type=int, default=10,
                             help='Maximum depth')
    
    # Command: read
    read_parser = subparsers.add_parser('read', help='Read file contents')
    read_parser.add_argument('path', help='File path to read')
    
    # Command: stats
    stats_parser = subparsers.add_parser('stats', help='Show statistics')
    stats_parser.add_argument('path', nargs='?', default='/hdfs-data',
                              help='Base path for stats')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    explorer = HDFSExplorer(hdfs_url=args.hdfs_url, hdfs_user=args.hdfs_user)
    
    if args.command == 'list':
        explorer.list_directory(args.path)
    
    elif args.command == 'tree':
        print(f"📂 {args.path}/")
        explorer.tree_view(args.path, max_depth=args.depth)
    
    elif args.command == 'read':
        explorer.read_file(args.path)
    
    elif args.command == 'stats':
        explorer.stats(args.path)


if __name__ == "__main__":
    main()

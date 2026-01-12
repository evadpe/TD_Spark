#!/usr/bin/env python3
"""
Script d'initialisation et de nettoyage HDFS
"""

from hdfs import InsecureClient
import argparse

def cleanup_hdfs(hdfs_url="http://namenode:9870", hdfs_user="root", base_path="/hdfs-data"):
    """Nettoie le répertoire HDFS"""
    print(f"🧹 Cleaning HDFS directory: {base_path}")
    
    try:
        client = InsecureClient(hdfs_url, user=hdfs_user)
        
        # Vérifier si le répertoire existe
        try:
            client.status(base_path)
            print(f"   Found existing directory: {base_path}")
            
            # Supprimer
            client.delete(base_path, recursive=True)
            print(f"   ✅ Deleted {base_path}")
        
        except Exception:
            print(f"   Directory {base_path} does not exist (nothing to clean)")
        
        # Recréer le répertoire
        client.makedirs(base_path)
        print(f"   ✅ Created fresh directory: {base_path}")
        
        print(f"\n✅ HDFS cleanup complete!")
        
    except Exception as e:
        print(f"❌ Error during cleanup: {e}")

def init_hdfs(hdfs_url="http://namenode:9870", hdfs_user="root", base_path="/hdfs-data"):
    """Initialise HDFS avec la structure de base"""
    print(f"🚀 Initializing HDFS at: {base_path}")
    
    try:
        client = InsecureClient(hdfs_url, user=hdfs_user)
        
        # Créer le répertoire de base
        client.makedirs(base_path)
        print(f"   ✅ Created {base_path}")
        
        # Créer quelques répertoires de test
        test_dirs = [
            f"{base_path}/France/Paris",
            f"{base_path}/Germany/Berlin",
            f"{base_path}/United_Kingdom/London"
        ]
        
        for dir_path in test_dirs:
            client.makedirs(dir_path)
            print(f"   ✅ Created {dir_path}")
        
        print(f"\n✅ HDFS initialization complete!")
        
    except Exception as e:
        print(f"❌ Error during initialization: {e}")

def check_hdfs(hdfs_url="http://namenode:9870", hdfs_user="root"):
    """Vérifie l'état de HDFS"""
    print(f"🔍 Checking HDFS connection...")
    
    try:
        client = InsecureClient(hdfs_url, user=hdfs_user)
        
        # Test de connexion
        status = client.status('/')
        print(f"   ✅ HDFS is accessible")
        print(f"   URL: {hdfs_url}")
        print(f"   User: {hdfs_user}")
        
        # Vérifier l'espace disponible
        content_summary = client.content('/')
        space_consumed = content_summary.get('spaceConsumed', 0)
        
        print(f"\n📊 HDFS Status:")
        print(f"   Space consumed: {space_consumed / (1024**2):.2f} MB")
        
        # Lister les répertoires principaux
        print(f"\n📂 Root directories:")
        items = client.list('/', status=False)
        for item in items[:10]:  # Limiter à 10
            print(f"   - {item}")
        
        return True
        
    except Exception as e:
        print(f"   ❌ HDFS connection failed: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description="HDFS Maintenance Tool")
    parser.add_argument("--hdfs-url", default="http://namenode:9870",
                        help="HDFS NameNode URL")
    parser.add_argument("--hdfs-user", default="root",
                        help="HDFS user")
    parser.add_argument("--base-path", default="/hdfs-data",
                        help="Base path for weather data")
    
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # Command: check
    subparsers.add_parser('check', help='Check HDFS connection')
    
    # Command: init
    subparsers.add_parser('init', help='Initialize HDFS structure')
    
    # Command: cleanup
    subparsers.add_parser('cleanup', help='Clean HDFS directory')
    
    # Command: reset
    subparsers.add_parser('reset', help='Cleanup and reinitialize')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    print("=" * 80)
    print("HDFS MAINTENANCE TOOL")
    print("=" * 80)
    print()
    
    if args.command == 'check':
        check_hdfs(args.hdfs_url, args.hdfs_user)
    
    elif args.command == 'init':
        init_hdfs(args.hdfs_url, args.hdfs_user, args.base_path)
    
    elif args.command == 'cleanup':
        cleanup_hdfs(args.hdfs_url, args.hdfs_user, args.base_path)
    
    elif args.command == 'reset':
        cleanup_hdfs(args.hdfs_url, args.hdfs_user, args.base_path)
        print()
        init_hdfs(args.hdfs_url, args.hdfs_user, args.base_path)
    
    print()

if __name__ == "__main__":
    main()

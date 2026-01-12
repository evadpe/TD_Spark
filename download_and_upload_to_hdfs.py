#!/usr/bin/env python3
"""
Script Python pour télécharger les données GSOD et les uploader dans HDFS
Plus robuste que la version bash, avec gestion des erreurs et progression
"""

import os
import subprocess
import tarfile
import requests
from pathlib import Path
from typing import List, Tuple
import sys

# Configuration
YEARS = [2019, 2020, 2021, 2022, 2023]
BASE_URL = "https://www.ncei.noaa.gov/data/global-summary-of-the-day/archive"
LOCAL_DIR = Path("/tmp/gsod_data")
HDFS_DIR = "/data/gsod"
NAMENODE_CONTAINER = "namenode"

class Colors:
    """Couleurs pour le terminal"""
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'

def print_color(text: str, color: str = Colors.ENDC):
    """Affiche du texte en couleur"""
    print(f"{color}{text}{Colors.ENDC}")

def run_hdfs_command(command: List[str], check: bool = True) -> Tuple[bool, str]:
    """Exécute une commande HDFS dans le conteneur namenode"""
    full_command = ["docker", "exec", "-i", NAMENODE_CONTAINER, "hdfs", "dfs"] + command
    try:
        result = subprocess.run(
            full_command,
            capture_output=True,
            text=True,
            check=check
        )
        return True, result.stdout
    except subprocess.CalledProcessError as e:
        return False, e.stderr

def check_docker():
    """Vérifie que Docker est accessible et que le namenode tourne"""
    print_color("\n🔍 Vérification de Docker...", Colors.CYAN)
    
    try:
        result = subprocess.run(
            ["docker", "ps", "--filter", f"name={NAMENODE_CONTAINER}", "--format", "{{.Names}}"],
            capture_output=True,
            text=True,
            check=True
        )
        if NAMENODE_CONTAINER in result.stdout:
            print_color(f"✓ Conteneur {NAMENODE_CONTAINER} en cours d'exécution", Colors.GREEN)
            return True
        else:
            print_color(f"❌ Conteneur {NAMENODE_CONTAINER} non trouvé", Colors.RED)
            print("Assure-toi que docker-compose est lancé: docker-compose up -d")
            return False
    except Exception as e:
        print_color(f"❌ Erreur Docker: {e}", Colors.RED)
        return False

def create_hdfs_directory():
    """Crée le répertoire HDFS pour les données"""
    print_color(f"\n📁 Création du répertoire HDFS: {HDFS_DIR}", Colors.CYAN)
    success, output = run_hdfs_command(["-mkdir", "-p", HDFS_DIR])
    if success or "File exists" in output:
        print_color(f"✓ Répertoire créé/existant", Colors.GREEN)
        return True
    else:
        print_color(f"❌ Erreur: {output}", Colors.RED)
        return False

def download_year(year: int) -> bool:
    """Télécharge l'archive d'une année"""
    url = f"{BASE_URL}/{year}.tar.gz"
    tar_path = LOCAL_DIR / f"{year}.tar.gz"
    
    # Créer le répertoire local
    LOCAL_DIR.mkdir(parents=True, exist_ok=True)
    
    # Vérifier si déjà téléchargé
    if tar_path.exists():
        print_color(f"✓ {year}.tar.gz déjà téléchargé", Colors.YELLOW)
        return True
    
    print_color(f"📥 Téléchargement de {year}.tar.gz...", Colors.CYAN)
    
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        total_size = int(response.headers.get('content-length', 0))
        downloaded = 0
        
        with open(tar_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    
                    # Afficher la progression
                    if total_size > 0:
                        percent = (downloaded / total_size) * 100
                        print(f"\r  Progression: {percent:.1f}% ({downloaded/1024/1024:.1f} MB / {total_size/1024/1024:.1f} MB)", end='')
        
        print()  # Nouvelle ligne après la progression
        print_color(f"✓ {year}.tar.gz téléchargé ({total_size/1024/1024:.1f} MB)", Colors.GREEN)
        return True
        
    except Exception as e:
        print_color(f"❌ Erreur lors du téléchargement: {e}", Colors.RED)
        if tar_path.exists():
            tar_path.unlink()  # Supprimer le fichier partiel
        return False

def extract_year(year: int) -> bool:
    """Extrait l'archive d'une année"""
    tar_path = LOCAL_DIR / f"{year}.tar.gz"
    extract_dir = LOCAL_DIR / str(year)
    
    if extract_dir.exists() and list(extract_dir.glob("*.csv")):
        print_color(f"✓ {year} déjà extrait", Colors.YELLOW)
        return True
    
    print_color(f"📦 Extraction de {year}.tar.gz...", Colors.CYAN)
    
    try:
        extract_dir.mkdir(parents=True, exist_ok=True)
        
        with tarfile.open(tar_path, 'r:gz') as tar:
            members = tar.getmembers()
            total = len(members)
            
            for i, member in enumerate(members):
                tar.extract(member, path=extract_dir)
                
                # Afficher la progression tous les 100 fichiers
                if (i + 1) % 100 == 0 or (i + 1) == total:
                    print(f"\r  Fichiers extraits: {i+1}/{total}", end='')
        
        print()  # Nouvelle ligne
        csv_count = len(list(extract_dir.glob("*.csv")))
        print_color(f"✓ {year} extrait ({csv_count} fichiers CSV)", Colors.GREEN)
        return True
        
    except Exception as e:
        print_color(f"❌ Erreur lors de l'extraction: {e}", Colors.RED)
        return False

def upload_to_hdfs(year: int) -> bool:
    """Upload les données d'une année vers HDFS"""
    extract_dir = LOCAL_DIR / str(year)
    hdfs_year_dir = f"{HDFS_DIR}/{year}"
    
    print_color(f"☁️  Upload vers HDFS: {hdfs_year_dir}", Colors.CYAN)
    
    # Créer le répertoire HDFS pour l'année
    success, _ = run_hdfs_command(["-mkdir", "-p", hdfs_year_dir])
    if not success:
        print_color(f"❌ Impossible de créer {hdfs_year_dir}", Colors.RED)
        return False
    
    try:
        # Compter les fichiers à uploader
        csv_files = list(extract_dir.glob("*.csv"))
        total_files = len(csv_files)
        print(f"  {total_files} fichiers CSV à uploader...")
        
        # Copier le répertoire vers le conteneur
        subprocess.run(
            ["docker", "cp", str(extract_dir), f"{NAMENODE_CONTAINER}:/tmp/"],
            check=True,
            capture_output=True
        )
        
        # Uploader vers HDFS
        subprocess.run(
            ["docker", "exec", "-i", NAMENODE_CONTAINER, "bash", "-c",
             f"hdfs dfs -put -f /tmp/{year}/*.csv {hdfs_year_dir}/"],
            check=True,
            capture_output=True
        )
        
        # Nettoyer le répertoire temporaire dans le conteneur
        subprocess.run(
            ["docker", "exec", "-i", NAMENODE_CONTAINER, "rm", "-rf", f"/tmp/{year}"],
            check=True,
            capture_output=True
        )
        
        # Vérifier l'upload
        success, output = run_hdfs_command(["-ls", hdfs_year_dir])
        if success:
            hdfs_file_count = len([line for line in output.split('\n') if '.csv' in line])
            print_color(f"✓ {year} uploadé ({hdfs_file_count} fichiers dans HDFS)", Colors.GREEN)
            return True
        else:
            print_color(f"⚠️  Upload effectué mais vérification échouée", Colors.YELLOW)
            return True
            
    except Exception as e:
        print_color(f"❌ Erreur lors de l'upload: {e}", Colors.RED)
        return False

def verify_hdfs_data():
    """Vérifie les données dans HDFS"""
    print_color("\n" + "="*50, Colors.BOLD)
    print_color("📊 VÉRIFICATION DES DONNÉES HDFS", Colors.BOLD)
    print_color("="*50, Colors.BOLD)
    
    # Lister la structure
    success, output = run_hdfs_command(["-ls", HDFS_DIR])
    if success:
        print("\n📁 Structure HDFS:")
        print(output)
    
    # Statistiques par année
    print("\n📈 Statistiques par année:")
    for year in YEARS:
        success, output = run_hdfs_command(["-ls", f"{HDFS_DIR}/{year}"])
        if success:
            file_count = len([line for line in output.split('\n') if '.csv' in line])
            print(f"  • {year}: {file_count} fichiers CSV")
    
    # Taille totale
    success, output = run_hdfs_command(["-du", "-s", "-h", HDFS_DIR])
    if success:
        print(f"\n💾 Espace utilisé: {output.strip()}")

def main():
    """Fonction principale"""
    print_color("\n" + "="*50, Colors.HEADER)
    print_color("🌍 TÉLÉCHARGEMENT ET UPLOAD GSOD → HDFS", Colors.HEADER + Colors.BOLD)
    print_color("="*50, Colors.HEADER)
    
    # Vérifications préalables
    if not check_docker():
        sys.exit(1)
    
    if not create_hdfs_directory():
        sys.exit(1)
    
    # Traiter chaque année
    success_count = 0
    for year in YEARS:
        print_color(f"\n{'='*50}", Colors.BLUE)
        print_color(f"📅 Traitement de l'année {year}", Colors.BLUE + Colors.BOLD)
        print_color(f"{'='*50}", Colors.BLUE)
        
        # Télécharger
        if not download_year(year):
            print_color(f"⚠️  Échec du téléchargement de {year}, passage à l'année suivante", Colors.YELLOW)
            continue
        
        # Extraire
        if not extract_year(year):
            print_color(f"⚠️  Échec de l'extraction de {year}, passage à l'année suivante", Colors.YELLOW)
            continue
        
        # Uploader
        if upload_to_hdfs(year):
            success_count += 1
        else:
            print_color(f"⚠️  Échec de l'upload de {year}", Colors.YELLOW)
    
    # Résumé final
    print_color("\n" + "="*50, Colors.HEADER)
    print_color("✅ PROCESSUS TERMINÉ", Colors.HEADER + Colors.BOLD)
    print_color("="*50, Colors.HEADER)
    print(f"\n{success_count}/{len(YEARS)} années traitées avec succès")
    
    if success_count > 0:
        verify_hdfs_data()
        
        print_color("\n" + "="*50, Colors.GREEN)
        print_color("🎯 PROCHAINES ÉTAPES:", Colors.GREEN + Colors.BOLD)
        print_color("="*50, Colors.GREEN)
        print("\n1. Ouvre Jupyter Notebook: http://localhost:8888")
        print("2. Utilise ce chemin HDFS dans ton notebook:")
        print_color(f"   hdfs://namenode:9000{HDFS_DIR}/*/*.csv", Colors.CYAN)
        print("\n3. Interface HDFS: http://localhost:9870")
        
        print_color(f"\n💡 Pour nettoyer les fichiers locaux:", Colors.YELLOW)
        print(f"   rm -rf {LOCAL_DIR}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print_color("\n\n⚠️  Interruption par l'utilisateur", Colors.YELLOW)
        sys.exit(1)
    except Exception as e:
        print_color(f"\n❌ Erreur inattendue: {e}", Colors.RED)
        sys.exit(1)

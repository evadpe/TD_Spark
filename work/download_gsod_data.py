#!/usr/bin/env python3
"""
Script pour télécharger les données GSOD (Global Surface Summary of the Day) de NOAA
Usage: python download_gsod_data.py [année_début] [année_fin]
Exemple: python download_gsod_data.py 2019 2023
"""

import os
import sys
import tarfile
import requests
from pathlib import Path
from tqdm import tqdm

def download_file(url, destination):
    """Télécharge un fichier avec une barre de progression."""
    response = requests.get(url, stream=True)
    total_size = int(response.headers.get('content-length', 0))
    
    with open(destination, 'wb') as file, tqdm(
        desc=destination.name,
        total=total_size,
        unit='iB',
        unit_scale=True,
        unit_divisor=1024,
    ) as progress_bar:
        for data in response.iter_content(chunk_size=1024):
            size = file.write(data)
            progress_bar.update(size)

def download_and_extract_year(year, data_dir):
    """Télécharge et extrait les données pour une année donnée."""
    url = f"https://www.ncei.noaa.gov/data/global-summary-of-the-day/archive/{year}.tar.gz"
    tar_path = data_dir / f"{year}.tar.gz"
    extract_dir = data_dir / str(year)
    
    # Télécharger si pas déjà fait
    if not tar_path.exists():
        print(f"\n📥 Téléchargement de {year}...")
        try:
            download_file(url, tar_path)
            print(f"✓ {year} téléchargé ({tar_path.stat().st_size / (1024*1024):.1f} MB)")
        except Exception as e:
            print(f"❌ Erreur lors du téléchargement de {year}: {e}")
            return False
    else:
        print(f"✓ {year}.tar.gz existe déjà")
    
    # Extraire
    if not extract_dir.exists():
        print(f"📦 Extraction de {year}...")
        try:
            with tarfile.open(tar_path, 'r:gz') as tar:
                tar.extractall(path=extract_dir)
            
            # Compter les fichiers extraits
            csv_files = list(extract_dir.glob("*.csv"))
            print(f"✓ {year} extrait ({len(csv_files)} fichiers CSV)")
        except Exception as e:
            print(f"❌ Erreur lors de l'extraction de {year}: {e}")
            return False
    else:
        csv_files = list(extract_dir.glob("*.csv"))
        print(f"✓ {year} déjà extrait ({len(csv_files)} fichiers CSV)")
    
    return True

def main():
    # Configuration
    if len(sys.argv) == 3:
        start_year = int(sys.argv[1])
        end_year = int(sys.argv[2])
    else:
        # Années par défaut
        start_year = 2019
        end_year = 2023
    
    print(f"🌍 Téléchargement des données GSOD de {start_year} à {end_year}")
    
    # Créer le répertoire de données
    # Pour Docker: /home/jovyan/work/data
    # Pour local: ./data
    if os.path.exists('/home/jovyan'):
        data_dir = Path('/home/jovyan/work/data')
    else:
        data_dir = Path('./data')
    
    data_dir.mkdir(parents=True, exist_ok=True)
    print(f"📁 Répertoire de données: {data_dir.absolute()}")
    
    # Télécharger les données
    years = range(start_year, end_year + 1)
    success_count = 0
    
    for year in years:
        if download_and_extract_year(year, data_dir):
            success_count += 1
    
    print(f"\n{'='*60}")
    print(f"✓ Téléchargement terminé: {success_count}/{len(years)} années")
    print(f"📊 Les données sont prêtes pour l'analyse Spark!")
    print(f"{'='*60}")
    
    # Résumé
    total_size = sum(f.stat().st_size for f in data_dir.glob("*.tar.gz"))
    total_csv = sum(len(list(d.glob("*.csv"))) for d in data_dir.iterdir() if d.is_dir())
    
    print(f"\n📈 Statistiques:")
    print(f"   - Taille totale des archives: {total_size / (1024*1024):.1f} MB")
    print(f"   - Nombre total de fichiers CSV: {total_csv:,}")
    print(f"   - Répertoire: {data_dir.absolute()}")
    
    print(f"\n💡 Pour utiliser ces données dans Spark:")
    print(f"   data_path = '{data_dir.absolute()}/*/*.csv'")
    print(f"   df = spark.read.csv(data_path, header=True, schema=gsod_schema)")

if __name__ == "__main__":
    main()

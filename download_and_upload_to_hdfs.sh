#!/bin/bash

# Script pour télécharger les données GSOD et les uploader dans HDFS
# Usage: ./download_and_upload_to_hdfs.sh

set -e  # Arrêt en cas d'erreur

echo "==========================================="
echo "Téléchargement et Upload GSOD vers HDFS"
echo "==========================================="

# Configuration
YEARS=(2019 2020 2021 2022 2023)
BASE_URL="https://www.ncei.noaa.gov/data/global-summary-of-the-day/archive"
LOCAL_DIR="/tmp/gsod_data"
HDFS_DIR="/data/gsod"

# Créer le répertoire local temporaire
mkdir -p "$LOCAL_DIR"
cd "$LOCAL_DIR"

# Créer le répertoire HDFS
echo ""
echo "Création du répertoire HDFS: $HDFS_DIR"
docker exec -it namenode hdfs dfs -mkdir -p "$HDFS_DIR"

# Télécharger et uploader chaque année
for YEAR in "${YEARS[@]}"; do
    echo ""
    echo "========================================="
    echo "Traitement de l'année: $YEAR"
    echo "========================================="
    
    TAR_FILE="${YEAR}.tar.gz"
    EXTRACT_DIR="${YEAR}"
    
    # Télécharger si nécessaire
    if [ ! -f "$TAR_FILE" ]; then
        echo "📥 Téléchargement de $YEAR..."
        wget -q --show-progress "${BASE_URL}/${TAR_FILE}" -O "$TAR_FILE"
        echo "✓ Téléchargement terminé"
    else
        echo "✓ Archive $YEAR déjà téléchargée"
    fi
    
    # Extraire
    if [ ! -d "$EXTRACT_DIR" ]; then
        echo "📦 Extraction de $TAR_FILE..."
        mkdir -p "$EXTRACT_DIR"
        tar -xzf "$TAR_FILE" -C "$EXTRACT_DIR"
        echo "✓ Extraction terminée"
    else
        echo "✓ Données $YEAR déjà extraites"
    fi
    
    # Upload vers HDFS
    echo "☁️  Upload vers HDFS: ${HDFS_DIR}/${YEAR}/"
    docker exec -i namenode hdfs dfs -mkdir -p "${HDFS_DIR}/${YEAR}"
    
    # Copier tous les fichiers CSV vers HDFS
    # On copie depuis le host vers le conteneur puis vers HDFS
    docker cp "${LOCAL_DIR}/${EXTRACT_DIR}" namenode:/tmp/
    docker exec -i namenode bash -c "hdfs dfs -put -f /tmp/${EXTRACT_DIR}/*.csv ${HDFS_DIR}/${YEAR}/"
    docker exec -i namenode rm -rf "/tmp/${EXTRACT_DIR}"
    
    echo "✓ Upload terminé pour $YEAR"
    
    # Vérifier l'upload
    echo "📊 Vérification des fichiers dans HDFS:"
    docker exec -i namenode hdfs dfs -ls "${HDFS_DIR}/${YEAR}" | head -n 10
done

echo ""
echo "==========================================="
echo "✅ PROCESSUS TERMINÉ AVEC SUCCÈS"
echo "==========================================="
echo ""
echo "📁 Structure HDFS créée:"
docker exec -i namenode hdfs dfs -ls -R "$HDFS_DIR" | grep "^d"
echo ""
echo "📊 Statistiques:"
for YEAR in "${YEARS[@]}"; do
    COUNT=$(docker exec -i namenode hdfs dfs -ls "${HDFS_DIR}/${YEAR}" | grep -c "\.csv" || echo "0")
    echo "  - $YEAR: $COUNT fichiers CSV"
done
echo ""
echo "🎯 Les données sont maintenant disponibles dans HDFS à: hdfs://namenode:9000${HDFS_DIR}"
echo ""
echo "💡 Pour nettoyer les fichiers locaux temporaires:"
echo "   rm -rf $LOCAL_DIR"

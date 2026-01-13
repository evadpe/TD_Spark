# Script pour copier les données locales vers HDFS
Write-Host "=== Copie des données vers HDFS ===" -ForegroundColor Green

$localPath = ".\work\hdfs-data"

# Parcourir tous les répertoires
Get-ChildItem -Path $localPath -Directory | ForEach-Object {
    $country = $_.Name
    
    Get-ChildItem -Path $_.FullName -Directory | ForEach-Object {
        $city = $_.Name
        $alertsFile = Join-Path $_.FullName "alerts.json"
        
        if (Test-Path $alertsFile) {
            $hdfsDir = "/hdfs-data/$country/$city"
            $hdfsFile = "$hdfsDir/alerts.json"
            
            Write-Host "Copie: $country/$city..." -ForegroundColor Yellow
            
            # Créer le répertoire HDFS
            docker exec namenode hdfs dfs -mkdir -p $hdfsDir
            
            # Copier le fichier dans namenode
            docker cp $alertsFile namenode:/tmp/alerts_temp.json
            
            # Copier vers HDFS
            docker exec namenode hdfs dfs -put -f /tmp/alerts_temp.json $hdfsFile
            
            # Nettoyer
            docker exec namenode rm /tmp/alerts_temp.json
            
            Write-Host "  ✓ Copié vers HDFS: $hdfsFile" -ForegroundColor Green
        }
    }
}

Write-Host "`n=== Copie terminée ===" -ForegroundColor Green
Write-Host "Vérification:" -ForegroundColor Cyan
docker exec namenode hdfs dfs -ls -R /hdfs-data
# Script PowerShell pour configurer l'environnement Spark/HDFS
# Usage: .\setup.ps1

param(
    [Parameter(Position=0)]
    [ValidateSet('help', 'start', 'stop', 'restart', 'check', 'download', 'verify', 'logs', 'jupyter', 'token', 'ui', 'clean', 'setup')]
    [string]$Command = 'help'
)

# Couleurs PowerShell
function Write-ColorOutput {
    param(
        [string]$Message,
        [string]$Color = 'White'
    )
    Write-Host $Message -ForegroundColor $Color
}

function Show-Help {
    Write-ColorOutput "`n==========================================" "Cyan"
    Write-ColorOutput "   TP SPARK - ANALYSE CLIMATIQUE" "Cyan"
    Write-ColorOutput "==========================================" "Cyan"
    Write-ColorOutput "`nCommandes disponibles:" "Green"
    Write-ColorOutput "  .\setup.ps1 help       - Affiche cette aide" "Yellow"
    Write-ColorOutput "  .\setup.ps1 start      - Démarre les conteneurs" "Yellow"
    Write-ColorOutput "  .\setup.ps1 stop       - Arrête les conteneurs" "Yellow"
    Write-ColorOutput "  .\setup.ps1 restart    - Redémarre les conteneurs" "Yellow"
    Write-ColorOutput "  .\setup.ps1 check      - Vérifie l'environnement" "Yellow"
    Write-ColorOutput "  .\setup.ps1 download   - Télécharge les données" "Yellow"
    Write-ColorOutput "  .\setup.ps1 verify     - Vérifie les données HDFS" "Yellow"
    Write-ColorOutput "  .\setup.ps1 logs       - Affiche les logs" "Yellow"
    Write-ColorOutput "  .\setup.ps1 jupyter    - Affiche l'URL Jupyter" "Yellow"
    Write-ColorOutput "  .\setup.ps1 token      - Affiche le token Jupyter" "Yellow"
    Write-ColorOutput "  .\setup.ps1 ui         - Affiche toutes les URLs" "Yellow"
    Write-ColorOutput "  .\setup.ps1 clean      - Nettoie les fichiers locaux" "Yellow"
    Write-ColorOutput "  .\setup.ps1 setup      - Installation complète" "Yellow"
    Write-ColorOutput "`n"
}

function Start-Environment {
    Write-ColorOutput "`n🚀 Démarrage des conteneurs..." "Cyan"
    docker-compose up -d
    if ($LASTEXITCODE -eq 0) {
        Write-ColorOutput "✓ Conteneurs démarrés" "Green"
        Write-ColorOutput "⏳ Attendre 30 secondes pour l'initialisation..." "Yellow"
        Start-Sleep -Seconds 30
        Check-Environment
    } else {
        Write-ColorOutput "❌ Erreur lors du démarrage" "Red"
    }
}

function Stop-Environment {
    Write-ColorOutput "`n🛑 Arrêt des conteneurs..." "Cyan"
    docker-compose stop
    if ($LASTEXITCODE -eq 0) {
        Write-ColorOutput "✓ Conteneurs arrêtés" "Green"
    }
}

function Restart-Environment {
    Write-ColorOutput "`n🔄 Redémarrage des conteneurs..." "Cyan"
    docker-compose restart
    Start-Sleep -Seconds 20
    Check-Environment
}

function Check-Environment {
    Write-ColorOutput "`n🔍 Vérification de l'environnement..." "Cyan"
    
    # Vérifier Docker
    Write-ColorOutput "`n1. Docker" "Blue"
    $dockerVersion = docker --version
    if ($dockerVersion) {
        Write-ColorOutput "✓ Docker est installé ($dockerVersion)" "Green"
    } else {
        Write-ColorOutput "❌ Docker n'est pas installé" "Red"
        return
    }
    
    # Vérifier les conteneurs
    Write-ColorOutput "`n2. Conteneurs" "Blue"
    $containers = @("namenode", "datanode", "spark-master", "spark-worker", "pyspark_notebook", "kafka", "zookeeper")
    $allRunning = $true
    
    foreach ($container in $containers) {
        $running = docker ps --filter "name=$container" --format "{{.Names}}" | Select-String -Pattern "^$container$"
        if ($running) {
            Write-ColorOutput "✓ $container est en cours d'exécution" "Green"
        } else {
            Write-ColorOutput "✗ $container n'est PAS en cours d'exécution" "Red"
            $allRunning = $false
        }
    }
    
    if (-not $allRunning) {
        Write-ColorOutput "`n⚠ Certains conteneurs ne sont pas démarrés" "Yellow"
        Write-ColorOutput "   Exécute: .\setup.ps1 start" "Yellow"
    }
    
    # Vérifier HDFS
    Write-ColorOutput "`n3. HDFS" "Blue"
    $hdfsCheck = docker exec namenode hdfs dfsadmin -report 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-ColorOutput "✓ HDFS est accessible" "Green"
        
        $hdfsTest = docker exec namenode hdfs dfs -test -d /data/gsod 2>&1
        if ($LASTEXITCODE -eq 0) {
            Write-ColorOutput "✓ Répertoire /data/gsod existe" "Green"
            
            $fileCount = (docker exec namenode hdfs dfs -ls -R /data/gsod 2>&1 | Select-String "\.csv").Count
            if ($fileCount -gt 0) {
                Write-ColorOutput "✓ $fileCount fichiers CSV trouvés dans HDFS" "Green"
            } else {
                Write-ColorOutput "⚠ Aucun fichier CSV dans HDFS" "Yellow"
                Write-ColorOutput "   Exécute: .\setup.ps1 download" "Yellow"
            }
        } else {
            Write-ColorOutput "⚠ Répertoire /data/gsod n'existe pas" "Yellow"
            Write-ColorOutput "   Exécute: .\setup.ps1 download" "Yellow"
        }
    } else {
        Write-ColorOutput "✗ HDFS n'est pas accessible" "Red"
    }
    
    Write-ColorOutput "`n"
}

function Download-Data {
    Write-ColorOutput "`n📥 Téléchargement et upload des données..." "Cyan"
    
    # Vérifier si Python est disponible
    $pythonCmd = Get-Command python -ErrorAction SilentlyContinue
    if (-not $pythonCmd) {
        $pythonCmd = Get-Command python3 -ErrorAction SilentlyContinue
    }
    
    if ($pythonCmd) {
        Write-ColorOutput "✓ Python trouvé, utilisation du script Python" "Green"
        & $pythonCmd.Source download_and_upload_to_hdfs.py
    } else {
        Write-ColorOutput "⚠ Python non trouvé, utilisation du script bash" "Yellow"
        Write-ColorOutput "Note: Installe Git Bash ou WSL pour une meilleure expérience" "Yellow"
        
        # Essayer avec Git Bash si disponible
        $gitBash = "C:\Program Files\Git\bin\bash.exe"
        if (Test-Path $gitBash) {
            & $gitBash download_and_upload_to_hdfs.sh
        } else {
            Write-ColorOutput "❌ Ni Python ni Git Bash trouvés" "Red"
            Write-ColorOutput "Installe Python depuis: https://www.python.org/downloads/" "Yellow"
        }
    }
}

function Verify-Data {
    Write-ColorOutput "`n📊 Vérification des données HDFS..." "Cyan"
    
    Write-ColorOutput "`n📁 Structure HDFS:" "Blue"
    docker exec namenode hdfs dfs -ls /data/gsod
    
    Write-ColorOutput "`n📈 Statistiques par année:" "Blue"
    $years = @(2019, 2020, 2021, 2022, 2023)
    foreach ($year in $years) {
        $count = (docker exec namenode hdfs dfs -ls "/data/gsod/$year" 2>&1 | Select-String "\.csv").Count
        Write-ColorOutput "  • $year : $count fichiers CSV" "White"
    }
    
    Write-ColorOutput "`n💾 Espace utilisé:" "Blue"
    docker exec namenode hdfs dfs -du -s -h /data/gsod
    Write-ColorOutput "`n"
}

function Show-Logs {
    Write-ColorOutput "`n📋 Logs des conteneurs..." "Cyan"
    docker-compose logs --tail=50
}

function Show-JupyterToken {
    Write-ColorOutput "`n🔑 Token Jupyter:" "Cyan"
    $logs = docker logs pyspark_notebook 2>&1
    $tokenLine = $logs | Select-String "token=" | Select-Object -Last 1
    if ($tokenLine) {
        $token = ($tokenLine -split "token=")[1] -split "&" | Select-Object -First 1
        Write-ColorOutput $token "Green"
        Write-ColorOutput "`nAccède à Jupyter:" "Blue"
        Write-ColorOutput "  http://localhost:8888" "Yellow"
        Write-ColorOutput "  Token: $token" "Yellow"
    } else {
        Write-ColorOutput "❌ Token non trouvé. Le conteneur Jupyter est-il démarré ?" "Red"
    }
    Write-ColorOutput "`n"
}

function Show-UI {
    Write-ColorOutput "`n🌐 Interfaces Web disponibles:" "Cyan"
    Write-ColorOutput "`nJupyter Notebook:" "Green"
    Write-ColorOutput "  http://localhost:8888" "Yellow"
    Write-ColorOutput "`nHDFS NameNode UI:" "Green"
    Write-ColorOutput "  http://localhost:9870" "Yellow"
    Write-ColorOutput "`nSpark Master UI:" "Green"
    Write-ColorOutput "  http://localhost:8080" "Yellow"
    Write-ColorOutput "`n"
}

function Clean-Local {
    Write-ColorOutput "`n🧹 Nettoyage des fichiers locaux..." "Cyan"
    $tempDir = "$env:TEMP\gsod_data"
    if (Test-Path $tempDir) {
        Remove-Item -Recurse -Force $tempDir
        Write-ColorOutput "✓ Fichiers locaux supprimés" "Green"
    } else {
        Write-ColorOutput "✓ Aucun fichier local à supprimer" "Green"
    }
}

function Complete-Setup {
    Write-ColorOutput "`n=========================================" "Cyan"
    Write-ColorOutput "   INSTALLATION COMPLÈTE" "Cyan"
    Write-ColorOutput "=========================================" "Cyan"
    
    Start-Environment
    Write-ColorOutput "`nAttente de 10 secondes supplémentaires..." "Yellow"
    Start-Sleep -Seconds 10
    
    Download-Data
    Verify-Data
    Show-UI
    Show-JupyterToken
    
    Write-ColorOutput "`n=========================================" "Green"
    Write-ColorOutput "   ✅ INSTALLATION TERMINÉE !" "Green"
    Write-ColorOutput "=========================================" "Green"
    Write-ColorOutput "`n"
}

# Exécuter la commande demandée
switch ($Command) {
    'help'     { Show-Help }
    'start'    { Start-Environment }
    'stop'     { Stop-Environment }
    'restart'  { Restart-Environment }
    'check'    { Check-Environment }
    'download' { Download-Data }
    'verify'   { Verify-Data }
    'logs'     { Show-Logs }
    'jupyter'  { Show-JupyterToken }
    'token'    { Show-JupyterToken }
    'ui'       { Show-UI }
    'clean'    { Clean-Local }
    'setup'    { Complete-Setup }
    default    { Show-Help }
}

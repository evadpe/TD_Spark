# Configuration Spark pour HDFS et modifications du notebook
# À copier dans ton notebook Jupyter

# ============================================
# CELLULE 1: Initialisation Spark avec HDFS
# ============================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Créer la session Spark avec configuration HDFS
spark = SparkSession.builder \
    .appName("Analyse Climatique GSOD - HDFS") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

# Configurer le niveau de log
spark.sparkContext.setLogLevel("WARN")

print(f"Spark version: {spark.version}")
print(f"✓ Session Spark initialisée avec support HDFS")

# Vérifier la connexion HDFS
try:
    sc = spark.sparkContext
    hadoop_conf = sc._jsc.hadoopConfiguration()
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
    print(f"✓ Connexion HDFS établie: {fs.getUri()}")
except Exception as e:
    print(f"⚠️  Erreur de connexion HDFS: {e}")

# ============================================
# CELLULE 2: Vérification des données HDFS
# ============================================

import subprocess

# Fonction pour lister les fichiers HDFS
def list_hdfs(path):
    """Liste les fichiers dans un répertoire HDFS"""
    try:
        result = subprocess.run(
            ['docker', 'exec', 'namenode', 'hdfs', 'dfs', '-ls', path],
            capture_output=True,
            text=True
        )
        return result.stdout
    except Exception as e:
        return f"Erreur: {e}"

# Vérifier les données disponibles
print("📁 Contenu de HDFS /data/gsod:")
print(list_hdfs("/data/gsod"))

# ============================================
# CELLULE 3: Définir le schéma (inchangé)
# ============================================

# Définir le schéma pour les données GSOD
gsod_schema = StructType([
    StructField("STATION", StringType(), True),
    StructField("DATE", StringType(), True),
    StructField("LATITUDE", DoubleType(), True),
    StructField("LONGITUDE", DoubleType(), True),
    StructField("ELEVATION", DoubleType(), True),
    StructField("NAME", StringType(), True),
    StructField("TEMP", DoubleType(), True),
    StructField("TEMP_ATTRIBUTES", IntegerType(), True),
    StructField("DEWP", DoubleType(), True),
    StructField("DEWP_ATTRIBUTES", IntegerType(), True),
    StructField("SLP", DoubleType(), True),
    StructField("SLP_ATTRIBUTES", IntegerType(), True),
    StructField("STP", DoubleType(), True),
    StructField("STP_ATTRIBUTES", IntegerType(), True),
    StructField("VISIB", DoubleType(), True),
    StructField("VISIB_ATTRIBUTES", IntegerType(), True),
    StructField("WDSP", DoubleType(), True),
    StructField("WDSP_ATTRIBUTES", IntegerType(), True),
    StructField("MXSPD", DoubleType(), True),
    StructField("GUST", DoubleType(), True),
    StructField("MAX", DoubleType(), True),
    StructField("MAX_ATTRIBUTES", StringType(), True),
    StructField("MIN", DoubleType(), True),
    StructField("MIN_ATTRIBUTES", StringType(), True),
    StructField("PRCP", DoubleType(), True),
    StructField("PRCP_ATTRIBUTES", StringType(), True),
    StructField("SNDP", DoubleType(), True),
    StructField("FRSHTT", StringType(), True)
])

print("✓ Schéma défini")

# ============================================
# CELLULE 4: Charger les données depuis HDFS
# ============================================

# Charger tous les fichiers CSV depuis HDFS
# Utiliser le chemin HDFS complet
hdfs_path = "hdfs://namenode:9000/data/gsod/*/*.csv"

print(f"📥 Chargement des données depuis: {hdfs_path}")

df = spark.read.csv(
    hdfs_path,
    header=True,
    schema=gsod_schema
)

print(f"✓ Données chargées")
print(f"Nombre total d'enregistrements: {df.count():,}")
print(f"Nombre de partitions: {df.rdd.getNumPartitions()}")

# Afficher le schéma
df.printSchema()

# ============================================
# CELLULE 5: Aperçu des données
# ============================================

# Afficher quelques enregistrements
print("\n📊 Aperçu des données:")
df.show(5, truncate=False)

# Statistiques descriptives
print("\n📈 Statistiques descriptives:")
df.select("TEMP", "PRCP", "MAX", "MIN").describe().show()

# ============================================
# NOTE: Le reste du notebook reste identique
# ============================================
# Toutes les analyses et requêtes SQL fonctionneront de la même manière
# car les données sont maintenant dans le DataFrame Spark 'df'

print("\n✅ Configuration HDFS terminée!")
print("Tu peux maintenant exécuter le reste de ton notebook normalement.")

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"
LOCAL_WAREHOUSE = "file:///home/onyxia/work/lab1-bigdata-Khaled-grp6/warehouse"

spark = (
    SparkSession.builder
    .appName("lab-users-exercises")
    .master("local[1]")
    .config("spark.driver.memory", "1g")
    .config("spark.jars.packages", ICEBERG_PACKAGE)
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lab.type", "hadoop")
    .config("spark.sql.catalog.lab.warehouse", LOCAL_WAREHOUSE)
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

print("--- 1. Création de la table Iceberg lab.silver.users ---")
users_bronze = spark.read.csv("users.csv", header=True, inferSchema=False, multiLine=True)
users_typed = users_bronze.select(
    F.col("uuid").cast("string").alias("user_id"),
    F.trim("username").alias("username"),
    F.trim("name").alias("name"),
    F.upper(F.trim("sex")).alias("sex"),
    F.lower(F.trim("mail")).alias("email"),
    F.col("birthdate").cast("date").alias("birthdate"),
)
spark.sql("CREATE NAMESPACE IF NOT EXISTS lab.silver")
users_typed.writeTo("lab.silver.users").createOrReplace()

print("--- 2. Append d'un nouveau batch d'utilisateurs et inspection des snapshots ---")
new_users = users_typed.limit(2).withColumn("user_id", F.expr("uuid()"))
new_users.writeTo("lab.silver.users").append()
spark.sql("SELECT snapshot_id, operation, committed_at FROM lab.silver.users.snapshots ORDER BY committed_at").show(truncate=False)

print("--- 3. Time Travel sur users ---")
snapshots = spark.sql("SELECT snapshot_id FROM lab.silver.users.snapshots ORDER BY committed_at").collect()
first_snapshot = snapshots[0]["snapshot_id"]
print("Comptage avec premier snapshot :")
spark.sql(f"SELECT count(*) FROM lab.silver.users VERSION AS OF {first_snapshot}").show()
print("Comptage actuel :")
spark.sql("SELECT count(*) FROM lab.silver.users").show()

print("--- 4. Évolution du schéma (Ajout de la colonne country) ---")
spark.sql("ALTER TABLE lab.silver.users ADD COLUMN country STRING")
spark.sql("DESCRIBE lab.silver.users").show()

spark.stop()
print("Exercices users terminés avec succès !")

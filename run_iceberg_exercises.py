import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"
LOCAL_WAREHOUSE = "file:///home/onyxia/work/lab1-bigdata-Khaled-grp6/warehouse"

spark = (
    SparkSession.builder
    .appName("lab-iceberg-exercises")
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

# 1. Charger stg_orders existante depuis la table Iceberg
stg_orders = spark.table("lab.silver.orders")

# 2. Créer un second snapshot (Append de nouvelles lignes)
print("--- Ajout de nouvelles commandes (Append) ---")
new_orders = stg_orders.limit(3).withColumn("order_id", F.expr("uuid()"))
new_orders.writeTo("lab.silver.orders").append()

print("Snapshots après append :")
spark.sql("SELECT snapshot_id, operation, committed_at FROM lab.silver.orders.snapshots ORDER BY committed_at").show(truncate=False)

# 3. Time Travel (Requêter le premier snapshot)
snapshots = spark.sql("SELECT snapshot_id FROM lab.silver.orders.snapshots ORDER BY committed_at").collect()
first_snapshot = snapshots[0]["snapshot_id"]

print(f"Comptage avec Time Travel (premier snapshot ID: {first_snapshot}) :")
spark.sql(f"SELECT count(*) FROM lab.silver.orders VERSION AS OF {first_snapshot}").show()

print("Comptage actuel :")
spark.sql("SELECT count(*) FROM lab.silver.orders").show()

# 4. Évolution du schéma (Schema Evolution)
print("--- Évolution du schéma (Ajout de colonne source) ---")
spark.sql("ALTER TABLE lab.silver.orders ADD COLUMN source STRING")
spark.sql("DESCRIBE lab.silver.orders").show()

spark.stop()
print("Exercices Iceberg exécutés avec succès !")

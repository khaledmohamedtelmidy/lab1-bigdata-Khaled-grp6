import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"
LOCAL_WAREHOUSE = "file:///home/onyxia/work/lab1-bigdata-Khaled-grp6/warehouse"

print("Initialisation de Spark (mode léger)...")
spark = (
    SparkSession.builder
    .appName("lab-iceberg-script")
    .master("local[1]")  # Un seul cœur pour consommer le minimum de RAM
    .config("spark.driver.memory", "1g")  # Limitation stricte de la mémoire
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

print("Lecture des fichiers CSV...")
orders_bronze = spark.read.csv("orders.csv", header=True, inferSchema=False, multiLine=True)

# Typage direct sans surcharge mémoire
orders_typed = orders_bronze.select(
    F.col("uuid").cast("string").alias("order_id"),
    F.col("user_uuid").cast("string").alias("user_id"),
    F.col("date").cast("timestamp").alias("ordered_at"),
    F.col("quantity").cast("int").alias("quantity"),
    F.lower(F.trim("product")).alias("product"),
)
stg_orders = orders_typed.dropDuplicates(["order_id"])

print("Création de la table Iceberg lab.silver.orders...")
spark.sql("CREATE NAMESPACE IF NOT EXISTS lab.silver")
stg_orders.writeTo("lab.silver.orders").createOrReplace()

print("Vérification de la table Iceberg :")
spark.sql("SELECT count(*) AS orders, sum(quantity) AS quantity FROM lab.silver.orders").show()

spark.stop()
print("Terminé avec succès sans crash !")

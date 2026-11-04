"""Lab 9 - Gouverner les tables Iceberg avec Polaris.

Usage (racine du repo, terminal Onyxia, apres `source lab09_polaris/env.sh`) :
    python lab09_polaris/run_polaris.py dev            # register + create + schema change
    python lab09_polaris/run_polaris.py analyst        # lectures OK, ecritures refusees
    python lab09_polaris/run_polaris.py snapshots      # verifier (dev) que l'INSERT refuse n'a rien cree
    python lab09_polaris/run_polaris.py exercise1      # supprimer la colonne country
    python lab09_polaris/run_polaris.py gold           # creer le namespace gold (dev) pour l'exercice 2
    python lab09_polaris/run_polaris.py silver_reader  # tester le role silver_reader (exercice 2)
    python lab09_polaris/run_polaris.py cleanup        # nettoyage de fin de lab

Une session Spark = un principal : chaque mode relance donc une session propre.
"""
import os
import sys

from pyspark.sql import SparkSession

ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"
BUCKET = os.environ["LAB_BUCKET_NAME"]
POLARIS_URI = os.environ["POLARIS_URI"]
POLARIS_CATALOG = os.environ["POLARIS_CATALOG"]
REPO_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOCAL_WAREHOUSE = f"file://{REPO_DIR}/warehouse"  # warehouse local du lab 8

PRINCIPAL_BY_MODE = {
    "dev": "dev",
    "analyst": "analyst",
    "snapshots": "dev",
    "exercise1": "dev",
    "gold": "dev",
    "silver_reader": "silver_reader",
    "cleanup": "dev",
}


def build_spark(principal: str) -> SparkSession:
    credential = os.environ[f"POLARIS_{principal.upper()}_CREDENTIAL"]
    spark = (
        SparkSession.builder
        .appName(f"lab-polaris-{principal}")
        .master("local[2]")
        .config("spark.driver.memory", "1g")
        # Acces S3 (identique au lab Spark)
        .config("spark.hadoop.fs.s3a.endpoint", f"https://{os.environ['AWS_S3_ENDPOINT']}")
        .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
        .config("spark.hadoop.fs.s3a.session.token", os.environ.get("AWS_SESSION_TOKEN", ""))
        .config("spark.hadoop.fs.s3a.endpoint.region", os.environ.get("AWS_REGION", "us-east-1"))
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.jars.packages", ICEBERG_PACKAGE)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        # Catalogue Hadoop du lab Spark (dans le bucket)
        .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lab.type", "hadoop")
        .config("spark.sql.catalog.lab.warehouse", f"s3a://{BUCKET}/warehouse")
        # Catalogue Hadoop local (les tables du lab 8 ont ete ecrites ici)
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", LOCAL_WAREHOUSE)
        # Catalogue Polaris (REST)
        .config("spark.sql.catalog.polaris", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.polaris.type", "rest")
        .config("spark.sql.catalog.polaris.uri", f"{POLARIS_URI}/api/catalog")
        .config("spark.sql.catalog.polaris.warehouse", POLARIS_CATALOG)
        .config("spark.sql.catalog.polaris.credential", credential)
        .config("spark.sql.catalog.polaris.scope", "PRINCIPAL_ROLE:ALL")
        # Pas de credential vending sur ce stockage : fichiers lus avec les cles S3A
        .config("spark.sql.catalog.polaris.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    print("=" * 60)
    print("Polaris principal :", principal)
    print("fs.s3.impl        :", spark.sparkContext.getConf().get("spark.hadoop.fs.s3.impl", None))
    print("=" * 60)
    return spark


def title(text: str) -> None:
    print(f"\n----- {text} -----")


def table_exists(spark: SparkSession, catalog_ns: str, table: str) -> bool:
    return spark.sql(f"SHOW TABLES IN {catalog_ns}").filter(f"tableName = '{table}'").count() > 0


def try_sql(spark: SparkSession, statement: str) -> None:
    """Execute une requete qui DOIT echouer et affiche l'erreur."""
    print(f"> {statement}")
    try:
        spark.sql(statement).collect()
        print("  !! Inattendu : la requete a reussi")
    except Exception as exc:  # noqa: BLE001
        first_lines = "\n  ".join(str(exc).splitlines()[:4])
        print(f"  Refuse comme prevu ({type(exc).__name__}) :\n  {first_lines}")


def ensure_lab_tables_in_s3(spark: SparkSession) -> None:
    """Le lab 8 a ecrit silver.users/orders en local : on les recopie dans le bucket si besoin."""
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lab.silver")
    for table in ("orders", "users"):
        if table_exists(spark, "lab.silver", table):
            print(f"lab.silver.{table} deja present dans s3a://{BUCKET}/warehouse")
            continue
        print(f"lab.silver.{table} absent du bucket : copie depuis local.silver.{table}")
        spark.table(f"local.silver.{table}").writeTo(f"lab.silver.{table}").create()


def mode_dev(spark: SparkSession) -> None:
    ensure_lab_tables_in_s3(spark)

    title("Catalogues joignables")
    spark.sql("SHOW NAMESPACES IN lab").show()
    spark.sql("SHOW NAMESPACES IN polaris").show()

    title("Enregistrement de silver.orders dans Polaris")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.silver")
    if table_exists(spark, "polaris.silver", "orders"):
        print("polaris.silver.orders deja enregistree")
    else:
        latest = (
            spark.sql("SELECT file FROM lab.silver.orders.metadata_log_entries ORDER BY timestamp DESC LIMIT 1")
            .first()["file"]
        )
        print("Fichier metadata courant :", latest)
        spark.sql(f"""
            CALL polaris.system.register_table(
              table => 'silver.orders',
              metadata_file => '{latest}'
            )
        """).show(truncate=False)
    spark.sql("SELECT count(*) AS orders FROM polaris.silver.orders").show()

    title("Tables et snapshots via Polaris")
    spark.sql("SHOW TABLES IN polaris.silver").show()
    spark.sql(
        "SELECT snapshot_id, operation, committed_at FROM polaris.silver.orders.snapshots"
    ).show(truncate=False)

    title("Creation de polaris.silver.users (geree par Polaris)")
    spark.table("lab.silver.users").writeTo("polaris.silver.users").createOrReplace()
    spark.sql("SELECT count(*) AS users FROM polaris.silver.users").show()
    print("Location :", spark.sql("DESCRIBE TABLE EXTENDED polaris.silver.users")
          .filter("col_name = 'Location'").first()["data_type"])

    title("Evolution de schema : ADD COLUMN country")
    columns = [r["col_name"] for r in spark.sql("DESCRIBE polaris.silver.users").collect()]
    if "country" not in columns:
        spark.sql("ALTER TABLE polaris.silver.users ADD COLUMN country STRING")
    spark.sql("DESCRIBE polaris.silver.users").show(50)
    spark.sql("SELECT timestamp, file FROM polaris.silver.users.metadata_log_entries").show(truncate=False)


def mode_analyst(spark: SparkSession) -> None:
    title("Lectures en tant qu'analyst")
    spark.sql("SHOW TABLES IN polaris.silver").show()
    spark.sql("SELECT product, count(*) AS n FROM polaris.silver.orders GROUP BY product").show()

    title("Ecritures en tant qu'analyst (doivent echouer : Forbidden)")
    try_sql(spark, "INSERT INTO polaris.silver.orders SELECT * FROM polaris.silver.orders LIMIT 1")
    try_sql(spark, "ALTER TABLE polaris.silver.users ADD COLUMN phone STRING")


def mode_snapshots(spark: SparkSession) -> None:
    title("Snapshots de silver.orders apres l'INSERT refuse")
    spark.sql(
        "SELECT snapshot_id, operation, committed_at FROM polaris.silver.orders.snapshots"
    ).show(truncate=False)


def mode_exercise1(spark: SparkSession) -> None:
    title("Exercice 1 : DROP COLUMN country")
    spark.sql("SELECT snapshot_id, operation, committed_at FROM polaris.silver.users.snapshots").show(truncate=False)
    columns = [r["col_name"] for r in spark.sql("DESCRIBE polaris.silver.users").collect()]
    if "country" in columns:
        spark.sql("ALTER TABLE polaris.silver.users DROP COLUMN country")
    spark.sql("DESCRIBE polaris.silver.users").show(50)
    print("Snapshots apres le DROP COLUMN (aucun nouveau snapshot attendu) :")
    spark.sql("SELECT snapshot_id, operation, committed_at FROM polaris.silver.users.snapshots").show(truncate=False)
    print("Fichiers metadata (un nouveau fichier par changement de schema) :")
    spark.sql("SELECT timestamp, file, latest_schema_id FROM polaris.silver.users.metadata_log_entries").show(truncate=False)


def mode_gold(spark: SparkSession) -> None:
    title("Exercice 2 : creation de polaris.gold (dev)")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.gold")
    spark.sql("""
        CREATE OR REPLACE TABLE polaris.gold.orders_by_product AS
        SELECT product, count(*) AS orders FROM polaris.silver.orders GROUP BY product
    """)
    spark.sql("SELECT * FROM polaris.gold.orders_by_product").show()


def mode_silver_reader(spark: SparkSession) -> None:
    title("Exercice 2 : principal silver_reader")
    print("Lecture silver (doit reussir) :")
    spark.sql("SHOW TABLES IN polaris.silver").show()
    spark.sql("SELECT count(*) AS orders FROM polaris.silver.orders").show()
    print("Lecture gold (doit echouer) :")
    try_sql(spark, "SHOW TABLES IN polaris.gold")
    try_sql(spark, "SELECT * FROM polaris.gold.orders_by_product")
    print("Ecriture silver (doit echouer) :")
    try_sql(spark, "INSERT INTO polaris.silver.orders SELECT * FROM polaris.silver.orders LIMIT 1")


def mode_cleanup(spark: SparkSession) -> None:
    title("Nettoyage")
    spark.sql("DROP TABLE IF EXISTS polaris.silver.users")
    spark.sql("DROP TABLE IF EXISTS polaris.gold.orders_by_product")
    spark.sql("DROP NAMESPACE IF EXISTS polaris.gold")
    spark.sql("SHOW TABLES IN polaris.silver").show()
    print("silver.orders reste enregistree : le lab Trino en a besoin.")


MODES = {
    "dev": mode_dev,
    "analyst": mode_analyst,
    "snapshots": mode_snapshots,
    "exercise1": mode_exercise1,
    "gold": mode_gold,
    "silver_reader": mode_silver_reader,
    "cleanup": mode_cleanup,
}

if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "dev"
    if mode not in MODES:
        sys.exit(f"Mode inconnu '{mode}'. Choix : {', '.join(MODES)}")
    session = build_spark(PRINCIPAL_BY_MODE[mode])
    try:
        MODES[mode](session)
    finally:
        session.stop()

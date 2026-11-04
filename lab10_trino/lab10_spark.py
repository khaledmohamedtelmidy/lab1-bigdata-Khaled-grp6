"""Lab 10 - etapes Spark (session Polaris du lab 9, principal dev).

    python lab10_trino/lab10_spark.py users     # (re)cree polaris.silver.users depuis bronze, adresse decoupee (lab 7)
    python lab10_trino/lab10_spark.py compare   # exercice 5 : orders_by_state_month calcule par Spark

Prerequis : source lab09_polaris/env.sh
"""
import os
import sys

from pyspark.sql import functions as F

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "lab09_polaris"))
from run_polaris import BUCKET, build_spark  # noqa: E402


def mode_users(spark):
    # Le cleanup du lab 9 a supprime polaris.silver.users et la table du lab 8 n'avait pas
    # l'adresse decoupee : on reconstruit silver.users comme dans le lab Spark (lab 7).
    users_bronze = spark.read.csv(f"s3a://{BUCKET}/bronze/users.csv", header=True, multiLine=True)
    users_typed = (
        users_bronze.select(
            F.col("uuid").cast("string").alias("user_id"),
            F.trim("username").alias("username"),
            F.trim("name").alias("name"),
            F.upper(F.trim("sex")).alias("sex"),
            F.lower(F.trim("mail")).alias("email"),
            F.col("birthdate").cast("date").alias("birthdate"),
            F.split(F.col("address"), "\n").alias("address_lines"),
        )
        .withColumn("street", F.element_at("address_lines", 1))
        .withColumn("address_line_2", F.element_at("address_lines", 2))
        .drop("address_lines")
    )
    stg_users = users_typed.select(
        "user_id", "username", "name", "sex", "email", "birthdate", "street",
        F.regexp_extract("address_line_2", "^(.+), [A-Z]{2} [0-9]{5}$", 1).alias("city"),
        F.regexp_extract("address_line_2", "([A-Z]{2}) ([0-9]{5})$", 1).alias("state"),
        F.regexp_extract("address_line_2", "([A-Z]{2}) ([0-9]{5})$", 2).alias("zip_code"),
    ).withColumn("city", F.when(F.col("city") == "", None).otherwise(F.col("city")))

    spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.silver")
    stg_users.writeTo("polaris.silver.users").createOrReplace()
    spark.sql("SELECT count(*) AS users FROM polaris.silver.users").show()
    spark.table("polaris.silver.users").show(5, truncate=False)
    spark.sql("SHOW TABLES IN polaris.silver").show()


def mode_compare(spark):
    df = spark.sql("""
        SELECT u.state, date_trunc('month', o.ordered_at) AS month,
               count(*) AS orders, sum(o.quantity) AS units, count(DISTINCT o.user_id) AS buyers
        FROM polaris.silver.orders o JOIN polaris.silver.users u ON u.user_id = o.user_id
        GROUP BY 1, 2
    """)
    print("Spark - lignes de orders_by_state_month :", df.count())
    print("Spark - total units :", df.agg(F.sum("units")).first()[0])
    gold = spark.table("polaris.gold.orders_by_state_month")
    print("Table gold ecrite par Trino, lue par Spark - lignes :", gold.count())
    print("Difference (Spark EXCEPT gold) :", df.select("state", "month", "orders", "units", "buyers")
          .exceptAll(gold.select("state", "month", "orders", "units", "buyers")).count())


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "users"
    session = build_spark("dev")
    try:
        {"users": mode_users, "compare": mode_compare}[mode](session)
    finally:
        session.stop()

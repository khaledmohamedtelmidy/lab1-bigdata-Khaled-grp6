"""Lab 10 - Trino : silver -> gold (lab 2), exercices, star schema (lab 3).

Usage (racine du repo) :
    export TRINO_HOST=trino TRINO_PORT=8080 TRINO_USER=$KUBERNETES_NAMESPACE
    uv run python lab10_trino/trino_lab.py gold        # lab 2 : exploration, metadata, plans, gold, validation, refresh
    uv run python lab10_trino/trino_lab.py exercises   # exercices 1 a 4
    uv run python lab10_trino/trino_lab.py star        # lab 3 : star schema + validation
"""
import os
import sys
import uuid

import pandas as pd
import trino

pd.set_option("display.width", 200)
pd.set_option("display.max_columns", 20)
pd.set_option("display.max_colwidth", 60)

CATALOG = "lakehouse"
conn = trino.dbapi.connect(
    host=os.environ.get("TRINO_HOST", "trino"),
    port=int(os.environ.get("TRINO_PORT", "8080")),
    user=os.environ.get("TRINO_USER", os.environ.get("KUBERNETES_NAMESPACE", "lab")),
    http_scheme="http",
    catalog=CATALOG,
)


def sql(statement: str) -> pd.DataFrame:
    cur = conn.cursor()
    cur.execute(statement)
    rows = cur.fetchall()  # la requete n'est terminee qu'une fois les resultats lus
    columns = [c[0] for c in cur.description] if cur.description else []
    return pd.DataFrame(rows, columns=columns)


def show(title: str, statement: str) -> pd.DataFrame:
    print(f"\n----- {title} -----")
    df = sql(statement)
    print(df.to_string(index=False) if not df.empty else "(aucune ligne)")
    return df


def plan_summary(title: str, statement: str, keys=("Fragment", "TableScan", "Exchange", "Join", "Input:", "CPU", "Peak")) -> None:
    print(f"\n----- {title} -----")
    text = sql(statement).iloc[0, 0]
    for line in text.splitlines():
        if any(k in line for k in keys):
            print(line.rstrip()[:160])


# --------------------------------------------------------------------------- lab 2
def mode_gold() -> None:
    show("Version", "SELECT version() AS version")
    show("Catalogues", "SHOW CATALOGS")
    show("Schemas de lakehouse", "SHOW SCHEMAS FROM lakehouse")
    show("Tables silver", "SHOW TABLES FROM lakehouse.silver")
    show("DESCRIBE silver.orders", "DESCRIBE lakehouse.silver.orders")
    show("DESCRIBE silver.users", "DESCRIBE lakehouse.silver.users")

    show("Comptages silver", """
        SELECT (SELECT count(*) FROM lakehouse.silver.orders) AS orders,
               (SELECT count(*) FROM lakehouse.silver.users)  AS users""")
    show("Users par state (parsing d'adresse)", """
        SELECT state, count(*) AS users, count_if(city IS NULL) AS missing_city
        FROM lakehouse.silver.users GROUP BY state ORDER BY users DESC, state LIMIT 10""")
    show("Commandes orphelines", """
        SELECT count(*) AS orphan_orders
        FROM lakehouse.silver.orders AS o
        LEFT JOIN lakehouse.silver.users AS u ON u.user_id = o.user_id
        WHERE u.user_id IS NULL""")

    show("orders$snapshots", """
        SELECT snapshot_id, committed_at, operation
        FROM lakehouse.silver."orders$snapshots" ORDER BY committed_at""")
    show("orders$files", """
        SELECT file_path, file_format, record_count, file_size_in_bytes
        FROM lakehouse.silver."orders$files" """)
    show("orders$history", 'SELECT * FROM lakehouse.silver."orders$history"')

    join_q = """
        SELECT u.state, count(*) AS orders
        FROM lakehouse.silver.orders AS o
        JOIN lakehouse.silver.users  AS u ON u.user_id = o.user_id
        GROUP BY u.state"""
    plan_summary("EXPLAIN (TYPE DISTRIBUTED) - lignes cles", f"EXPLAIN (TYPE DISTRIBUTED) {join_q}")
    plan_summary("EXPLAIN ANALYZE - lignes cles", f"EXPLAIN ANALYZE {join_q}")
    try:
        show("Historique des requetes", """
            SELECT query_id, state,
                   date_diff('millisecond', created, coalesce("end", current_timestamp)) AS elapsed_ms,
                   substr(query, 1, 60) AS query
            FROM system.runtime.queries ORDER BY created DESC LIMIT 5""")
    except Exception as exc:  # noqa: BLE001
        print("system.runtime.queries refuse :", str(exc).splitlines()[0])

    print("\n===== GOLD =====")
    sql("CREATE SCHEMA IF NOT EXISTS lakehouse.gold")
    show("Schemas apres CREATE SCHEMA", "SHOW SCHEMAS FROM lakehouse")

    show("CTAS orders_by_state_month (lignes ecrites)", """
        CREATE OR REPLACE TABLE lakehouse.gold.orders_by_state_month AS
        SELECT u.state,
               date_trunc('month', o.ordered_at) AS month,
               count(*)                          AS orders,
               sum(o.quantity)                   AS units,
               count(DISTINCT o.user_id)         AS buyers
        FROM lakehouse.silver.orders AS o
        JOIN lakehouse.silver.users  AS u ON u.user_id = o.user_id
        GROUP BY 1, 2""")
    show("orders_by_state_month", "SELECT * FROM lakehouse.gold.orders_by_state_month ORDER BY month, state LIMIT 10")

    show("CTAS top_products", """
        CREATE OR REPLACE TABLE lakehouse.gold.top_products AS
        SELECT product, count(*) AS orders, sum(quantity) AS units,
               rank() OVER (ORDER BY sum(quantity) DESC) AS units_rank
        FROM lakehouse.silver.orders GROUP BY product""")
    show("top_products", "SELECT * FROM lakehouse.gold.top_products ORDER BY units_rank LIMIT 10")

    show("CTAS user_activity", """
        CREATE OR REPLACE TABLE lakehouse.gold.user_activity AS
        SELECT u.user_id, u.state,
               date_diff('year', u.birthdate, current_date) AS age,
               count(o.order_id)            AS orders,
               coalesce(sum(o.quantity), 0) AS units,
               min(o.ordered_at)            AS first_order_at,
               max(o.ordered_at)            AS last_order_at
        FROM lakehouse.silver.users AS u
        LEFT JOIN lakehouse.silver.orders AS o ON o.user_id = u.user_id
        GROUP BY u.user_id, u.state, u.birthdate""")
    show("user_activity", "SELECT * FROM lakehouse.gold.user_activity ORDER BY orders DESC LIMIT 10")

    show("Tables gold", "SHOW TABLES FROM lakehouse.gold")
    show("top_products$files", 'SELECT file_path, record_count, file_size_in_bytes FROM lakehouse.gold."top_products$files"')

    print("\n===== VALIDATION =====")
    silver_units = sql("SELECT sum(quantity) AS v FROM lakehouse.silver.orders").iloc[0, 0]
    gold_units = sql("SELECT sum(units) AS v FROM lakehouse.gold.orders_by_state_month").iloc[0, 0]
    product_units = sql("SELECT sum(units) AS v FROM lakehouse.gold.top_products").iloc[0, 0]
    silver_users = sql("SELECT count(*) AS v FROM lakehouse.silver.users").iloc[0, 0]
    gold_users = sql("SELECT count(*) AS v FROM lakehouse.gold.user_activity").iloc[0, 0]
    silver_orders = sql("SELECT count(*) AS v FROM lakehouse.silver.orders").iloc[0, 0]
    gold_orders = sql("SELECT sum(orders) AS v FROM lakehouse.gold.user_activity").iloc[0, 0]
    checks = {
        "silver_units == gold_units (orders_by_state_month)": (silver_units, gold_units),
        "silver_units == product_units (top_products)": (silver_units, product_units),
        "silver_users == gold_users (user_activity)": (silver_users, gold_users),
        "silver_orders == gold_orders (user_activity)": (silver_orders, gold_orders),
    }
    for name, (a, b) in checks.items():
        print(f"{'OK ' if a == b else 'KO '} {name}: {a} vs {b}")

    print("\n===== REFRESH top_products =====")
    sql("DELETE FROM lakehouse.gold.top_products")
    sql("""
        INSERT INTO lakehouse.gold.top_products
        SELECT product, count(*), sum(quantity), rank() OVER (ORDER BY sum(quantity) DESC)
        FROM lakehouse.silver.orders GROUP BY product""")
    snaps = show("top_products$snapshots", """
        SELECT snapshot_id, committed_at, operation
        FROM lakehouse.gold."top_products$snapshots" ORDER BY committed_at""")
    first = snaps.iloc[0, 0]
    show("Time travel : premier snapshot", f"SELECT count(*) AS products FROM lakehouse.gold.top_products FOR VERSION AS OF {first}")
    show("Etat courant", "SELECT count(*) AS products FROM lakehouse.gold.top_products")


# --------------------------------------------------------------------------- exercices
def mode_exercises() -> None:
    print("===== Exercice 1 : table partitionnee par mois =====")
    sql("""
        CREATE OR REPLACE TABLE lakehouse.gold.orders_by_state_month_part
        WITH (partitioning = ARRAY['month(month)']) AS
        SELECT * FROM lakehouse.gold.orders_by_state_month""")
    show("Fichiers : non partitionnee", """
        SELECT count(*) AS files, sum(record_count) AS records
        FROM lakehouse.gold."orders_by_state_month$files" """)
    show("Fichiers : partitionnee", """
        SELECT count(*) AS files, sum(record_count) AS records
        FROM lakehouse.gold."orders_by_state_month_part$files" """)
    show("Partitions", 'SELECT partition, record_count, file_count FROM lakehouse.gold."orders_by_state_month_part$partitions" ORDER BY 1')
    one_month = sql("SELECT min(month) AS m FROM lakehouse.gold.orders_by_state_month").iloc[0, 0]
    flt = f"WHERE month = from_iso8601_timestamp('{pd.Timestamp(one_month).isoformat()}')"
    for t in ("orders_by_state_month", "orders_by_state_month_part"):
        plan_summary(f"EXPLAIN ANALYZE filtre 1 mois : {t}",
                     f"EXPLAIN ANALYZE SELECT sum(units) FROM lakehouse.gold.{t} {flt}",
                     keys=("TableScan", "Input:", "ScanFilter"))

    print("\n===== Exercice 2 : cohortes par decennie de naissance =====")
    sql("""
        CREATE OR REPLACE TABLE lakehouse.gold.cohorts AS
        SELECT (year(u.birthdate) / 10) * 10                    AS birth_decade,
               count(DISTINCT u.user_id)                        AS users,
               count(o.order_id)                                AS orders,
               round(avg(CAST(o.quantity AS double)), 2)        AS avg_units_per_order
        FROM lakehouse.silver.users AS u
        LEFT JOIN lakehouse.silver.orders AS o ON o.user_id = u.user_id
        GROUP BY 1""")
    show("gold.cohorts", "SELECT * FROM lakehouse.gold.cohorts ORDER BY birth_decade")

    print("\n===== Exercice 3 : INSERT silver + MERGE gold + time travel =====")
    before = sql("""SELECT snapshot_id FROM lakehouse.gold."orders_by_state_month$snapshots"
                    ORDER BY committed_at DESC LIMIT 1""").iloc[0, 0]
    new_id = str(uuid.uuid4())
    sql(f"""
        INSERT INTO lakehouse.silver.orders (order_id, user_id, ordered_at, quantity, product)
        SELECT '{new_id}', user_id, current_timestamp, 7, 'croissant'
        FROM lakehouse.silver.users ORDER BY user_id LIMIT 1""")
    print("Commande ajoutee :", new_id)
    sql("""
        MERGE INTO lakehouse.gold.orders_by_state_month AS t
        USING (
            SELECT u.state, date_trunc('month', o.ordered_at) AS month,
                   count(*) AS orders, sum(o.quantity) AS units, count(DISTINCT o.user_id) AS buyers
            FROM lakehouse.silver.orders AS o
            JOIN lakehouse.silver.users  AS u ON u.user_id = o.user_id
            GROUP BY 1, 2
        ) AS s
        ON t.state = s.state AND t.month = s.month
        WHEN MATCHED AND (t.orders <> s.orders OR t.units <> s.units OR t.buyers <> s.buyers)
            THEN UPDATE SET orders = s.orders, units = s.units, buyers = s.buyers
        WHEN NOT MATCHED
            THEN INSERT (state, month, orders, units, buyers) VALUES (s.state, s.month, s.orders, s.units, s.buyers)""")
    show("Snapshots gold apres MERGE", """
        SELECT snapshot_id, committed_at, operation
        FROM lakehouse.gold."orders_by_state_month$snapshots" ORDER BY committed_at""")
    show("Avant (FOR VERSION AS OF) vs apres", f"""
        SELECT 'avant' AS etat, count(*) AS rows_, sum(units) AS units
        FROM lakehouse.gold.orders_by_state_month FOR VERSION AS OF {before}
        UNION ALL
        SELECT 'apres', count(*), sum(units) FROM lakehouse.gold.orders_by_state_month""")
    show("Controle silver", "SELECT sum(quantity) AS silver_units FROM lakehouse.silver.orders")

    print("\n===== Exercice 4 : optimize top_products =====")
    snap_q = 'SELECT snapshot_id, committed_at, operation FROM lakehouse.gold."top_products$snapshots" ORDER BY committed_at'
    files_q = 'SELECT count(*) AS files, sum(record_count) AS records, sum(file_size_in_bytes) AS bytes FROM lakehouse.gold."top_products$files"'
    show("Snapshots avant", snap_q)
    show("Fichiers avant", files_q)
    sql("ALTER TABLE lakehouse.gold.top_products EXECUTE optimize")
    show("Snapshots apres", snap_q)
    show("Fichiers apres", files_q)

    show("Exercice 5 (cote Trino) : lignes de orders_by_state_month",
         "SELECT count(*) AS rows_, sum(units) AS units FROM lakehouse.gold.orders_by_state_month")


# --------------------------------------------------------------------------- lab 3
def mode_star() -> None:
    sql("CREATE SCHEMA IF NOT EXISTS lakehouse.gold")
    sql("""
        CREATE OR REPLACE TABLE lakehouse.gold.dim_user AS
        SELECT row_number() OVER (ORDER BY user_id) AS user_key,
               user_id, username, name, sex, birthdate, city, state, zip_code
        FROM lakehouse.silver.users""")
    sql("""
        CREATE OR REPLACE TABLE lakehouse.gold.dim_product AS
        SELECT row_number() OVER (ORDER BY product) AS product_key, product
        FROM (SELECT DISTINCT product FROM lakehouse.silver.orders)""")
    first_day, last_day = sql("""
        SELECT CAST(min(ordered_at) AS date) AS first_day, CAST(max(ordered_at) AS date) AS last_day
        FROM lakehouse.silver.orders""").iloc[0]
    print("Plage de dates :", first_day, "->", last_day)
    sql(f"""
        CREATE OR REPLACE TABLE lakehouse.gold.dim_date AS
        SELECT CAST(date_format(CAST(d AS timestamp), '%Y%m%d') AS integer) AS date_key,
               d                                       AS full_date,
               year(d)                                 AS year,
               quarter(d)                              AS quarter,
               month(d)                                AS month,
               date_format(CAST(d AS timestamp), '%M') AS month_name,
               day_of_week(d)                          AS day_of_week,
               day_of_week(d) IN (6, 7)                AS is_weekend
        FROM UNNEST(sequence(DATE '{first_day}', DATE '{last_day}', INTERVAL '1' DAY)) AS t(d)""")
    sql("""
        CREATE OR REPLACE TABLE lakehouse.gold.fact_orders AS
        SELECT o.order_id, u.user_key, p.product_key,
               CAST(date_format(o.ordered_at, '%Y%m%d') AS integer) AS date_key,
               o.ordered_at, o.quantity
        FROM lakehouse.silver.orders AS o
        LEFT JOIN lakehouse.gold.dim_user    AS u ON u.user_id = o.user_id
        LEFT JOIN lakehouse.gold.dim_product AS p ON p.product = o.product""")
    show("Volumetrie du star schema", """
        SELECT 'dim_user' AS table_name, count(*) AS row_count FROM lakehouse.gold.dim_user
        UNION ALL SELECT 'dim_product', count(*) FROM lakehouse.gold.dim_product
        UNION ALL SELECT 'dim_date', count(*) FROM lakehouse.gold.dim_date
        UNION ALL SELECT 'fact_orders', count(*) FROM lakehouse.gold.fact_orders""")
    show("Top 10 annee x state x produit", """
        SELECT d.year, u.state, p.product, count(*) AS orders, sum(f.quantity) AS units
        FROM lakehouse.gold.fact_orders AS f
        JOIN lakehouse.gold.dim_date    AS d ON d.date_key    = f.date_key
        JOIN lakehouse.gold.dim_user    AS u ON u.user_key    = f.user_key
        JOIN lakehouse.gold.dim_product AS p ON p.product_key = f.product_key
        GROUP BY d.year, u.state, p.product ORDER BY units DESC LIMIT 10""")
    show("orders_by_state_month reconstruit depuis le star", """
        SELECT u.state, d.year, d.month, count(*) AS orders, sum(f.quantity) AS units,
               count(DISTINCT f.user_key) AS buyers
        FROM lakehouse.gold.fact_orders AS f
        JOIN lakehouse.gold.dim_date    AS d ON d.date_key = f.date_key
        JOIN lakehouse.gold.dim_user    AS u ON u.user_key = f.user_key
        GROUP BY u.state, d.year, d.month ORDER BY d.year, d.month, u.state LIMIT 10""")

    print("\n===== VALIDATION STAR =====")
    silver_orders = sql("SELECT count(*) AS v FROM lakehouse.silver.orders").iloc[0, 0]
    fact_orders = sql("SELECT count(*) AS v FROM lakehouse.gold.fact_orders").iloc[0, 0]
    silver_units = sql("SELECT sum(quantity) AS v FROM lakehouse.silver.orders").iloc[0, 0]
    fact_units = sql("SELECT sum(quantity) AS v FROM lakehouse.gold.fact_orders").iloc[0, 0]
    unmatched = sql("""
        SELECT count(*) AS v
        FROM lakehouse.gold.fact_orders AS f
        LEFT JOIN lakehouse.gold.dim_user    AS u ON u.user_key    = f.user_key
        LEFT JOIN lakehouse.gold.dim_product AS p ON p.product_key = f.product_key
        LEFT JOIN lakehouse.gold.dim_date    AS d ON d.date_key    = f.date_key
        WHERE u.user_key IS NULL OR p.product_key IS NULL OR d.date_key IS NULL""").iloc[0, 0]
    for name, ok, detail in (
        ("silver_orders == fact_orders", silver_orders == fact_orders, f"{silver_orders} vs {fact_orders}"),
        ("silver_units == fact_units", silver_units == fact_units, f"{silver_units} vs {fact_units}"),
        ("unmatched == 0", unmatched == 0, f"{unmatched}"),
    ):
        print(f"{'OK ' if ok else 'KO '} {name}: {detail}")
    show("Controles supplementaires", """
        SELECT (SELECT count(*) - count(DISTINCT order_id) FROM lakehouse.gold.fact_orders) AS duplicate_orders,
               (SELECT count(*) - count(DISTINCT user_id) FROM lakehouse.gold.dim_user)     AS duplicate_users,
               (SELECT count(*) FROM lakehouse.gold.fact_orders f
                  JOIN lakehouse.gold.dim_date d ON d.date_key = f.date_key
                 WHERE d.full_date <> CAST(f.ordered_at AS date))                            AS wrong_date_key""")
    conn.close()


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "gold"
    {"gold": mode_gold, "exercises": mode_exercises, "star": mode_star}[mode]()

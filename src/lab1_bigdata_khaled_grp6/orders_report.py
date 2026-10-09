import argparse
import os
import duckdb

def orders_report(product=None):
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    # Lecture locale ou via variable d'environnement
    bucket = os.environ.get("LAB_BUCKET_NAME", ".")
    bronze_path = f"s3://{bucket}/bronze/orders.csv" if "LAB_BUCKET_NAME" in os.environ and os.environ.get("S3_ENDPOINT_URL") else "orders.csv"

    return con.execute(
        """
        SELECT strftime(date, '%Y-%m') AS month, count(*) AS orders, sum(quantity) AS quantity
        FROM read_csv(?, strict_mode = false)
        WHERE ? IS NULL OR product = ?
        GROUP BY month
        ORDER BY month
        """,
        [bronze_path, product, product],
    ).fetchall()

def main():
    parser = argparse.ArgumentParser(prog="orders-report", description="Monthly orders report")
    parser.add_argument("-p", "--product", help="Filter the orders on a product.")
    args = parser.parse_args()
    for month, orders, quantity in orders_report(args.product):
        print(f"{month}\t{orders}\t{quantity}")

if __name__ == "__main__":
    main()

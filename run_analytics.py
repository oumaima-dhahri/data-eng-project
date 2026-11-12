from pathlib import Path
from getpass import getpass
import pandas as pd
import psycopg

output_dir = Path(__file__).resolve().parent / "outputs"
output_dir.mkdir(exist_ok=True)

password = getpass("PostgreSQL password: ")

queries = {
    "kpis": """
        SELECT COUNT(*) AS total_order_items,
               COUNT(DISTINCT order_id) AS total_orders,
               ROUND(SUM(price), 2) AS product_sales,
               ROUND(SUM(freight_value), 2) AS total_freight,
               ROUND(AVG(price), 2) AS average_item_price
        FROM dw.fact_order_items;
    """,
    "monthly_sales": """
        SELECT DATE_TRUNC('month', d.full_date)::date AS month,
               COUNT(DISTINCT f.order_id) AS total_orders,
               ROUND(SUM(f.price), 2) AS product_sales,
               ROUND(SUM(f.freight_value), 2) AS freight
        FROM dw.fact_order_items f
        JOIN dw.dim_date d ON f.purchase_date_key = d.date_key
        GROUP BY 1 ORDER BY 1;
    """,
    "top_categories": """
        SELECT COALESCE(p.product_category_name, 'Unknown') AS category,
               COUNT(*) AS items_sold,
               ROUND(SUM(f.price), 2) AS product_sales
        FROM dw.fact_order_items f
        JOIN dw.dim_products p ON f.product_key = p.product_key
        GROUP BY 1 ORDER BY 3 DESC LIMIT 10;
    """,
    "top_sellers": """
        SELECT s.seller_id,
               COUNT(*) AS items_sold,
               ROUND(SUM(f.price), 2) AS product_sales
        FROM dw.fact_order_items f
        JOIN dw.dim_sellers s ON f.seller_key = s.seller_key
        GROUP BY 1 ORDER BY 3 DESC LIMIT 10;
    """
}

try:
    with psycopg.connect(
        host="localhost", port=5433, dbname="olist_dw",
        user="postgres", password=password
    ) as conn:
        for name, sql in queries.items():
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
                columns = [c.name for c in cur.description]

            df = pd.DataFrame(rows, columns=columns)
            path = output_dir / f"{name}.csv"
            df.to_csv(path, index=False)
            print(f"\n{name}:")
            print(df.to_string(index=False))
            print(f"Saved: {path}")

    print("\nSUCCESS: All reports generated.")
except Exception as e:
    print(f"\nERROR: {e}")

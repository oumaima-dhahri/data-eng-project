import os
import psycopg

password = os.environ.get("PGPASSWORD")
if not password:
    raise RuntimeError("PGPASSWORD environment variable is not set.")

with psycopg.connect(
    host="172.30.0.1",
    port=5433,
    dbname="olist_dw",
    user="postgres",
    password=password,
) as conn:

    with conn.cursor() as cur:

        # 1. Check total rows
        cur.execute("""
            SELECT COUNT(*)
            FROM dw.fact_order_items;
        """)
        total_rows = cur.fetchone()[0]

        # 2. Check missing required values
        cur.execute("""
            SELECT COUNT(*)
            FROM dw.fact_order_items
            WHERE order_id IS NULL
               OR customer_key IS NULL
               OR product_key IS NULL
               OR seller_key IS NULL
               OR purchase_date_key IS NULL;
        """)
        missing_rows = cur.fetchone()[0]

        # 3. Check duplicate order items
        cur.execute("""
            SELECT COUNT(*)
            FROM (
                SELECT order_id, order_item_id
                FROM dw.fact_order_items
                GROUP BY order_id, order_item_id
                HAVING COUNT(*) > 1
            ) AS duplicates;
        """)
        duplicate_groups = cur.fetchone()[0]

        # 4. Check negative prices or freight
        cur.execute("""
            SELECT COUNT(*)
            FROM dw.fact_order_items
            WHERE price < 0
               OR freight_value < 0;
        """)
        negative_rows = cur.fetchone()[0]

        # 5. Check missing customers in dimension
        cur.execute("""
            SELECT COUNT(*)
            FROM dw.fact_order_items f
            LEFT JOIN dw.dim_customers d
                ON f.customer_key = d.customer_key
            WHERE d.customer_key IS NULL;
        """)
        missing_customers = cur.fetchone()[0]

        # 6. Check missing products in dimension
        cur.execute("""
            SELECT COUNT(*)
            FROM dw.fact_order_items f
            LEFT JOIN dw.dim_products d
                ON f.product_key = d.product_key
            WHERE d.product_key IS NULL;
        """)
        missing_products = cur.fetchone()[0]

        # 7. Check missing sellers in dimension
        cur.execute("""
            SELECT COUNT(*)
            FROM dw.fact_order_items f
            LEFT JOIN dw.dim_sellers d
                ON f.seller_key = d.seller_key
            WHERE d.seller_key IS NULL;
        """)
        missing_sellers = cur.fetchone()[0]

        # 8. Check missing dates in dimension
        cur.execute("""
            SELECT COUNT(*)
            FROM dw.fact_order_items f
            LEFT JOIN dw.dim_date d
                ON f.purchase_date_key = d.date_key
            WHERE d.date_key IS NULL;
        """)
        missing_dates = cur.fetchone()[0]


print("\n========== DATA QUALITY REPORT ==========")
print(f"Total rows: {total_rows}")
print(f"Rows with missing required values: {missing_rows}")
print(f"Duplicate order-item groups: {duplicate_groups}")
print(f"Rows with negative prices/freight: {negative_rows}")
print(f"Customers missing from dimension: {missing_customers}")
print(f"Products missing from dimension: {missing_products}")
print(f"Sellers missing from dimension: {missing_sellers}")
print(f"Dates missing from dimension: {missing_dates}")

errors = []

if total_rows == 0:
    errors.append("The fact table is empty.")

if missing_rows > 0:
    errors.append("Missing required values detected.")

if duplicate_groups > 0:
    errors.append("Duplicate order items detected.")

if negative_rows > 0:
    errors.append("Negative prices or freight detected.")

if missing_customers > 0:
    errors.append("Customers missing from dimension.")

if missing_products > 0:
    errors.append("Products missing from dimension.")

if missing_sellers > 0:
    errors.append("Sellers missing from dimension.")

if missing_dates > 0:
    errors.append("Dates missing from dimension.")

if errors:
    print("\nDATA QUALITY CHECKS FAILED:")
    for error in errors:
        print(f"- {error}")
    raise ValueError("Data quality checks failed.")

print("\nAll 8 data quality checks passed!")

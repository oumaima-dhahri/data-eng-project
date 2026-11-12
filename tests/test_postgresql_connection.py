
import os
import pytest
import psycopg2


def test_postgresql_connection():
    password = os.environ.get("PGPASSWORD")

    if not password:
        pytest.skip("PGPASSWORD environment variable is not set.")

    conn = psycopg2.connect(
        host="172.30.0.1",
        port=5433,
        dbname="olist_dw",
        user="postgres",
        password=password,
    )

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT current_database();")
            result = cur.fetchone()

        assert result[0] == "olist_dw"

    finally:
        conn.close()

from pathlib import Path
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip

PROJECT_DIR = Path(__file__).resolve().parents[3]
DELTA_PATH = PROJECT_DIR / "outputs/streaming/delta/orders"

builder = (
    SparkSession.builder
    .appName("Olist-Streaming-Data-Quality")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

try:
    if not DELTA_PATH.exists():
        raise RuntimeError("La table Delta Lake n'existe pas.")

    df = spark.read.format("delta").load(str(DELTA_PATH))

    expected_columns = {
        "event_id", "order_id", "customer_id",
        "order_status", "order_amount", "event_timestamp"
    }

    actual_columns = set(df.columns)
    missing_columns = expected_columns - actual_columns

    total = df.count()

    duplicates = (
        df.groupBy("event_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    missing_values = df.filter(
        F.col("event_id").isNull()
        | F.col("order_id").isNull()
        | F.col("customer_id").isNull()
        | F.col("order_status").isNull()
        | F.col("order_amount").isNull()
        | F.col("event_timestamp").isNull()
    ).count()

    checks = {
        "Table Delta accessible": True,
        "Colonnes attendues présentes": not missing_columns,
        "Au moins une commande": total > 0,
        "Aucun event_id dupliqué": duplicates == 0,
        "Aucune valeur obligatoire manquante": missing_values == 0
    }

    print("\n===== OLIST STREAMING DATA QUALITY =====")
    print(f"Nombre de commandes : {total}")

    for name, passed in checks.items():
        print(f"[{'PASS' if passed else 'FAIL'}] {name}")

    if missing_columns:
        print("Colonnes manquantes :", missing_columns)

    print("\n===== RESULTAT FINAL =====")
    print("TOUS LES TESTS SONT PASSES" if all(checks.values())
          else "CERTAINS TESTS ONT ECHOUE")

    if not all(checks.values()):
        sys.exit(1)

finally:
    spark.stop()

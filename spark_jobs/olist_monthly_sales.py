
import csv
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


PROJECT_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_DIR / "data" / "raw"
OUTPUT_DIR = PROJECT_DIR / "outputs" / "pyspark" / "monthly_sales"


def main():
    spark = (
        SparkSession.builder
        .appName("OlistMonthlySales")
        .master("local[2]")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        orders_path = str(RAW_DIR / "olist_orders_dataset.csv")
        items_path = str(RAW_DIR / "olist_order_items_dataset.csv")

        # 1. Lire les fichiers CSV
        orders = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(orders_path)
        )

        items = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(items_path)
        )

        print("\n--- DATA CHECK ---")
        print("Nombre de commandes:", orders.count())
        print("Nombre de lignes articles:", items.count())

        # 2. Sélectionner les colonnes nécessaires
        orders_selected = orders.select(
            "order_id",
            "order_purchase_timestamp"
        )

        items_selected = items.select(
            "order_id",
            F.col("price").cast("double").alias("price"),
            F.col("freight_value").cast("double").alias("freight_value")
        )

        # 3. Joindre les commandes et les articles
        joined = items_selected.join(
            orders_selected,
            on="order_id",
            how="inner"
        )

        # 4. Calculer les ventes mensuelles
        monthly_sales = (
            joined
            .withColumn(
                "order_month",
                F.date_format(
                    F.to_timestamp("order_purchase_timestamp"),
                    "yyyy-MM"
                )
            )
            .filter(F.col("order_month").isNotNull())
            .groupBy("order_month")
            .agg(
                F.countDistinct("order_id").alias("total_orders"),
                F.count("*").alias("total_items"),
                F.round(F.sum("price"), 2).alias("product_sales"),
                F.round(F.sum("freight_value"), 2).alias("total_freight")
            )
            .orderBy("order_month")
        )

        print("\n--- VENTES MENSUELLES ---")
        monthly_sales.show(50, truncate=False)

        # 5. Enregistrer le résultat avec Python
        # Cela évite l'erreur Hadoop lors de l'écriture CSV sous Windows.
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

        output_file = OUTPUT_DIR / "monthly_sales.csv"
        rows = monthly_sales.collect()

        with open(
            output_file,
            "w",
            newline="",
            encoding="utf-8-sig"
        ) as file:
            writer = csv.DictWriter(
                file,
                fieldnames=monthly_sales.columns
            )
            writer.writeheader()
            writer.writerows([row.asDict() for row in rows])

        print("\nRésultat enregistré dans:", output_file)
        print("Nombre de mois enregistrés:", len(rows))

    finally:
        spark.stop()


if __name__ == "__main__":
    main()


from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip


PROJECT_DIR = Path(__file__).resolve().parents[1]
SILVER_DIR = PROJECT_DIR / "data" / "silver"
GOLD_DIR = PROJECT_DIR / "data" / "gold"


def main():
    builder = (
        SparkSession.builder
        .appName("OlistGoldLayer")
        .master("local[2]")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "2")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        GOLD_DIR.mkdir(parents=True, exist_ok=True)

        orders = spark.read.format("delta").load(str(SILVER_DIR / "orders"))
        items = spark.read.format("delta").load(str(SILVER_DIR / "order_items"))
        products = spark.read.format("delta").load(str(SILVER_DIR / "products"))
        translation = spark.read.format("delta").load(
            str(SILVER_DIR / "product_category_name_translation")
        )

        # Gold 1: ventes mensuelles
        monthly_sales = (
            items.join(
                orders.select(
                    "order_id",
                    "order_purchase_timestamp",
                    "order_status",
                ),
                on="order_id",
                how="inner",
            )
            .filter(F.col("order_purchase_timestamp").isNotNull())
            .withColumn(
                "order_month",
                F.date_format("order_purchase_timestamp", "yyyy-MM"),
            )
            .groupBy("order_month")
            .agg(
                F.countDistinct("order_id").alias("total_orders"),
                F.count("*").alias("total_items"),
                F.round(F.sum("price"), 2).alias("product_sales"),
                F.round(F.sum("freight_value"), 2).alias("total_freight"),
            )
            .orderBy("order_month")
        )

        monthly_sales_path = GOLD_DIR / "monthly_sales"
        (
            monthly_sales.write
            .format("delta")
            .mode("overwrite")
            .save(str(monthly_sales_path))
        )

        print("\n=== GOLD: MONTHLY SALES ===")
        monthly_sales.show(12, truncate=False)
        print(f"Nombre de mois : {monthly_sales.count()}")

        # Gold 2: ventes par catégorie
        category_sales = (
            items.join(
                products.select("product_id", "product_category_name"),
                on="product_id",
                how="left",
            )
            .join(translation, on="product_category_name", how="left")
            .groupBy(
                F.coalesce(
                    F.col("product_category_name_english"),
                    F.col("product_category_name"),
                    F.lit("unknown"),
                ).alias("product_category")
            )
            .agg(
                F.count("*").alias("total_items"),
                F.round(F.sum("price"), 2).alias("product_sales"),
                F.round(F.sum("freight_value"), 2).alias("total_freight"),
            )
            .orderBy(F.desc("product_sales"))
        )

        category_sales_path = GOLD_DIR / "sales_by_category"
        (
            category_sales.write
            .format("delta")
            .mode("overwrite")
            .save(str(category_sales_path))
        )

        print("\n=== GOLD: SALES BY CATEGORY (TOP 10) ===")
        category_sales.show(10, truncate=False)
        print(f"Nombre de catégories : {category_sales.count()}")

        print("\nGOLD : TRAITEMENT TERMINÉ")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

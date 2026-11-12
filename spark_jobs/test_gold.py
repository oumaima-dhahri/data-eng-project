
from pathlib import Path

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


PROJECT_DIR = Path(__file__).resolve().parents[1]
GOLD_DIR = PROJECT_DIR / "data" / "gold"


def main():
    builder = (
        SparkSession.builder
        .appName("OlistGoldQualityChecks")
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
        monthly = spark.read.format("delta").load(
            str(GOLD_DIR / "monthly_sales")
        )
        categories = spark.read.format("delta").load(
            str(GOLD_DIR / "sales_by_category")
        )

        checks = []

        checks.append((
            "monthly_sales non vide",
            monthly.count() > 0,
        ))
        checks.append((
            "sales_by_category non vide",
            categories.count() > 0,
        ))
        checks.append((
            "Aucune ligne mensuelle sans mois",
            monthly.filter("order_month IS NULL").count() == 0,
        ))
        checks.append((
            "Aucune catégorie vide",
            categories.filter(
                "product_category IS NULL OR product_category = ''"
            ).count() == 0,
        ))
        checks.append((
            "Aucune vente mensuelle négative",
            monthly.filter("product_sales < 0").count() == 0,
        ))
        checks.append((
            "Aucune vente par catégorie négative",
            categories.filter("product_sales < 0").count() == 0,
        ))
        checks.append((
            "Un seul enregistrement par mois",
            monthly.count() == monthly.select("order_month").distinct().count(),
        ))
        checks.append((
            "Une seule ligne par catégorie",
            categories.count()
            == categories.select("product_category").distinct().count(),
        ))

        print("\n=== GOLD DATA QUALITY TESTS ===")
        for name, passed in checks:
            print(f"{'PASS' if passed else 'FAIL'} - {name}")

        failed = sum(not passed for _, passed in checks)
        print(f"\nRésultat : {len(checks) - failed}/{len(checks)} tests réussis")

        if failed:
            raise SystemExit("Des tests ont échoué.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

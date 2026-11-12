

from pathlib import Path

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

PROJECT_DIR = Path(__file__).resolve().parents[1]
BRONZE_DIR = PROJECT_DIR / "data" / "bronze"


def main():
    builder = (
        SparkSession.builder
        .appName("OlistInspectBronze")
        .master("local[2]")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "2")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        for table_path in sorted(BRONZE_DIR.iterdir()):
            if not table_path.is_dir():
                continue

            print("\n" + "=" * 60)
            print(f"TABLE : {table_path.name}")

            df = spark.read.format("delta").load(str(table_path))

            print(f"Nombre de lignes : {df.count()}")
            print("Colonnes et types :")
            df.printSchema()

            print("Exemple de 3 lignes :")
            df.show(3, truncate=True)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()




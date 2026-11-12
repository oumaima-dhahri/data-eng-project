
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip

PROJECT_DIR = Path(__file__).resolve().parents[1]
BRONZE_DIR = PROJECT_DIR / "data" / "bronze"
SILVER_DIR = PROJECT_DIR / "data" / "silver"


def main():
    builder = (
        SparkSession.builder
        .appName("OlistSilverLayer")
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
        SILVER_DIR.mkdir(parents=True, exist_ok=True)

        for source in sorted(BRONZE_DIR.iterdir()):
            if not source.is_dir() or not (source / "_delta_log").exists():
                continue

            name = source.name
            print(f"\nTraitement : {name}")

            df = spark.read.format("delta").load(str(source))

            # Nettoyer les espaces dans les colonnes texte.
            for column_name, dtype in df.dtypes:
                if dtype == "string":
                    df = df.withColumn(
                        column_name,
                        F.trim(F.col(column_name))
                    )

            # Supprimer uniquement les lignes entièrement identiques.
            before = df.count()
            df = df.dropDuplicates()
            after = df.count()

            output = SILVER_DIR / name

            (
                df.write
                .format("delta")
                .mode("overwrite")
                .save(str(output))
            )

            print(f"Lignes Bronze : {before}")
            print(f"Lignes Silver : {after}")
            print(f"Doublons exacts supprimés : {before - after}")
            print(f"Enregistré dans : {output}")

        print("\nSILVER : TRAITEMENT TERMINÉ")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

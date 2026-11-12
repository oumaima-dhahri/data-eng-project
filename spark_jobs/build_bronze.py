
from pathlib import Path

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


# 1. Chemins du projet
PROJECT_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_DIR / "data" / "raw"
BRONZE_DIR = PROJECT_DIR / "data" / "bronze"


# 2. Création de la session Spark
def create_spark_session():
    builder = (
        SparkSession.builder
        .appName("OlistBronzeLayer")
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
    spark.sparkContext.setLogLevel("WARN")

    return spark


# 3. Construction de la couche Bronze
def main():
    spark = None

    try:
        if not RAW_DIR.exists():
            raise FileNotFoundError(
                f"Dossier raw introuvable : {RAW_DIR}"
            )

        csv_files = sorted(RAW_DIR.glob("*.csv"))

        if not csv_files:
            raise FileNotFoundError(
                f"Aucun fichier CSV trouvé dans {RAW_DIR}"
            )

        BRONZE_DIR.mkdir(parents=True, exist_ok=True)

        print(f"Fichiers CSV trouvés : {len(csv_files)}")

        spark = create_spark_session()

        for csv_file in csv_files:
            table_name = (
                csv_file.stem
                .replace("olist_", "")
                .replace("_dataset", "")
            )

            output_path = BRONZE_DIR / table_name

            print(f"\nLecture : {csv_file.name}")

            df = (
                spark.read
                .option("header", True)
                .option("inferSchema", True)
                .csv(str(csv_file))
            )

            source_count = df.count()

            (
                df.write
                .format("delta")
                .mode("overwrite")
                .save(str(output_path))
            )

            saved_df = (
                spark.read
                .format("delta")
                .load(str(output_path))
            )

            saved_count = saved_df.count()

            print(f"Table : {table_name}")
            print(f"Lignes source : {source_count}")
            print(f"Lignes Delta : {saved_count}")

            if source_count != saved_count:
                raise ValueError(
                    f"Échec de vérification pour {table_name}"
                )

        print("\nBRONZE LAYER : SUCCÈS")

    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()

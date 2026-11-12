
from pathlib import Path
import shutil

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

PROJECT_DIR = Path(__file__).resolve().parent.parent
GOLD_DIR = PROJECT_DIR / "data" / "gold"
OUTPUT_DIR = PROJECT_DIR / "outputs" / "gold"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


builder = (
    SparkSession.builder
    .appName("OlistExportGoldCSV")
    .master("local[2]")
    .config("spark.driver.memory", "2g")
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


tables = {
    "monthly_sales": GOLD_DIR / "monthly_sales",
    "sales_by_category": GOLD_DIR / "sales_by_category",
}

try:
    for table_name, table_path in tables.items():
        print(f"\nExporting: {table_name}")

        df = (
            spark.read
            .format("delta")
            .load(str(table_path))
        )

        temp_dir = OUTPUT_DIR / f"_temp_{table_name}"

        if temp_dir.exists():
            shutil.rmtree(temp_dir)

        (
            df.coalesce(1)
            .write
            .mode("overwrite")
            .option("header", True)
            .csv(str(temp_dir))
        )

        csv_files = list(temp_dir.glob("part-*.csv"))

        if not csv_files:
            raise FileNotFoundError(
                f"No CSV file generated for {table_name}"
            )

        final_file = OUTPUT_DIR / f"{table_name}.csv"
        shutil.copy2(csv_files[0], final_file)
        shutil.rmtree(temp_dir)

        print(f"Created: {final_file}")
        print(f"Rows: {df.count()}")

    print("\nExport finished successfully!")

finally:
    spark.stop()

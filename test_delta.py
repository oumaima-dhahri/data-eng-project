from pathlib import Path
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

project_dir = Path.cwd()
delta_path = project_dir / "outputs" / "delta_test"

builder = (
    SparkSession.builder
    .appName("OlistDeltaTest")
    .master("local[2]")
    .config("spark.driver.memory", "1g")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

try:
    data = [
        ("2017-01", 789, 120312.87),
        ("2017-02", 1658, 247000.50),
    ]

    df = spark.createDataFrame(
        data,
        ["order_month", "total_orders", "product_sales"]
    )

    df.write.format("delta").mode("overwrite").save(str(delta_path))

    result = spark.read.format("delta").load(str(delta_path))
    result.show(truncate=False)

    print("DELTA TEST SUCCESS")
    print("Chemin:", delta_path)

finally:
    spark.stop()

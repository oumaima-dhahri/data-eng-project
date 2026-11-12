from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, row_number
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, DoubleType
)
from pyspark.sql.window import Window

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable


# =====================================================
# 1. CONFIGURATION
# =====================================================

PROJECT_DIR = Path(__file__).resolve().parents[3]

DELTA_PATH = str(
    PROJECT_DIR / "outputs" / "streaming" / "delta" / "orders"
)

CHECKPOINT_PATH = str(
    PROJECT_DIR / "outputs" / "streaming" / "checkpoints" / "orders"
)


# =====================================================
# 2. SPARK SESSION
# =====================================================

builder = (
    SparkSession.builder
    .appName("Olist-Kafka-Delta-Streaming")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
)

spark = configure_spark_with_delta_pip(
    builder,
    extra_packages=[
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.2"
    ]
).getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("Spark session created successfully!")


# =====================================================
# 3. JSON SCHEMA
# =====================================================

schema = StructType([
    StructField("event_id", IntegerType(), True),
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("order_amount", DoubleType(), True),
    StructField("event_timestamp", StringType(), True)
])


# =====================================================
# 4. KAFKA STREAM
# =====================================================

kafka_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "orders")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

print("Connected to Kafka topic: orders")


# =====================================================
# 5. JSON TRANSFORMATION
# =====================================================

orders = (
    kafka_stream
    .select(
        from_json(
            col("value").cast("string"),
            schema
        ).alias("order")
    )
    .select("order.*")
    .filter(
        col("order_id").isNotNull()
        & col("event_id").isNotNull()
    )
)


# =====================================================
# 6. IDEMPOTENT DELTA WRITING
# =====================================================

def save_batch(batch_df, batch_id):

    print(f"Processing micro-batch: {batch_id}")

    # Keep only one occurrence of each event_id.
    # If duplicated, keep the earliest timestamp.
    window = (
        Window
        .partitionBy("event_id")
        .orderBy(col("event_timestamp").asc_nulls_last())
    )

    clean_batch = (
        batch_df
        .withColumn("_row_number", row_number().over(window))
        .filter(col("_row_number") == 1)
        .drop("_row_number")
    )

    # Create the Delta table if it does not exist.
    if not DeltaTable.isDeltaTable(spark, DELTA_PATH):

        clean_batch.write \
            .format("delta") \
            .mode("append") \
            .save(DELTA_PATH)

    else:

        delta_table = DeltaTable.forPath(spark, DELTA_PATH)

        # Insert only events whose event_id is not already present.
        (
            delta_table.alias("target")
            .merge(
                clean_batch.alias("source"),
                "target.event_id = source.event_id"
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

    print(f"Micro-batch {batch_id} saved successfully.")


# =====================================================
# 7. STREAMING QUERY
# =====================================================

Path(DELTA_PATH).parent.mkdir(
    parents=True,
    exist_ok=True
)

Path(CHECKPOINT_PATH).mkdir(
    parents=True,
    exist_ok=True
)

query = (
    orders.writeStream
    .foreachBatch(save_batch)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="10 seconds")
    .start()
)

print("============================================")
print("OLIST KAFKA TO DELTA STREAMING STARTED")
print(f"Kafka topic: orders")
print(f"Delta destination: {DELTA_PATH}")
print(f"Checkpoint: {CHECKPOINT_PATH}")
print("Deduplication key: event_id")
print("Writing strategy: Delta MERGE")
print("============================================")


# =====================================================
# 8. KEEP STREAMING ACTIVE
# =====================================================

try:
    query.awaitTermination()

except KeyboardInterrupt:
    print("Stopping streaming application...")
    query.stop()

finally:
    spark.stop()
    print("Spark session stopped.")

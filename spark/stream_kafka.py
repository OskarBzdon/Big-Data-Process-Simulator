#!/usr/bin/env python3
"""
Simple Spark Streaming job: Kafka → Delta Lake in MinIO

This is a simplified implementation for easy verification.
Environment variables:
- KAFKA_BOOTSTRAP_SERVERS (default: kafka:29092)
- KAFKA_TOPIC (default: business_postgres.public.business_ncr_ride_bookings)
- DELTA_TABLE_PATH (default: s3a://business-data/delta/ride-bookings)
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    regexp_replace,
    from_unixtime,
    when,
    current_timestamp,
)
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType


def main():
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    topic = os.getenv(
        "KAFKA_TOPIC", "business_postgres.public.business_ncr_ride_bookings"
    )
    delta_table_path = os.getenv(
        "DELTA_TABLE_PATH", "s3a://business-data/delta/ride-bookings"
    )

    spark = (
        SparkSession.builder.appName("KafkaJSONStreamingJob")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"📊 Writing Delta Lake tables to: {delta_table_path}")

    # Read Kafka stream
    df_kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")  # Don't fail if offsets change (topic recreated, data aged out)
        .load()
    )

    df_json = df_kafka.selectExpr("CAST(value AS STRING) AS json_str")

    # Minimal schema - we can add more fields later as needed
    json_schema = StructType(
        [
            StructField("id", LongType(), True),
            StructField("created_at", LongType(), True),
            StructField("updated_at", LongType(), True),
            StructField("__op", StringType(), True),
            StructField("ncr_ride_bookings.csv/booking+value", StringType(), True),
            StructField("ncr_ride_bookings.csv/ride+distance", StringType(), True),
        ]
    )

    parsed = from_json(col("json_str"), json_schema).alias("data")
    df = df_json.select(parsed)

    # Access special-name fields using backticks
    booking_val_raw = col("data.`ncr_ride_bookings.csv/booking+value`")
    ride_distance_raw = col("data.`ncr_ride_bookings.csv/ride+distance`")

    # Clean values like b'1138.0' -> 1138.0 and cast
    def clean_bytes_like(c):
        c1 = regexp_replace(c, r"^b'", "")
        c2 = regexp_replace(c1, r"'$", "")
        c3 = regexp_replace(c2, r'^"|"$', "")
        return c3

    booking_value_clean = clean_bytes_like(booking_val_raw).cast(DoubleType())
    ride_distance_clean = clean_bytes_like(ride_distance_raw).cast(DoubleType())

    # Convert microseconds to seconds and to timestamp (if present)
    created_at_ts = when(
        col("data.created_at").isNotNull(),
        from_unixtime((col("data.created_at") / 1_000_000).cast("double")),
    )
    updated_at_ts = when(
        col("data.updated_at").isNotNull(),
        from_unixtime((col("data.updated_at") / 1_000_000).cast("double")),
    )

    result = (
        df.select(
            col("data.id").alias("id"),
            col("data.__op").alias("op"),
            created_at_ts.alias("created_at_ts"),
            updated_at_ts.alias("updated_at_ts"),
            booking_value_clean.alias("booking_value"),
            ride_distance_clean.alias("ride_distance"),
            current_timestamp().alias("processed_at"),
        )
    )

    # Write to Delta Lake format in MinIO
    query = (
        result.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{delta_table_path}/_checkpoints")
        .option("path", delta_table_path)
        .trigger(processingTime="10 seconds")
        .start()
    )
    
    print(f"✅ Started streaming query. Writing to Delta Lake: {delta_table_path}")
    print("📝 Query status updates will appear in logs...")

    query.awaitTermination()


if __name__ == "__main__":
    main()



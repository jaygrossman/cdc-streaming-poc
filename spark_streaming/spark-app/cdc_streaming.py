#!/usr/bin/env python3
"""
Spark Structured Streaming job: CDC Policy Pipeline

Reads Debezium CDC events from Kafka, flattens nested JSONB into 5 entity
types, and writes to append-only staging tables on PostgreSQL. The merge
into output tables is handled by merge_cdc_batch() via PG LISTEN/NOTIFY.
"""

import logging
import sys

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    coalesce,
    from_json,
    get_json_object,
    explode_outer,
    to_timestamp,
    lit,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    IntegerType,
    DoubleType,
    BooleanType,
    ArrayType,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("cdc-streaming")

# =============================================================================
# JDBC config
# =============================================================================
JDBC_URL = "jdbc:postgresql://postgres-cdc:5432/cdc_db"
JDBC_PROPS = {
    "user": "cdc_user",
    "password": "cdc_pass",
    "driver": "org.postgresql.Driver",
}

# =============================================================================
# Debezium envelope schema
# =============================================================================
record_schema = StructType([
    StructField("id", LongType()),
    StructField("data", StringType()),
    StructField("created_at", StringType()),
    StructField("updated_at", StringType()),
])

envelope_schema = StructType([
    StructField("before", record_schema),
    StructField("after", record_schema),
    StructField("op", StringType()),
    StructField("ts_ms", LongType()),
])

# =============================================================================
# JSON array schemas for explode
# =============================================================================
coverage_schema = ArrayType(StructType([
    StructField("type", StringType()),
    StructField("limit", DoubleType()),
    StructField("deductible", DoubleType()),
    StructField("premium", DoubleType()),
]))

driver_schema = ArrayType(StructType([
    StructField("name", StringType()),
    StructField("license_number", StringType()),
    StructField("is_primary", BooleanType()),
]))

vehicle_schema = ArrayType(StructType([
    StructField("vin", StringType()),
    StructField("year", IntegerType()),
    StructField("make", StringType()),
    StructField("model", StringType()),
    StructField("drivers", driver_schema),
]))

claim_schema = ArrayType(StructType([
    StructField("claim_id", StringType()),
    StructField("date", StringType()),
    StructField("amount", DoubleType()),
    StructField("status", StringType()),
    StructField("description", StringType()),
]))


def write_to_staging(batch_df: DataFrame, batch_id: int) -> None:
    """Process a micro-batch: flatten CDC events and write to 5 staging tables."""
    if batch_df.isEmpty():
        return

    # Resolve before/after and compute common columns
    resolved = (
        batch_df
        .withColumn("policy_id", coalesce(col("after.id"), col("before.id")))
        .withColumn("data", coalesce(col("after.data"), col("before.data")))
        .withColumn("op", col("op"))
        .withColumn(
            "event_time",
            to_timestamp(col("ts_ms") / 1000),
        )
        .filter(col("op").isin("c", "r", "u", "d"))
        .select("policy_id", "data", "op", "event_time")
    )

    # Cache since we derive 5 DataFrames from it
    resolved.cache()
    count = resolved.count()
    if count == 0:
        resolved.unpersist()
        return

    log.info("Batch %d: processing %d CDC event(s)", batch_id, count)

    # --- stg_policy ---
    policy_df = resolved.select(
        col("policy_id"),
        get_json_object("data", "$.policy_number").alias("policy_number"),
        get_json_object("data", "$.status").alias("status"),
        get_json_object("data", "$.effective_date").cast("date").alias("effective_date"),
        get_json_object("data", "$.expiration_date").cast("date").alias("expiration_date"),
        get_json_object("data", "$.policyholder.first_name").alias("holder_first_name"),
        get_json_object("data", "$.policyholder.last_name").alias("holder_last_name"),
        get_json_object("data", "$.policyholder.date_of_birth").cast("date").alias("holder_dob"),
        get_json_object("data", "$.policyholder.contact.email").alias("holder_email"),
        get_json_object("data", "$.policyholder.contact.phone").alias("holder_phone"),
        get_json_object("data", "$.policyholder.contact.address.street").alias("holder_street"),
        get_json_object("data", "$.policyholder.contact.address.city").alias("holder_city"),
        get_json_object("data", "$.policyholder.contact.address.state").alias("holder_state"),
        get_json_object("data", "$.policyholder.contact.address.zip").alias("holder_zip"),
        col("event_time").alias("source_event_time"),
        col("op"),
        col("event_time"),
    )
    policy_df.write.jdbc(url=JDBC_URL, table="stg_policy", mode="append", properties=JDBC_PROPS)

    # --- stg_coverage ---
    coverage_df = (
        resolved
        .withColumn(
            "coverages",
            from_json(get_json_object("data", "$.coverages"), coverage_schema),
        )
        .select("policy_id", explode_outer("coverages").alias("c"), "op", "event_time")
        .filter(col("c").isNotNull())
        .select(
            col("policy_id"),
            col("c.type").alias("coverage_type"),
            col("c.limit").alias("coverage_limit"),
            col("c.deductible"),
            col("c.premium"),
            col("op"),
            col("event_time"),
        )
    )
    coverage_df.write.jdbc(url=JDBC_URL, table="stg_coverage", mode="append", properties=JDBC_PROPS)

    # --- stg_vehicle ---
    vehicle_df = (
        resolved
        .withColumn(
            "vehicles",
            from_json(get_json_object("data", "$.vehicles"), vehicle_schema),
        )
        .select("policy_id", explode_outer("vehicles").alias("v"), "op", "event_time")
        .filter(col("v").isNotNull())
        .select(
            col("policy_id"),
            col("v.vin").alias("vin"),
            col("v.year").alias("year_made"),
            col("v.make"),
            col("v.model"),
            col("op"),
            col("event_time"),
        )
    )
    vehicle_df.write.jdbc(url=JDBC_URL, table="stg_vehicle", mode="append", properties=JDBC_PROPS)

    # --- stg_driver (explode vehicles, then explode nested drivers) ---
    driver_df = (
        resolved
        .withColumn(
            "vehicles",
            from_json(get_json_object("data", "$.vehicles"), vehicle_schema),
        )
        .select("policy_id", explode_outer("vehicles").alias("v"), "op", "event_time")
        .filter(col("v").isNotNull())
        .select(
            "policy_id",
            col("v.vin").alias("vehicle_vin"),
            explode_outer("v.drivers").alias("d"),
            "op",
            "event_time",
        )
        .filter(col("d").isNotNull())
        .select(
            col("policy_id"),
            col("vehicle_vin"),
            col("d.name").alias("driver_name"),
            col("d.license_number").alias("license_number"),
            col("d.is_primary").alias("is_primary"),
            col("op"),
            col("event_time"),
        )
    )
    driver_df.write.jdbc(url=JDBC_URL, table="stg_driver", mode="append", properties=JDBC_PROPS)

    # --- stg_claim ---
    claim_df = (
        resolved
        .withColumn(
            "claims",
            from_json(get_json_object("data", "$.claims_history"), claim_schema),
        )
        .select("policy_id", explode_outer("claims").alias("c"), "op", "event_time")
        .filter(col("c").isNotNull())
        .select(
            col("policy_id"),
            col("c.claim_id").alias("claim_id"),
            col("c.date").cast("date").alias("claim_date"),
            col("c.amount"),
            col("c.status"),
            col("c.description"),
            col("op"),
            col("event_time"),
        )
    )
    claim_df.write.jdbc(url=JDBC_URL, table="stg_claim", mode="append", properties=JDBC_PROPS)

    resolved.unpersist()
    log.info("Batch %d: wrote to all staging tables", batch_id)


def main():
    spark = (
        SparkSession.builder
        .appName("cdc-policy-streaming")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    log.info("Starting CDC streaming pipeline...")

    # Read from Kafka
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:29092")
        .option("subscribe", "cdc.public.policy")
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Parse the Debezium JSON envelope
    parsed = (
        raw
        .select(
            from_json(col("value").cast("string"), envelope_schema).alias("envelope")
        )
        .select("envelope.*")
        .filter(col("op").isNotNull())
    )

    # Write stream using foreachBatch
    query = (
        parsed.writeStream
        .foreachBatch(write_to_staging)
        .trigger(processingTime="5 seconds")
        .option("checkpointLocation", "/tmp/spark-checkpoints/cdc-policy")
        .start()
    )

    log.info("Streaming query started. Awaiting termination...")
    query.awaitTermination()


if __name__ == "__main__":
    main()

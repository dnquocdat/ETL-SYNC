"""
ETL-SYNC: Production-Grade Spark Structured Streaming CDC Pipeline
===================================================================
Consumes Debezium CDC events from Kafka (multi-table), validates data,
performs bulk upsert/delete on MongoDB, tracks metrics, and routes failed
records to a Dead Letter Queue.

Architecture:
  Kafka (CDC topics) → Spark Structured Streaming → [validate] → MongoDB
                                                  → [failed]  → DLQ (Kafka)
"""

import json
import time
import logging
import sys

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col, lit
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    LongType, DecimalType, TimestampType, MapType
)

# ── Local modules ──────────────────────────────────────────────
sys.path.insert(0, "/opt/spark-app")
from config import Config
from metrics import PipelineMetrics, BatchTimer
from validators import validate_batch

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("etl_job")

# ── CDC Schemas per table ──────────────────────────────────────
# Debezium wraps row data in "before" and "after" structs.
# We define the inner struct per table.

FIELD_SCHEMAS = {
    "products": StructType([
        StructField("id",          IntegerType()),
        StructField("name",        StringType()),
        StructField("category",    StringType()),
        StructField("price",       StringType()),       # Debezium sends DECIMAL as string
        StructField("description", StringType()),
        StructField("status",      StringType()),
        StructField("created_at",  LongType()),         # Debezium epoch millis
        StructField("updated_at",  LongType()),
    ]),
    "inventory": StructType([
        StructField("id",          IntegerType()),
        StructField("product_id",  IntegerType()),
        StructField("warehouse",   StringType()),
        StructField("quantity",    IntegerType()),
        StructField("reserved",    IntegerType()),
        StructField("updated_at",  LongType()),
    ]),
    "orders": StructType([
        StructField("id",               IntegerType()),
        StructField("customer_name",     StringType()),
        StructField("customer_email",    StringType()),
        StructField("status",            StringType()),
        StructField("total_amount",      StringType()),  # DECIMAL as string
        StructField("shipping_address",  StringType()),
        StructField("created_at",        LongType()),
        StructField("updated_at",        LongType()),
    ]),
    "order_items": StructType([
        StructField("id",          IntegerType()),
        StructField("order_id",    IntegerType()),
        StructField("product_id",  IntegerType()),
        StructField("quantity",    IntegerType()),
        StructField("unit_price",  StringType()),    # DECIMAL as string
        StructField("created_at",  LongType()),
    ]),
}


def build_cdc_schema(inner_schema: StructType) -> StructType:
    """Build the full Debezium CDC envelope schema for a given table schema."""
    return StructType([
        StructField("before", inner_schema),
        StructField("after",  inner_schema),
        StructField("op",     StringType()),      # c=create, u=update, d=delete, r=read
        StructField("ts_ms",  LongType()),
        StructField("source", MapType(StringType(), StringType())),
    ])


# Generic envelope schema (used for initial parsing to extract "op" and topic)
GENERIC_CDC_SCHEMA = StructType([
    StructField("before", MapType(StringType(), StringType())),
    StructField("after",  MapType(StringType(), StringType())),
    StructField("op",     StringType()),
    StructField("ts_ms",  LongType()),
])

# ── MongoDB connection pool (singleton) ────────────────────────
_mongo_client = None


def get_mongo_client():
    """Get or create a singleton MongoClient with connection pooling."""
    global _mongo_client
    if _mongo_client is None:
        from pymongo import MongoClient
        _mongo_client = MongoClient(
            Config.MONGO_URI,
            maxPoolSize=20,
            minPoolSize=2,
            maxIdleTimeMS=30000,
            serverSelectionTimeoutMS=5000,
            retryWrites=True,
        )
        logger.info("MongoDB client created: %s", Config.MONGO_URI)
    return _mongo_client


def get_collection(collection_name: str):
    """Get a MongoDB collection handle."""
    client = get_mongo_client()
    return client[Config.MONGO_DATABASE][collection_name]


# ── DLQ Producer ───────────────────────────────────────────────
_dlq_producer = None


def get_dlq_producer():
    """Get or create a Kafka producer for the Dead Letter Queue."""
    global _dlq_producer
    if _dlq_producer is None and Config.DLQ_ENABLED:
        try:
            from kafka import KafkaProducer
            _dlq_producer = KafkaProducer(
                bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                retries=3,
            )
            logger.info("DLQ Kafka producer created for topic: %s", Config.DLQ_TOPIC)
        except Exception as e:
            logger.error("Failed to create DLQ producer: %s", e)
    return _dlq_producer


def send_to_dlq(failed_records: list):
    """Send failed records to the Dead Letter Queue Kafka topic."""
    producer = get_dlq_producer()
    if producer is None:
        logger.warning("DLQ disabled or unavailable, dropping %d records", len(failed_records))
        return

    for record in failed_records:
        try:
            producer.send(Config.DLQ_TOPIC, value={
                "timestamp": time.time(),
                "table": record.get("table", "unknown"),
                "errors": record.get("errors", []),
                "record": record.get("record", {}),
            })
        except Exception as e:
            logger.error("Failed to send to DLQ: %s", e)

    producer.flush()


# ── Bulk Write Operations ──────────────────────────────────────
def bulk_upsert(collection_name: str, records: list, metrics: PipelineMetrics):
    """
    Perform bulk upsert (ReplaceOne with upsert=True) on MongoDB.
    Much faster than individual writes — reduces round trips.
    """
    if not records:
        return

    from pymongo import ReplaceOne

    coll = get_collection(collection_name)
    operations = []

    for record in records:
        doc = dict(record)
        doc_id = doc.pop("id", None)
        if doc_id is None:
            continue
        doc["_id"] = doc_id

        # Convert Debezium decimal strings to float
        for key, val in doc.items():
            if isinstance(val, str):
                try:
                    if "." in val and key in ("price", "total_amount", "unit_price"):
                        doc[key] = float(val)
                except ValueError:
                    pass

        operations.append(
            ReplaceOne({"_id": doc_id}, doc, upsert=True)
        )

    if operations:
        try:
            result = coll.bulk_write(operations, ordered=False)
            metrics.track_processed(
                collection_name, "upsert",
                result.upserted_count + result.modified_count
            )
            logger.info(
                "  [%s] bulk_upsert: %d upserted, %d modified",
                collection_name, result.upserted_count, result.modified_count
            )
        except Exception as e:
            metrics.track_failed(collection_name, "bulk_write_error")
            logger.error("  [%s] bulk_upsert failed: %s", collection_name, e)
            raise


def bulk_delete(collection_name: str, ids: list, metrics: PipelineMetrics):
    """Batch delete documents from MongoDB by _id."""
    if not ids:
        return

    coll = get_collection(collection_name)
    try:
        result = coll.delete_many({"_id": {"$in": ids}})
        metrics.track_processed(collection_name, "delete", result.deleted_count)
        logger.info("  [%s] bulk_delete: %d deleted", collection_name, result.deleted_count)
    except Exception as e:
        metrics.track_failed(collection_name, "delete_error")
        logger.error("  [%s] bulk_delete failed: %s", collection_name, e)
        raise


# ── Main Batch Processor ──────────────────────────────────────
def process_batch(batch_df: DataFrame, batch_id: int, metrics: PipelineMetrics):
    """
    Process a single micro-batch of CDC events from Kafka.

    Steps:
      1. Group records by table (topic)
      2. Parse CDC payload per table
      3. Validate records
      4. Bulk upsert valid records
      5. Bulk delete for 'd' operations
      6. Route invalid records to DLQ
      7. Track metrics
    """
    if batch_df.rdd.isEmpty():
        return

    with BatchTimer(metrics):
        logger.info("━" * 60)
        logger.info("BATCH #%d — processing", batch_id)

        # Collect all records with their topic
        rows = batch_df.select(
            col("topic"),
            col("value").cast("string").alias("json_value"),
            col("timestamp").alias("kafka_ts"),
        ).collect()

        # Group by topic
        topic_groups = {}
        for row in rows:
            topic = row["topic"]
            if topic not in topic_groups:
                topic_groups[topic] = []
            topic_groups[topic].append(row)

        for topic, topic_rows in topic_groups.items():
            collection_name = Config.get_collection_for_topic(topic)
            logger.info("  Processing topic=%s → collection=%s (%d records)",
                        topic, collection_name, len(topic_rows))

            metrics.track_batch_size(collection_name, len(topic_rows))

            upsert_records = []
            delete_ids = []
            dlq_records = []

            for row in topic_rows:
                try:
                    payload = json.loads(row["json_value"])
                except (json.JSONDecodeError, TypeError) as e:
                    dlq_records.append({
                        "table": collection_name,
                        "errors": [f"JSON parse error: {e}"],
                        "record": {"raw": row["json_value"][:500]},
                    })
                    metrics.track_failed(collection_name, "json_parse_error")
                    continue

                op = payload.get("op")
                ts_ms = payload.get("ts_ms", 0)

                # Track sync lag
                if ts_ms and row["kafka_ts"]:
                    try:
                        lag = (time.time() * 1000 - ts_ms) / 1000.0
                        metrics.track_sync_lag(lag)
                    except Exception:
                        pass

                if op in ("c", "u", "r"):
                    record = payload.get("after")
                    if record:
                        upsert_records.append(record)
                    else:
                        dlq_records.append({
                            "table": collection_name,
                            "errors": ["op=%s but 'after' is null" % op],
                            "record": payload,
                        })
                        metrics.track_failed(collection_name, "null_after")

                elif op == "d":
                    before = payload.get("before")
                    if before and "id" in before:
                        delete_ids.append(before["id"])
                    else:
                        dlq_records.append({
                            "table": collection_name,
                            "errors": ["op=d but 'before.id' is null"],
                            "record": payload,
                        })
                        metrics.track_failed(collection_name, "null_before_id")

                else:
                    logger.warning("  Unknown op '%s', skipping", op)

            # ── Validate upsert records ────────────────────────
            valid_records, invalid_records = validate_batch(collection_name, upsert_records)
            dlq_records.extend(invalid_records)

            # ── Execute writes ─────────────────────────────────
            try:
                bulk_upsert(collection_name, valid_records, metrics)
            except Exception as e:
                logger.error("  Bulk upsert failed, routing to DLQ: %s", e)
                for rec in valid_records:
                    dlq_records.append({
                        "table": collection_name,
                        "errors": [f"bulk_upsert exception: {e}"],
                        "record": rec,
                    })

            try:
                bulk_delete(collection_name, delete_ids, metrics)
            except Exception as e:
                logger.error("  Bulk delete failed: %s", e)

            # ── Send failures to DLQ ───────────────────────────
            if dlq_records:
                send_to_dlq(dlq_records)
                logger.warning("  Sent %d records to DLQ", len(dlq_records))

        # ── Push metrics ───────────────────────────────────────
        if batch_id % Config.METRICS_PUSH_INTERVAL == 0:
            metrics.push()

        logger.info("BATCH #%d — complete", batch_id)
        logger.info("━" * 60)


# ══════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════
def main():
    logger.info("=" * 60)
    logger.info("ETL-SYNC Spark Streaming Job Starting")
    logger.info("  Kafka:      %s", Config.KAFKA_BOOTSTRAP_SERVERS)
    logger.info("  Topics:     %s", Config.KAFKA_TOPIC_PATTERN)
    logger.info("  MongoDB:    %s/%s", Config.MONGO_URI, Config.MONGO_DATABASE)
    logger.info("  DLQ:        %s (enabled=%s)", Config.DLQ_TOPIC, Config.DLQ_ENABLED)
    logger.info("  Metrics:    %s (enabled=%s)", Config.PUSHGATEWAY_URL, Config.METRICS_ENABLED)
    logger.info("=" * 60)

    # ── SparkSession ───────────────────────────────────────────
    spark = SparkSession.builder \
        .appName("ETL-Sync-ECommerce") \
        .config("spark.mongodb.output.uri",
                f"{Config.MONGO_URI}/{Config.MONGO_DATABASE}") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel(Config.LOG_LEVEL)

    # ── Initialize metrics ─────────────────────────────────────
    metrics = PipelineMetrics(
        pushgateway_url=Config.PUSHGATEWAY_URL,
        job_name=Config.METRICS_JOB_NAME,
        enabled=Config.METRICS_ENABLED,
    )

    # ── Read from Kafka (subscribe to all CDC topics via pattern) ──
    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", Config.KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribePattern", Config.KAFKA_TOPIC_PATTERN) \
        .option("startingOffsets", Config.KAFKA_STARTING_OFFSETS) \
        .option("maxOffsetsPerTrigger", Config.KAFKA_MAX_OFFSETS_PER_TRIGGER) \
        .option("failOnDataLoss", "false") \
        .load()

    # ── Process with foreachBatch ──────────────────────────────
    query = raw_df.writeStream \
        .foreachBatch(lambda df, bid: process_batch(df, bid, metrics)) \
        .outputMode("update") \
        .option("checkpointLocation", Config.CHECKPOINT_LOCATION) \
        .trigger(processingTime=Config.PROCESSING_TIME) \
        .start()

    logger.info("Streaming query started, awaiting termination...")
    query.awaitTermination()


if __name__ == "__main__":
    main()

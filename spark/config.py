"""
Centralized configuration for the ETL-SYNC Spark job.
All values are loaded from environment variables with sensible defaults.
"""

import os


class Config:
    # ── Kafka ──────────────────────────────────────────────────
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka1:29092,kafka2:29092,kafka3:29092")
    KAFKA_TOPIC_PATTERN     = os.getenv("KAFKA_TOPIC_PATTERN", "mysql_server\\.etl_db\\..*")
    KAFKA_STARTING_OFFSETS  = os.getenv("KAFKA_STARTING_OFFSETS", "earliest")
    KAFKA_MAX_OFFSETS_PER_TRIGGER = os.getenv("KAFKA_MAX_OFFSETS_PER_TRIGGER", "5000")

    # ── MongoDB ────────────────────────────────────────────────
    MONGO_URI      = os.getenv("MONGO_URI", "mongodb://mongo:27017")
    MONGO_DATABASE = os.getenv("MONGO_DATABASE", "etl_db")

    # ── Spark ──────────────────────────────────────────────────
    CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION", "/opt/spark-app/checkpoint")
    LOG_LEVEL           = os.getenv("SPARK_LOG_LEVEL", "WARN")
    PROCESSING_TIME     = os.getenv("PROCESSING_TIME", "5 seconds")

    # ── Dead Letter Queue ──────────────────────────────────────
    DLQ_ENABLED = os.getenv("DLQ_ENABLED", "true").lower() == "true"
    DLQ_TOPIC   = os.getenv("DLQ_TOPIC", "dlq.etl_sync")

    # ── Metrics ────────────────────────────────────────────────
    METRICS_ENABLED       = os.getenv("METRICS_ENABLED", "true").lower() == "true"
    PUSHGATEWAY_URL       = os.getenv("PUSHGATEWAY_URL", "http://pushgateway:9091")
    METRICS_JOB_NAME      = os.getenv("METRICS_JOB_NAME", "etl_sync_spark")
    METRICS_PUSH_INTERVAL = int(os.getenv("METRICS_PUSH_INTERVAL", "1"))  # push every N batches

    # ── Table mapping ──────────────────────────────────────────
    # topic suffix → MongoDB collection name
    TABLE_COLLECTION_MAP = {
        "products":    "products",
        "inventory":   "inventory",
        "orders":      "orders",
        "order_items": "order_items",
    }

    @classmethod
    def get_collection_for_topic(cls, topic: str) -> str:
        """Extract collection name from Kafka topic like 'mysql_server.etl_db.products'."""
        table_name = topic.rsplit(".", 1)[-1] if "." in topic else topic
        return cls.TABLE_COLLECTION_MAP.get(table_name, table_name)

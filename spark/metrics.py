"""
Prometheus metrics for ETL-SYNC pipeline.
Tracks records processed, errors, batch duration, and sync lag.
Uses Prometheus Pushgateway to export metrics from the Spark driver.
"""

import time
import logging

logger = logging.getLogger("etl_metrics")


class PipelineMetrics:
    """Collect and push pipeline metrics to Prometheus Pushgateway."""

    def __init__(self, pushgateway_url: str, job_name: str, enabled: bool = True):
        self.enabled = enabled
        self.pushgateway_url = pushgateway_url
        self.job_name = job_name

        # In-memory counters (pushed periodically)
        self._records_processed = {}   # (table, op) → count
        self._records_failed    = {}   # (table, error_type) → count
        self._batch_durations   = []   # list of seconds
        self._sync_lags         = []   # list of seconds
        self._batch_count       = 0

        if self.enabled:
            try:
                from prometheus_client import CollectorRegistry, Counter, Histogram, Gauge, push_to_gateway
                self.registry = CollectorRegistry()
                self.push_fn = push_to_gateway

                self.records_processed = Counter(
                    "etl_records_processed_total",
                    "Total records processed",
                    ["table", "operation"],
                    registry=self.registry,
                )
                self.records_failed = Counter(
                    "etl_records_failed_total",
                    "Total records that failed processing",
                    ["table", "error_type"],
                    registry=self.registry,
                )
                self.batch_duration = Histogram(
                    "etl_batch_processing_seconds",
                    "Time spent processing each micro-batch",
                    buckets=[0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0],
                    registry=self.registry,
                )
                self.sync_lag = Gauge(
                    "etl_sync_lag_seconds",
                    "Delay between MySQL event and MongoDB write",
                    registry=self.registry,
                )
                self.batch_size_gauge = Gauge(
                    "etl_batch_size",
                    "Number of records in the latest micro-batch",
                    ["table"],
                    registry=self.registry,
                )
                logger.info("Prometheus metrics initialized (pushgateway=%s)", pushgateway_url)
            except ImportError:
                logger.warning("prometheus_client not installed, metrics disabled")
                self.enabled = False

    def track_processed(self, table: str, operation: str, count: int = 1):
        if not self.enabled:
            return
        self.records_processed.labels(table=table, operation=operation).inc(count)

    def track_failed(self, table: str, error_type: str, count: int = 1):
        if not self.enabled:
            return
        self.records_failed.labels(table=table, error_type=error_type).inc(count)

    def track_batch_duration(self, duration_seconds: float):
        if not self.enabled:
            return
        self.batch_duration.observe(duration_seconds)

    def track_sync_lag(self, lag_seconds: float):
        if not self.enabled:
            return
        self.sync_lag.set(lag_seconds)

    def track_batch_size(self, table: str, size: int):
        if not self.enabled:
            return
        self.batch_size_gauge.labels(table=table).set(size)

    def push(self):
        """Push all collected metrics to Pushgateway."""
        if not self.enabled:
            return
        try:
            self.push_fn(self.pushgateway_url, job=self.job_name, registry=self.registry)
        except Exception as e:
            logger.warning("Failed to push metrics: %s", e)


class BatchTimer:
    """Context manager to time a micro-batch and report to metrics."""

    def __init__(self, metrics: PipelineMetrics):
        self.metrics = metrics
        self.start = None

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        duration = time.time() - self.start
        self.metrics.track_batch_duration(duration)

# MySQL to MongoDB Real-Time Sync ETL Project

A real-time ETL pipeline that captures MySQL change events and synchronizes them to MongoDB using CDC and stream processing.

## Tech Stack
- MySQL
- MongoDB
- Apache Kafka
- Kafka Connect + Debezium (CDC)
- Apache Spark Structured Streaming
- Python (PySpark, PyMongo)
- Docker / Docker Compose
- Prometheus + Grafana

## CV-Ready Highlights
- Designed and implemented an end-to-end real-time ETL pipeline to synchronize operational data from MySQL to MongoDB.
- Implemented CDC with Debezium on Kafka Connect to capture INSERT, UPDATE, and DELETE events from MySQL binlog.
- Built a Spark Structured Streaming job to consume Kafka events, transform records, and apply corresponding upsert/delete logic in MongoDB.
- Added data validation rules and Dead Letter Queue handling to isolate invalid events and improve pipeline reliability.
- Maintained cross-database consistency by applying CRUD changes continuously and preserving near real-time alignment between source and target datasets.
- Added observability with Prometheus and Grafana to track processing metrics and support monitoring during test-load simulation.

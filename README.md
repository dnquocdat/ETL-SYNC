# ETL Sync Project: Realtime Data Synchronization between MySQL and MongoDB

This project demonstrates a real-time ETL (Extract, Transform, Load) pipeline to keep data in sync between a MySQL database and a MongoDB collection. It uses the following technologies:

* **Debezium MySQL Connector** for Change Data Capture (CDC)
* **Apache Kafka** as the messaging backbone
* **Apache Spark Structured Streaming** for processing and applying changes
* **MongoDB** as the target data store
* **Docker Compose** to orchestrate all services

---

## Architecture Overview

```
MySQL (binlog) → Debezium Connector → Kafka Topics → Spark Structured Streaming → MongoDB
```

1. **MySQL**: Hosts the source database with `binlog` enabled (binlog format = ROW).
2. **Debezium**: Captures row-level changes (INSERT, UPDATE, DELETE) from MySQL and publishes CDC events to Kafka topics.
3. **Kafka**: A distributed log that buffers and transports CDC events.
4. **Spark Structured Streaming**: Consumes CDC events from Kafka, parses them, and writes corresponding changes into MongoDB.
5. **MongoDB**: The target NoSQL datastore where the `users` collection mirrors the MySQL `users` table.

---

## Prerequisites

* **Docker** and **Docker Compose** installed
* Basic familiarity with MySQL and MongoDB commands
* Python 3 installed (for connector registration script)

---

## Getting Started

Follow these steps to launch and test the pipeline locally.

### 1. Clone the repository

```bash
git clone https://github.com/dnquocdat/ETL-SYNC.git
cd etl-sync-project
```

### 2. Launch core services

Start ZooKeeper, Kafka, MySQL, and MongoDB:

```bash
docker-compose up -d zookeeper kafka mysql mongo
```

Wait \~30 seconds for all containers to initialize and for MySQL to execute the initialization scripts.

### 3. Start Debezium Connect

```bash
docker-compose up -d debezium
```

Debezium Connect will be available at `http://localhost:8083`. Wait until its health check passes.

### 4. Register the MySQL Connector

Run the Python script to create the Debezium MySQL connector:

```bash
python register_connector.py
```

You should see HTTP `201 Created` and the connector name `mysql-connector` in the response.

Verify its status:

```bash
curl http://localhost:8083/connectors/mysql-connector/status
```

Ensure that the connector state is **RUNNING** and the task is not in a failed state.

### 5. Launch Spark Streaming Job

```bash
docker-compose up -d spark
```

This starts the Spark Structured Streaming job (`etl_job.py`), which listens to CDC events on the Kafka topic and applies them to MongoDB.

### 6. Verify Data Synchronization

#### a) Test in MySQL

Connect to the MySQL container and manipulate data:

```bash
docker exec -it etl-sync-project-mysql-1 mysql -uroot -prootpwd etl_db
```

Run SQL statements:

```sql
USE etl_db;
SELECT * FROM users;

-- Insert a new row
INSERT INTO users (name, email) VALUES ('Le Van C', 'c@example.com');

-- Update an existing row
UPDATE users SET email = 'a.updated@example.com' WHERE id = 1;

-- Delete a row
DELETE FROM users WHERE id = 2;
```

#### b) Inspect Kafka Topic (optional)

List available topics:

```bash
docker exec -it etl-sync-project-kafka-1 bash -c "kafka-topics --bootstrap-server kafka:9092 --list"
```

Consume CDC messages from the correct topic (e.g., `mysql_server.etl_db.users` or `mysql_server.dbserver1.etl_db.users`):

```bash
docker exec -it etl-sync-project-kafka-1 bash \
  -c "kafka-console-consumer --bootstrap-server kafka:9092 \
                             --topic mysql_server.etl_db.users \
                             --from-beginning \
                             --max-messages 5"
```

You should see JSON events containing `"op": "c"`, `"u"`, or `"d"`.

#### c) Check MongoDB

Connect to MongoDB and verify the `users` collection:

```bash
docker exec -it etl-sync-project-mongo-1 mongosh
```

Run:

```javascript
use etl_db;
db.users.find().pretty();
```

Or in one command:

```bash
docker exec -it etl-sync-project-mongo-1 mongosh --eval "use etl_db; db.users.find().pretty()"
```

You should see the inserted, updated, or deleted documents mirrored from MySQL.

---

## Project Structure

```
.
├── docker-compose.yml           # Docker Compose configuration
├── init-scripts/
│   └── init.sql                 # Initialize MySQL schema, table, sample data, and Debezium user
├── mysql/
│   └── my.cnf                   # MySQL configuration (binlog settings)
├── register_connector.py        # Python script to register the Debezium connector
├── spark/
│   └── etl_job.py               # Spark Structured Streaming job for CDC processing
├── README.md                    # This file
└── spark/checkpoint/            # Checkpoint directory for Spark (auto-created)
```

---

## Configuration Details

### MySQL (`my.cnf`)

Enable binary logging (ROW format):

```ini
[mysqld]
server-id=223344
log-bin=mysql-bin
binlog_format=row
expire_logs_days=1
```

### Debezium Connector (`register_connector.py`)

Key configuration fields:

* `connector.class`: `io.debezium.connector.mysql.MySqlConnector`
* `database.hostname`, `port`, `user`, `password`
* `topic.prefix`: prefix for generated Kafka topics
* `database.include.list`, `table.include.list`: filter for monitored schemas/tables
* `database.history.kafka.bootstrap.servers`, `database.history.kafka.topic`
* `snapshot.mode`: `initial` to snapshot existing data

### Spark ETL Job (`etl_job.py`)

* Reads from Kafka topic via Spark Structured Streaming
* Parses JSON against a defined CDC schema
* `foreachBatch` logic:

  * Upsert (insert/update) using Mongo Spark Connector
  * Delete operations via PyMongo client
* MongoDB output URI: `mongodb://mongo:27017/etl_db.users`
* Checkpoint location: `/opt/spark-app/checkpoint`

---

## Cleaning Up

To stop and remove containers, networks, and volumes:

```bash
docker-compose down -v
```


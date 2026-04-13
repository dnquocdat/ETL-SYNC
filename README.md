# 🔄 ETL-SYNC: E-Commerce Real-time Data Sync Platform

A production-grade system that synchronizes data in **real-time** from **MySQL** to **MongoDB** using a CDC (Change Data Capture) architecture.

Whenever data in MySQL changes (INSERT/UPDATE/DELETE), the system automatically detects and synchronizes it to MongoDB within seconds — **without requiring any database triggers or cron jobs**.

---

## 📖 Table of Contents

1. [What does this system do?](#-what-does-this-system-do)
2. [Architecture Overview](#-architecture-overview)
3. [Prerequisites](#-prerequisites)
4. [Step-by-Step Setup Guide](#-step-by-step-setup-guide)
5. [Verifying the Sync](#-verifying-the-sync)
6. [Load Testing with Data Generator](#-load-testing-with-data-generator)
7. [Service URLs](#-service-urls)
8. [Project Structure](#-project-structure)
9. [Performance Optimizations](#-performance-optimizations)
10. [Troubleshooting](#-troubleshooting)
11. [Cleanup](#-cleanup)

---

## 🎯 What does this system do?

**Scenario:** An e-commerce system uses MySQL as its primary OLTP database. We need to synchronize 4 core tables to MongoDB in real-time to serve:
- **Product Catalog API** — fast reads for product listings
- **Order Analytics** — complex queries and aggregations on MongoDB
- **Inventory Alerts** — real-time monitoring of stock levels

| MySQL Table | MongoDB Collection | Data Overview |
|---|---|---|
| `products` | `products` | 20 products (electronics, fashion, home) |
| `inventory` | `inventory` | Stock levels per warehouse |
| `orders` | `orders` | Customer orders and their status |
| `order_items` | `order_items` | Details of items within each order |

---

## 🏗️ Architecture Overview

```text
MySQL (binlog) → Debezium → Kafka (3 brokers) → Spark Streaming → MongoDB
                                                       ↓
                                                  [Validation]
                                                   ↙       ↘
                                              MongoDB    Dead Letter Queue
                                              (valid)    (invalid records)
```

| Component | Role |
|---|---|
| **MySQL** | Source database with binlog enabled to record all changes. |
| **Debezium** | Reads MySQL binlogs and converts them into events sent to Kafka. |
| **Kafka** (3 nodes KRaft) | Message queue acting as the central nervous system for CDC events. |
| **Spark Structured Streaming** | Consumes events from Kafka, validates them, and writes to MongoDB. |
| **MongoDB** | Target document database mirroring the MySQL data. |
| **Kafka-UI** | Web interface for managing the Kafka cluster and topics. |
| **Prometheus + Grafana** | Monitoring stack for pipeline performance and health. |

---

## 💻 Prerequisites

Ensure you have the following installed on your machine:

| Software | Minimum Version | How to check |
|---|---|---|
| **Docker Desktop** | 4.x | `docker --version` |
| **Docker Compose** | 2.x | `docker compose version` |
| **Python** | 3.8+ | `python --version` |
| **Available RAM** | ~4 GB | — |

> 💡 **Tip:** If you are using Windows, make sure Docker Desktop is running (the 🐳 icon in the system tray).

---

## 🚀 Step-by-Step Setup Guide

### Step 1: Clone the project

```bash
git clone https://github.com/dnquocdat/ETL-SYNC.git
cd ETL-SYNC
```

### Step 2: Create the environment configuration

```bash
# Create the .env file from the provided template
cp .env.example .env
```

> 📝 The `.env` file contains passwords and configurations. The default values are sufficient to run the project.

### Step 3: Start all services

```bash
# This command downloads the Docker images and starts all containers
# The first run may take 3-5 minutes to pull the images
docker compose up -d
```

You should see output similar to this:

```text
[+] Running 12/12
 ✔ Container kafka1                  Started
 ✔ Container kafka2                  Started
 ✔ Container kafka3                  Started
 ✔ Container kafka-init              Started
 ✔ Container etl-sync-mysql-1        Started
 ✔ Container etl-sync-mongo-1        Started
 ✔ Container etl-sync-debezium-1     Started
 ✔ Container etl-sync-spark-1        Started
 ✔ Container kafka-ui                Started
 ✔ Container etl-sync-prometheus-1   Started
 ✔ Container etl-sync-pushgateway-1  Started
 ✔ Container etl-sync-grafana-1      Started
```

### Step 4: Wait for services to initialize

```bash
# Check if all containers are running
docker ps

# Wait ~30 seconds for Debezium to be fully ready
# You can test it with this command:
curl http://localhost:8083/
```

When Debezium is ready, the above `curl` command will return a JSON response containing its version.

### Step 5: Register the Debezium Connector

This step "tells" Debezium which MySQL tables it needs to monitor.

```bash
# Install required Python libraries
pip install -r requirements.txt

# Run the connector registration script
python scripts/register_connectors.py
```

Expected output:

```text
⏳ Waiting for Debezium Connect... ✅ Ready!
🆕 Creating connector 'mysql-ecommerce-connector'...
   Status: 201
   ✅ Connector created successfully!

📋 Connector Status:
   Connector: RUNNING
   Task 0: RUNNING
```

> ⚠️ If it gets stuck at `Waiting...`, check the logs: `docker logs etl-sync-debezium-1`.

### Step 6: Verify System Health 🏥

Run the health check script to ensure all components are communicating properly:

```bash
python scripts/health_check.py
```

You should see all 7 checks pass:
```text
📋 Summary: 7/7 checks passed ✅ All healthy!
```

---

## ✅ Verifying the Sync

### 6.1 — Check initial synced data

```bash
# Connect to MongoDB and count documents
docker exec -it etl-sync-mongo-1 mongosh --eval "
  use etl_db;
  print('Products:', db.products.countDocuments());
  print('Inventory:', db.inventory.countDocuments());
  print('Orders:', db.orders.countDocuments());
  print('Order Items:', db.order_items.countDocuments());
"
```

Expected result:

```text
Products: 20
Inventory: 21
Orders: 10
Order Items: 14
```

### 6.2 — Test INSERT (Adding data)

```bash
# Insert a new product into MySQL
docker exec -it etl-sync-mysql-1 mysql -uroot -prootpwd etl_db -e "
  INSERT INTO products (name, category, price, description)
  VALUES ('Test Product', 'test', 99.99, 'This is a test');
"

# Wait a few seconds, then query MongoDB
docker exec -it etl-sync-mongo-1 mongosh --eval "
  use etl_db;
  db.products.find({name: 'Test Product'}).pretty();
"
```

The `Test Product` document will automatically appear in MongoDB! ✅

### 6.3 — Test UPDATE (Modifying data)

```bash
# Update the product price in MySQL
docker exec -it etl-sync-mysql-1 mysql -uroot -prootpwd etl_db -e "
  UPDATE products SET price = 199.99 WHERE name = 'Test Product';
"

# Wait a few seconds, then query MongoDB
docker exec -it etl-sync-mongo-1 mongosh --eval "
  use etl_db;
  db.products.find({name: 'Test Product'}, {price: 1}).pretty();
"
```

The price will change to `199.99` in MongoDB via an upsert operation. ✅

### 6.4 — Test DELETE (Removing data)

```bash
# Delete the test product in MySQL
docker exec -it etl-sync-mysql-1 mysql -uroot -prootpwd etl_db -e "
  DELETE FROM products WHERE name = 'Test Product';
"

# Wait a few seconds, then query MongoDB
docker exec -it etl-sync-mongo-1 mongosh --eval "
  use etl_db;
  db.products.find({name: 'Test Product'}).count();
"
```

Result: `0` — the document has been successfully deleted. ✅

---

## 📈 Load Testing with Data Generator

The `data_generator.py` script automatically creates orders, updates statuses, changes prices, and restocks inventory to simulate real-world traffic.

```bash
# Run 5 operations per second for 60 seconds
python scripts/data_generator.py --rate 5 --duration 60
```

Output:

```text
============================================================
🚀 ETL-SYNC Data Generator
   Host: localhost:3307
   Rate: 5.0 ops/sec | Duration: 60s
============================================================
  ✅ INSERT order #11 | 2 items | $1249.98
  🔄 UPDATE order #3: processing → shipped
  📦 RESTOCK inventory #5 (product #5): +47 units
  📈 PRICE product #7 (Adidas Ultraboost): $189.99 → $212.39
  ❌ CANCEL order #10
  ...
```

While the generator is running, open **Grafana** at `http://localhost:3000` to watch the real-time monitoring dashboard update.

---

## 🖥️ Service URLs

Once the system is running, you can access the following web interfaces:

| Service | URL | Default Credentials |
|---------|-----|-----------|
| **Kafka-UI** (manage cluster/topics) | http://localhost:8000 | None |
| **Grafana** (monitoring dashboards) | http://localhost:3000 | `admin` / `admin` |
| **Prometheus** (raw metrics) | http://localhost:9090 | None |
| **Debezium API** (connector status) | http://localhost:8083 | None |

**Databases** (Connect using tools like DBeaver or MongoDB Compass):

| Database | Host | Port | User | Password |
|----------|------|------|------|----------|
| **MySQL** | localhost | 3307 | root | rootpwd |
| **MongoDB** | localhost | 27017 | — | — |

---

## 📂 Project Structure

```text
ETL-SYNC/
│
├── docker-compose.yml                 # Orchestrates all Docker containers
├── .env.example                       # Environment variables template
├── requirements.txt                   # Python dependencies
├── README.md                          # This documentation file
│
├── init-scripts/
│   └── init-mysql.sql                 # E-commerce schema & seed data
│
├── mysql/
│   └── my.cnf                         # Enables MySQL binlog (required for CDC)
│
├── spark/
│   ├── etl_job.py                     # ⭐ Main Spark Streaming job
│   ├── config.py                      #    Centralized configuration
│   ├── metrics.py                     #    Prometheus metrics exporter
│   └── validators.py                  #    Data quality validation layer
│
├── scripts/
│   ├── register_connectors.py         # Registers the Debezium connector
│   ├── data_generator.py              # Simulates realistic database traffic
│   └── health_check.py                # End-to-end system health verification
│
└── monitoring/
    ├── prometheus.yml                 # Prometheus scrape configuration
    └── grafana/
        ├── provisioning/              # Auto-provisions datasources & dashboards
        └── dashboards/
            └── etl-sync.json          # Pre-built monitoring dashboard
```

---

## ⚡ Performance Optimizations

This project implements several production-grade optimizations compared to a basic CDC tutorial:

| Aspect | Basic Setup | This Production Setup |
|----------|-------------|------------------|
| **Kafka cluster** | 1 Node Zookeeper | 3-Node KRaft Cluster, 3 partitions per topic for HA |
| **MongoDB Writes** | Inefficient `append` mode | Idempotent `bulk_write` via `ReplaceOne(upsert=True)` |
| **DB Connections** | Re-created every micro-batch | Singleton Connection Pooling |
| **Routing** | Single hardcoded table | Regex topic subscription with dynamic routing |
| **Fault Tolerance** | Invalid data crashes pipeline | Validation checks + Dead Letter Queue (DLQ) topic |
| **Configuration** | Hardcoded variables | Externalized Environment Variables via `.env` |
| **Observability** | None | Prometheus + Pushgateway + Grafana Dashboard |

---

## 🛠️ Troubleshooting

| Issue | Solution |
|--------|-----------|
| `docker compose up` raises conflict errors | Run `docker compose down -v`, then `docker compose up -d` again. |
| Debezium connector `FAILED` | Check logs: `docker logs etl-sync-debezium-1` |
| Spark container restarts continuously | Check logs: `docker logs etl-sync-spark-1` |
| Data is not appearing in MongoDB | 1) Check connector: `curl http://localhost:8083/connectors/mysql-ecommerce-connector/status` <br> 2) Review Spark logs. |
| Kafka-UI fails to load | Wait ~30s for the KRaft cluster quorum to stabilize, then refresh. |
| Port conflicts (e.g., 3000 in use) | Change port mappings in `docker-compose.yml` (e.g., `"3001:3000"` for Grafana). |

### Viewing Detailed Logs

```bash
# Spark ETL process logs
docker logs etl-sync-spark-1 --tail 50

# Debezium Connect logs
docker logs etl-sync-debezium-1 --tail 50

# Kafka Broker 1 logs
docker logs kafka1 --tail 50
```

---

## 🧹 Cleanup

```bash
# Stop all containers and DELETE all data volumes
docker compose down -v

# Stop containers but KEEP data volumes
docker compose down
```

---

## 📝 Tech Stack

| Technology | Version | Role |
|---|---|---|
| MySQL | 8.0 | Source OLTP Database |
| Debezium | 2.7.3 | Change Data Capture (CDC) engine |
| Apache Kafka | latest | Event Streaming (3-Node KRaft) |
| Apache Spark | 3.5.3 | Stream processing and transformation |
| MongoDB | 8.0 | Target Document Database |
| Prometheus | latest | Metrics collection |
| Grafana | latest | Monitoring visualization |
| Kafka-UI | latest | Kafka cluster management UI |

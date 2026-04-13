"""
ETL-SYNC Connector Registration
=================================
Registers Debezium MySQL connectors for all monitored tables.
Supports idempotent registration (creates or updates).
"""

import time
import sys
import requests
import json

DEBEZIUM_URL = "http://localhost:8083"
CONNECTORS_URL = f"{DEBEZIUM_URL}/connectors/"

CONNECTOR_CONFIG = {
    "name": "mysql-ecommerce-connector",
    "config": {
        "connector.class": "io.debezium.connector.mysql.MySqlConnector",
        "database.hostname": "mysql",
        "database.port": "3306",
        "database.user": "debezium",
        "database.password": "dbzpwd",
        "database.server.id": "1",
        "topic.prefix": "mysql_server",

        # Monitor all 4 e-commerce tables
        "database.include.list": "etl_db",
        "table.include.list": "etl_db.products,etl_db.inventory,etl_db.orders,etl_db.order_items",

        # Schema history (Debezium 2.x property names)
        "schema.history.internal.kafka.bootstrap.servers": "kafka1:29092,kafka2:29092,kafka3:29092",
        "schema.history.internal.kafka.topic": "schema-changes.etl_db",

        # Snapshot settings
        "snapshot.mode": "initial",

        # Performance tuning
        "max.batch.size": "2048",
        "max.queue.size": "8192",
        "poll.interval.ms": "500",

        # Decimal handling — output as string (safer for precision)
        "decimal.handling.mode": "string",

        # Timestamp handling
        "time.precision.mode": "connect",

        # Heartbeat (keep connector alive during low-traffic periods)
        "heartbeat.interval.ms": "10000",

        # Schema evolution
        "include.schema.changes": "true",
    }
}


def wait_for_debezium(max_wait: int = 90):
    """Wait for Debezium Connect to be ready."""
    print("⏳ Waiting for Debezium Connect...", end="", flush=True)
    for i in range(max_wait // 5):
        try:
            r = requests.get(f"{DEBEZIUM_URL}/")
            if r.status_code == 200:
                print(" ✅ Ready!")
                return True
        except requests.exceptions.RequestException:
            pass
        print(".", end="", flush=True)
        time.sleep(5)

    print(" ❌ Timeout!")
    return False


def register_connector():
    """Register or update the Debezium connector (idempotent)."""
    name = CONNECTOR_CONFIG["name"]
    headers = {"Accept": "application/json", "Content-Type": "application/json"}

    # Check if connector already exists
    try:
        r = requests.get(f"{CONNECTORS_URL}{name}")
        if r.status_code == 200:
            print(f"🔄 Connector '{name}' exists, updating config...")
            r = requests.put(
                f"{CONNECTORS_URL}{name}/config",
                headers=headers,
                data=json.dumps(CONNECTOR_CONFIG["config"]),
            )
            print(f"   Status: {r.status_code}")
            if r.status_code in (200, 202):
                print(f"   ✅ Connector updated successfully!")
            else:
                print(f"   ⚠️ Response: {r.text}")
            return
    except requests.exceptions.RequestException:
        pass

    # Create new connector
    print(f"🆕 Creating connector '{name}'...")
    r = requests.post(
        CONNECTORS_URL,
        headers=headers,
        data=json.dumps(CONNECTOR_CONFIG),
    )
    print(f"   Status: {r.status_code}")
    if r.status_code == 201:
        print(f"   ✅ Connector created successfully!")
    else:
        print(f"   ⚠️ Response: {r.text}")


def check_connector_status():
    """Check and display connector status."""
    name = CONNECTOR_CONFIG["name"]
    try:
        r = requests.get(f"{CONNECTORS_URL}{name}/status")
        if r.status_code == 200:
            status = r.json()
            connector_state = status.get("connector", {}).get("state", "UNKNOWN")
            tasks = status.get("tasks", [])

            print(f"\n📋 Connector Status:")
            print(f"   Connector: {connector_state}")
            for task in tasks:
                task_state = task.get("state", "UNKNOWN")
                task_id = task.get("id", "?")
                print(f"   Task {task_id}: {task_state}")
                if task_state == "FAILED":
                    trace = task.get("trace", "")
                    print(f"   Error: {trace[:200]}")
        else:
            print(f"⚠️ Could not check status: HTTP {r.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"⚠️ Could not check status: {e}")


def main():
    if not wait_for_debezium():
        print("❌ Debezium Connect is not available. Exiting.")
        sys.exit(1)

    register_connector()
    time.sleep(3)
    check_connector_status()


if __name__ == "__main__":
    main()

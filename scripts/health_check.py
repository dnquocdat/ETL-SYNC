"""
ETL-SYNC Health Check
======================
Verifies all pipeline components are healthy and operational.

Usage:
    python health_check.py [--host HOST]
"""

import sys
import argparse
import requests
from pymongo import MongoClient
import mysql.connector


class HealthChecker:
    def __init__(self, host="localhost"):
        self.host = host
        self.results = []

    def check(self, name: str, fn):
        """Run a health check and record the result."""
        try:
            result = fn()
            self.results.append((name, True, result))
            print(f"  ✅ {name}: {result}")
        except Exception as e:
            self.results.append((name, False, str(e)))
            print(f"  ❌ {name}: {e}")

    def check_kafka(self):
        """Check Kafka connectivity via Kafka-UI API."""
        r = requests.get(f"http://{self.host}:8000/api/clusters/etl-sync-cluster/topics", timeout=5)
        topics = r.json()
        topic_names = [t.get("name", t) if isinstance(t, dict) else t for t in topics]
        cdc_topics = [t for t in topic_names if "etl_db" in str(t)]
        return f"{len(cdc_topics)} CDC topics found"

    def check_debezium(self):
        """Check Debezium Connect and connector status."""
        r = requests.get(f"http://{self.host}:8083/connectors", timeout=5)
        connectors = r.json()
        if not connectors:
            return "No connectors registered"

        statuses = []
        for name in connectors:
            sr = requests.get(f"http://{self.host}:8083/connectors/{name}/status", timeout=5)
            state = sr.json().get("connector", {}).get("state", "UNKNOWN")
            statuses.append(f"{name}={state}")
        return ", ".join(statuses)

    def check_mysql(self):
        """Check MySQL connectivity and table count."""
        conn = mysql.connector.connect(
            host=self.host, port=3307, user="root",
            password="rootpwd", database="etl_db",
        )
        cursor = conn.cursor()
        cursor.execute("SHOW TABLES")
        tables = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        return f"Tables: {', '.join(tables)}"

    def check_mongodb(self):
        """Check MongoDB connectivity and collection stats."""
        client = MongoClient(f"mongodb://{self.host}:27017", serverSelectionTimeoutMS=5000)
        db = client["etl_db"]
        collections = db.list_collection_names()
        stats = []
        for coll_name in collections:
            count = db[coll_name].count_documents({})
            stats.append(f"{coll_name}({count})")
        client.close()
        return f"Collections: {', '.join(stats) if stats else 'empty'}"

    def check_prometheus(self):
        """Check Prometheus connectivity."""
        r = requests.get(f"http://{self.host}:9090/-/healthy", timeout=5)
        return "Healthy" if r.status_code == 200 else f"HTTP {r.status_code}"

    def check_grafana(self):
        """Check Grafana connectivity."""
        r = requests.get(f"http://{self.host}:3000/api/health", timeout=5)
        data = r.json()
        return f"Version: {data.get('version', 'unknown')}, DB: {data.get('database', 'unknown')}"

    def check_pushgateway(self):
        """Check Prometheus Pushgateway."""
        r = requests.get(f"http://{self.host}:9091/-/healthy", timeout=5)
        return "Healthy" if r.status_code == 200 else f"HTTP {r.status_code}"

    def run_all(self):
        print("=" * 60)
        print("🏥 ETL-SYNC Pipeline Health Check")
        print("=" * 60)

        print("\n📡 Core Services:")
        self.check("MySQL",    self.check_mysql)
        self.check("MongoDB",  self.check_mongodb)

        print("\n🔗 CDC Pipeline:")
        self.check("Debezium", self.check_debezium)
        self.check("Kafka",    self.check_kafka)

        print("\n📊 Monitoring:")
        self.check("Prometheus",  self.check_prometheus)
        self.check("Grafana",     self.check_grafana)
        self.check("Pushgateway", self.check_pushgateway)

        # Summary
        total = len(self.results)
        passed = sum(1 for _, ok, _ in self.results if ok)
        failed = total - passed

        print(f"\n{'=' * 60}")
        print(f"📋 Summary: {passed}/{total} checks passed", end="")
        if failed:
            print(f" ({failed} failed)")
            return 1
        else:
            print(" ✅ All healthy!")
            return 0


def main():
    parser = argparse.ArgumentParser(description="ETL-SYNC Health Check")
    parser.add_argument("--host", default="localhost", help="Host address")
    args = parser.parse_args()

    checker = HealthChecker(host=args.host)
    exit_code = checker.run_all()
    sys.exit(exit_code)


if __name__ == "__main__":
    main()

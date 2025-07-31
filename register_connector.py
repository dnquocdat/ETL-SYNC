import time
import requests
import json

URL = "http://localhost:8083/connectors/"

# 1. Đợi Debezium sẵn sàng (tối đa 60s)
print("Waiting for Debezium to be ready...", end="", flush=True)
for _ in range(12):
    try:
        r = requests.get("http://localhost:8083/")
        if r.status_code == 200:
            print(" OK")
            break
    except requests.exceptions.RequestException:
        pass
    print(".", end="", flush=True)
    time.sleep(5)
else:
    raise RuntimeError("Debezium did not become ready in time")

# 2. Chuẩn bị payload
headers = {"Accept":"application/json","Content-Type":"application/json"}
body = {
    "name": "mysql-connector",
    "config": {
        "connector.class": "io.debezium.connector.mysql.MySqlConnector",
        "database.hostname": "mysql",
        "database.port": "3306",
        "database.user": "debezium",
        "database.password": "dbzpwd",
        "database.server.id": "1",
        "topic.prefix": "mysql_server",
        "database.include.list": "etl_db",
        "table.include.list": "etl_db.users",
        "database.history.kafka.bootstrap.servers": "kafka:9092",
        "database.history.kafka.topic": "schema-changes.etl_db",
        "snapshot.mode": "initial"
    }
}

# 3. Gửi POST tạo connector
response = requests.post(URL, headers=headers, data=json.dumps(body))
print("Status Code:", response.status_code)
print("Response:", response.text)

import json
import uuid
import time
from datetime import datetime
from kafka import KafkaProducer
import psycopg2

# --------------------------------------------------
# Postgres (Master DB)
# --------------------------------------------------
conn = psycopg2.connect(
    host="localhost",
    dbname="medwarehouse_master",
    user="kushagra",
    password="kushagra"
)
cur = conn.cursor()

cur.execute("SELECT product_id FROM products LIMIT 1;")
product_id = str(cur.fetchone()[0])

cur.execute("SELECT location_id FROM warehouse_locations LIMIT 1;")
warehouse_id = str(cur.fetchone()[0])

# --------------------------------------------------
# Kafka Producer
# --------------------------------------------------
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda v: v.encode("utf-8")
)

while True:
    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": "STOCK_RECEIVED",
        "event_time": datetime.utcnow().isoformat(),
        "producer": "inventory_service",
        "schema_version": 1,
        "payload": {
            "product_id": product_id,
            "warehouse_id": warehouse_id,
            "batch_number": "BATCH-001",
            "expiry_date": "2026-12-31",
            "quantity_delta": 100,
            "supplier_id": None
        }
    }

    producer.send(
        "inventory_events",
        key=product_id,
        value=event
    )

    print("Produced inventory event:", event)
    time.sleep(5)


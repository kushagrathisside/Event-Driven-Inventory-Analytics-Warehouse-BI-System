import json
import uuid
from datetime import datetime
from confluent_kafka import Producer

producer = Producer({
    "bootstrap.servers": "localhost:9092"
})

event = {
    "event_id": str(uuid.uuid4()),
    "event_type": "SALE_CREATED",
    "event_time": datetime.utcnow().isoformat(),
    "producer": "sales_service",
    "schema_version": 1,
    "payload": {
        "sale_id": str(uuid.uuid4()),
        "product_id": "P-001",
        "warehouse_id": "W-01",
        "quantity": 5,
        "unit_price": 120.50,
        "currency": "INR",
        "customer_type": "RETAIL"
    }
}

producer.produce(
    topic="sales_events",
    key=event["payload"]["product_id"],
    value=json.dumps(event)
)

producer.flush()
print("Sales event produced")


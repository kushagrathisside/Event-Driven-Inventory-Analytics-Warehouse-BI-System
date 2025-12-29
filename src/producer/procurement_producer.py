import json
import uuid
from datetime import datetime
from confluent_kafka import Producer

producer = Producer({
    "bootstrap.servers": "localhost:9092"
})

event = {
    "event_id": str(uuid.uuid4()),
    "event_type": "PO_CREATED",
    "event_time": datetime.utcnow().isoformat(),
    "producer": "procurement_service",
    "schema_version": 1,
    "payload": {
        "po_id": str(uuid.uuid4()),
        "product_id": "P-001",
        "ordered_quantity": 200,
        "supplier_id": "SUP-01",
        "trigger_reason": "AUTO_REORDER"
    }
}

producer.produce(
    topic="procurement_events",
    key=event["payload"]["product_id"],
    value=json.dumps(event)
)

producer.flush()
print("Procurement event produced")


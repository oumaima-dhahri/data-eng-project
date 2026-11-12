import json
from datetime import datetime, timezone
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

for i in range(2):
    order_event = {
        "event_id": 12,
        "order_id": "OLIST_0012",
        "customer_id": "CUST_0998",
        "order_status": "created",
        "order_amount": 199.90,
        "event_timestamp": datetime.now(timezone.utc).isoformat()
    }

    metadata = producer.send("orders", value=order_event).get(timeout=10)

    print(
        f"Envoi {i + 1}/2 : event_id={order_event['event_id']}, "
        f"partition={metadata.partition}, offset={metadata.offset}"
    )

producer.flush()
producer.close()

print("Test terminé : deux événements identiques envoyés.")

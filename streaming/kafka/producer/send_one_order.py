import json
from datetime import datetime, timezone
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

order_event = {
    "event_id": 11,
    "order_id": "OLIST_0011",
    "customer_id": "CUST_0999",
    "order_status": "created",
    "order_amount": 149.90,
    "event_timestamp": datetime.now(timezone.utc).isoformat()
}

future = producer.send("orders", value=order_event)
metadata = future.get(timeout=10)

producer.flush()
producer.close()

print(f"Nouvel événement envoyé : {order_event['order_id']}")
print(f"Topic : {metadata.topic}, partition : {metadata.partition}, offset : {metadata.offset}")

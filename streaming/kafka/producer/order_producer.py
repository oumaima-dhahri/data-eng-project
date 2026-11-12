import json
import time
import random
from datetime import datetime, timezone
from kafka import KafkaProducer

# Connexion au broker Kafka
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

# Simulation de commandes Olist
for i in range(1, 11):
    order_event = {
        "event_id": i,
        "order_id": f"OLIST_{i:04d}",
        "customer_id": f"CUST_{random.randint(1, 1000):04d}",
        "order_status": random.choice(["created", "approved", "shipped"]),
        "order_amount": round(random.uniform(20, 500), 2),
        "event_timestamp": datetime.now(timezone.utc).isoformat()
    }

    producer.send("orders", value=order_event)

    print(f"Événement envoyé : {order_event['order_id']}")

    time.sleep(1)

producer.flush()
producer.close()

print("Tous les événements ont été envoyés avec succès !")

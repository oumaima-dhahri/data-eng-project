import json
from kafka import KafkaConsumer

# Connexion au Topic Kafka
consumer = KafkaConsumer(
    "orders",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    group_id="olist-consumer-group-v1",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("Consumer connecté ! En attente des commandes...\n")

count = 0

try:
    for message in consumer:
        order = message.value

        print(f"Commande reçue : {order['order_id']}")
        print(f"Client : {order['customer_id']}")
        print(f"Statut : {order['order_status']}")
        print(f"Montant : {order['order_amount']} TND")
        print("-" * 40)

        count += 1

        if count == 10:
            break

finally:
    consumer.close()

print(f"\nTotal des commandes reçues : {count}")

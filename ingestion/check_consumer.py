"""Consumer de vérification : lit quelques messages et affiche partition / offset."""
import json
import os

from confluent_kafka import Consumer

consumer = Consumer({
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
    "group.id": "velib-check",
    "auto.offset.reset": "earliest",
})
consumer.subscribe(["velib-station-status"])

try:
    for _ in range(5):
        msg = consumer.poll(10)
        if msg is None:
            print("Aucun message reçu")
            continue
        if msg.error():
            print("Erreur :", msg.error())
            continue
        v = json.loads(msg.value())
        print(f"partition={msg.partition()} offset={msg.offset()} "
              f"station={msg.key().decode()} vélos={v['num_bikes_available']}")
finally:
    consumer.close()
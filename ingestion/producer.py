"""Producer Kafka : publie l'état des stations Vélib' (un message par station)."""
import json
import logging
import os
import time
from datetime import datetime, timezone

import requests
from confluent_kafka import KafkaException, Producer

from collector import build_session, fetch_feed

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
STATUS_TOPIC = os.getenv("STATUS_TOPIC", "velib-station-status")
INFO_TOPIC = os.getenv("INFO_TOPIC", "velib-station-information")
STATUS_EVERY = int(os.getenv("STATUS_EVERY_SECONDS", "60"))
INFO_EVERY = int(os.getenv("INFO_EVERY_SECONDS", "86400"))  # 1 fois par jour (cf. Q1)

log = logging.getLogger("velib.producer")


class DeliveryStats:
    """Compte les messages confirmés ou en échec par le broker."""

    def __init__(self) -> None:
        self.ok = 0
        self.failed = 0

    def callback(self, err, msg) -> None:
        if err is not None:
            self.failed += 1
            log.error("Échec de livraison sur %s : %s", msg.topic(), err)
        else:
            self.ok += 1


def build_producer() -> Producer:
    return Producer({
        "bootstrap.servers": BOOTSTRAP,
        "acks": "all",                # le broker confirme l'écriture
        "enable.idempotence": True,   # pas de doublons en cas de retry
        "compression.type": "lz4",
        "linger.ms": 50,              # regroupe les messages en batchs
        "client.id": "velib-producer",
    })


def publish_feed(producer: Producer, session, feed: str, topic: str, stats: DeliveryStats) -> int:
    payload = fetch_feed(session, feed)
    ingested_at = datetime.now(timezone.utc).isoformat()
    stations = payload["data"]["stations"]
    for station in stations:
        record = {**station, "ingested_at": ingested_at}
        producer.produce(
            topic,
            key=str(station["station_id"]),
            value=json.dumps(record, ensure_ascii=False),
            on_delivery=stats.callback,
        )
        producer.poll(0)  # déclenche les callbacks au fil de l'eau
    producer.flush(10)
    return len(stations)


def main() -> None:
    session = build_session()
    producer = build_producer()
    stats = DeliveryStats()
    last_info = None
    log.info("Producer démarré -> %s", BOOTSTRAP)
    try:
        while True:
            start = time.monotonic()
            try:
                if last_info is None or start - last_info >= INFO_EVERY:
                    n = publish_feed(producer, session, "station_information", INFO_TOPIC, stats)
                    last_info = start
                    log.info("station_information : %d messages -> %s", n, INFO_TOPIC)
                n = publish_feed(producer, session, "station_status", STATUS_TOPIC, stats)
                log.info("station_status : %d messages -> %s (total ok=%d, échecs=%d)",
                         n, STATUS_TOPIC, stats.ok, stats.failed)
            except (requests.RequestException, KeyError, ValueError, KafkaException) as exc:
                log.error("Cycle en échec : %s", exc)
            time.sleep(max(0, STATUS_EVERY - (time.monotonic() - start)))
    except KeyboardInterrupt:
        log.info("Arrêt demandé, vidage du buffer...")
    finally:
        producer.flush(10)


if __name__ == "__main__":
    main()
"""Collecte périodique des données Vélib"""
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole"
FEEDS = ["station_information", "station_status"]
RAW_DIR = Path(os.getenv("RAW_DIR", "data/raw"))
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "60"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("velib.collector")


def build_session() -> requests.Session:
    """Session HTTP avec retry et backoff."""
    retry = Retry(total=3, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def fetch_feed(session: requests.Session, feed: str) -> dict:
    resp = session.get(f"{BASE_URL}/{feed}.json", timeout=10)
    resp.raise_for_status()
    return resp.json()


def save_raw(feed: str, payload: dict, ingested_at: datetime) -> Path:
   
    partition = RAW_DIR / feed / f"date={ingested_at:%Y-%m-%d}" / f"hour={ingested_at:%H}"
    partition.mkdir(parents=True, exist_ok=True)
    path = partition / f"{feed}_{ingested_at:%Y%m%dT%H%M%S}.json"
    record = {
        "ingested_at": ingested_at.isoformat(),
        "source": f"{BASE_URL}/{feed}.json",
        "payload": payload,
    }
    path.write_text(json.dumps(record, ensure_ascii=False))
    return path


def run_once(session: requests.Session) -> None:
    ingested_at = datetime.now(timezone.utc)
    for feed in FEEDS:
        try:
            payload = fetch_feed(session, feed)
            n = len(payload["data"]["stations"])
            path = save_raw(feed, payload, ingested_at)
            log.info("%s : %d stations -> %s", feed, n, path)
        except (requests.RequestException, KeyError, ValueError) as exc:
            log.error("Échec sur %s : %s", feed, exc)


def main() -> None:
    session = build_session()
    log.info("Démarrage de la collecte (toutes les %ss)", POLL_SECONDS)
    try:
        while True:
            start = time.monotonic()
            run_once(session)
            time.sleep(max(0, POLL_SECONDS - (time.monotonic() - start)))
    except KeyboardInterrupt:
        log.info("Arrêt demandé, fin de la collecte.")


if __name__ == "__main__":
    main()
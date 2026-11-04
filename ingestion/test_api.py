import json
import requests

BASE = "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole"

info = requests.get(f"{BASE}/station_information.json", timeout=10).json()
status = requests.get(f"{BASE}/station_status.json", timeout=10).json()

stations = info["data"]["stations"]
statuses = status["data"]["stations"]

print(f"{len(stations)} stations référencées")
print("--- Exemple station_information ---")
print(json.dumps(stations[0], indent=2, ensure_ascii=False))
print("--- Exemple station_status ---")
print(json.dumps(statuses[0], indent=2, ensure_ascii=False))
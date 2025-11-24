import requests
from typing import Dict, Any, Optional

class EVCentralAPI:
    def __init__(self, server_url: str = "http://localhost:9000"):
        self.server_url = server_url.rstrip("/")

    # --- Charging points ---
    def load_charging_points(self) -> Dict[str, Any]:
        resp = requests.get(f"{self.server_url}/charging_points")
        resp.raise_for_status()
        return resp.json()

    def get_charging_point(self, cp_id: str) -> Optional[Dict[str, Any]]:
        resp = requests.get(f"{self.server_url}/charging_point/{cp_id}")
        if resp.status_code == 404:
            resp.raise_for_status()
        resp.raise_for_status()
        return resp.json()

    def save_charging_point(self, cp: Dict[str, Any]):
        resp = requests.post(f"{self.server_url}/charging_point/save", json=cp)
        resp.raise_for_status()

    def update_estado(self, cp_id: str, estado: str):
        resp = requests.post(f"{self.server_url}/charging_point/estado", json={"cp_id": cp_id, "estado": estado})
        resp.raise_for_status()

    def set_driver(self, cp_id: str, driver: Optional[str]):
        resp = requests.post(f"{self.server_url}/charging_point/set_driver", json={"cp_id": cp_id, "driver": driver})
        resp.raise_for_status()

    def update_kwh(self, cp_id: str, kwh: float):
        resp = requests.post(f"{self.server_url}/charging_point/kwh", json={"cp_id": cp_id, "kwh": kwh})
        resp.raise_for_status()

    def start_time(self, cp_id: str, timestamp: Optional[float]):
        resp = requests.post(f"{self.server_url}/charging_point/start_time", json={"cp_id": cp_id, "time": timestamp})
        resp.raise_for_status()

    def reset_all_charging_points(self):
        resp = requests.post(f"{self.server_url}/charging_points/reset")
        resp.raise_for_status()

    # --- Drivers ---
    def save_driver(self, driver_id: str, location: str):
        resp = requests.post(f"{self.server_url}/driver/save", json={"id": driver_id, "location": location})
        resp.raise_for_status()

    def load_drivers(self) -> Dict[str, str]:
        resp = requests.get(f"{self.server_url}/drivers")
        resp.raise_for_status()
        return resp.json()

    # --- Tickets ---
    def guardar_ticket(self, driver: str, cp_id: str, total: float):
        resp = requests.post(f"{self.server_url}/ticket/save", json={"driver": driver, "cp_id": cp_id, "total": total})
        resp.raise_for_status()

    def get_tickets_by_driver(self, driver: str):
        resp = requests.get(f"{self.server_url}/tickets/{driver}")
        resp.raise_for_status()
        return resp.json()
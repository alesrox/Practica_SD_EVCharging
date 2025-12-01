import time
import json
import socket
import argparse
import requests

class Monitor:
    def __init__(
        self, id: str,
        central="localhost:6000",
        engine="localhost:6001",
        registry="localhost:8000",
    ):
        self.id = id
        self.location = None
        self.price = 0

        self.central = central
        self.central_host, self.central_port = self.central.split(":")
        self.central_port = int(self.central_port)
        
        self.engine = engine
        self.engine_host, self.engine_port = self.engine.split(":")
        self.engine_port = int(self.engine_port)

        self.registry = f"https://{registry}"

    def _send(self, mensaje: dict):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((self.central_host, self.central_port))
                s.send(json.dumps(mensaje).encode("utf-8"))

                start_time = time.time()
                while time.time() - start_time < 2:
                    try:
                        data = s.recv(4096)
                        if not data: continue
                        response = json.loads(data.decode("utf-8"))

                        if response.get("id") == self.id:
                            if response.get("type") == "auth":
                                print(f"[AUTH] Respuesta de central: OK ({self.id})")
                            elif response.get("type") == "status":
                                print(f"[STATUS_CHECK] Respuesta de central: OK ({self.id})")
                    except json.JSONDecodeError:
                        continue

        except ConnectionRefusedError:
            print(f"[{self.id}] No se pudo conectar con central en {self.central_host}:{self.central_port}")

    def _check_engine(self) -> str:
        print("[INFO] Comprobando estado de Engine")
        mensaje = {"type": "check", "id": self.id}
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(2)
                s.connect((self.engine_host, self.engine_port))
                s.send(json.dumps(mensaje).encode("utf-8"))

                start_time = time.time()
                while time.time() - start_time < 2:
                    try:
                        data = s.recv(4096)
                        if not data:
                            continue
                        response = json.loads(data.decode("utf-8"))

                        if response.get("type") == "status" and response.get("id") == self.id:
                            self.location = response.get("location")
                            self.price = response.get("price")

                            print("[INFO] Engine Status: OK")
                            return response["status"]
                    except Exception:
                        continue
        except (ConnectionRefusedError, socket.timeout):
            pass

        print("[INFO] Engine Status: KO")
        return "AVERIADO"

    def auth_cp(self):
        while self._check_engine() == "AVERIADO":
            time.sleep(1)

        url = self.registry + "/alta"
        payload = {
            "cp_id": self.id,
            "location": self.location or "Unknown Location",
            "price": self.price
        }
        try:
            res = requests.put(url, json=payload, verify="registry_cert.pem")
            res.raise_for_status()
            data = res.json()
            if "keys_for_EV_Central" in data:
                keys = data["keys_for_EV_Central"]

                self.auth_key = keys["auth_key"]
                self.session_key = keys["session_key"]

                print("[AUTH] Autenticado correctamente.")
                print(keys)
            else:
                print("[ERROR] Respuesta inválida del registry.")
                raise Exception("Respuesta inválida del registry.")
        except requests.exceptions.HTTPError as e:
            print(f"Error HTTP: {e}")
            print("-" * 30)
            print("LO QUE DICE EL SERVIDOR (El error real):")
            # Esto imprimirá exactamente qué campo falta o está mal
            print(json.dumps(res.json(), indent=2)) 
            print("-" * 30)
            raise e

        # mensaje = {"type": "keys", "id": self.id, "auth_key": self.auth_key, "session_key": self.session_key}
        # with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        #     s.settimeout(2)
        #     s.connect((self.engine_host, self.engine_port))
        #     s.send(json.dumps(mensaje).encode("utf-8"))

    def update_status(self, intervalo: int = 1):
        while True:
            msg = self._check_engine()
            mensaje = {
                "type": "status",
                "id": self.id,
                "status": msg
            }
            
            print(f"[{self.id}] Enviando estado: {msg}")
            self._send(mensaje)
            time.sleep(intervalo) # * (3 if msg == "KO" else 1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("id", help="ID del Charging Point")
    parser.add_argument("--central", default="localhost:6000", help="IP de la central")
    parser.add_argument("--engine", default="localhost:6001", help="IP del engine")
    parser.add_argument("--registry", default="localhost:8000", help="IP del registry")

    args = parser.parse_args()

    monitor = Monitor(
        id=args.id,
        central=args.central,
        engine=args.engine,
        registry=args.registry
    )

    monitor.auth_cp()
    monitor.update_status(intervalo=1)
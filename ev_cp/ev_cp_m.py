import time
import json
import socket
import argparse

class Monitor:
    def __init__(
        self, id: str,
        central_host="localhost", central_port=5001,
        engine_host="localhost", engine_port=5002
    ):
        self.id = id
        self.location = None
        self.price = 0

        self.central_host = central_host
        self.central_port = central_port

        self.engine_host = engine_host
        self.engine_port = engine_port

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
                    except json.JSONDecodeError:
                        continue

        except (ConnectionRefusedError, socket.timeout):
            pass

        print("[INFO] Engine Status: KO")
        return "AVERIADO"

    def auth_cp(self):
        while self._check_engine() == "AVERIADO":
            time.sleep(1)

        mensaje = {
            "type": "auth",
            "id": self.id,
            "location": self.location,
            "price": self.price
        }

        print(f"[{self.id}] Autenticando Charging Point...")
        self._send(mensaje)

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
    parser.add_argument("--location", default="Zone0", help="Ubicación del CP")
    parser.add_argument("--price", type=float, default=0.6, help="Precio por kWh")
    parser.add_argument("--central-host", default="localhost", help="IP de la central")
    parser.add_argument("--central-port", type=int, default=5001, help="Puerto de la central")
    parser.add_argument("--engine-host", default="localhost", help="IP del engine")
    parser.add_argument("--engine-port", type=int, default=5002, help="Puerto del engine")

    args = parser.parse_args()

    monitor = Monitor(
        id=args.id,
        central_host=args.central_host,
        central_port=args.central_port,
        engine_host=args.engine_host,
        engine_port=args.engine_port
    )

    monitor.auth_cp()
    monitor.update_status(intervalo=1)
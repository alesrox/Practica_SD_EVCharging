import time
import json
import socket
import argparse

class Monitor:
    def __init__(
        self, cp_id: str,
        central_host="localhost", central_port=5001,
        engine_host="localhost", engine_port=5002
    ):
        self.cp_id = cp_id
        self.ubicacion = ""

        self.central_host = central_host
        self.central_port = central_port

        self.engine_host = engine_host
        self.engine_port = engine_port

    def _send(self, mensaje: dict):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((self.central_host, self.central_port))
                s.send(json.dumps(mensaje).encode("utf-8"))
                respuesta = s.recv(1024)
            print(f"[{self.cp_id}] Respuesta del servidor: {respuesta.decode()}")
        except ConnectionRefusedError:
            print(f"[{self.cp_id}] No se pudo conectar al servidor en {self.central_host}:{self.central_port}")

    def auth_cp(self):
        mensaje = {
            "type": "auth",
            "id": self.cp_id,
            "ubicacion": self.ubicacion
        }

        self._send(mensaje)

    def _check_engine(self) -> str:
        mensaje = {"type": "check"}
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(2)  # tiempo máximo de espera
                s.connect((self.engine_host, self.engine_port))
                s.send(json.dumps(mensaje).encode("utf-8"))
                respuesta = s.recv(1024)
            if respuesta.decode() == "OK":
                return "OK"
        except (ConnectionRefusedError, socket.timeout):
            pass
        return "KO"

    def update_status(self, intervalo: int = 1):
        while True:
            msg = self._check_engine()
            mensaje = {
                "type": "status",
                "id": self.cp_id,
                "status": msg
            }
            self._send(mensaje)
            time.sleep(intervalo) # * (3 if msg == "KO" else 1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("id", help="ID del Charging Point")
    parser.add_argument("--central-host", default="localhost", help="IP de la central")
    parser.add_argument("--central-port", type=int, default=5001, help="Puerto de la central")
    parser.add_argument("--engine-host", default="localhost", help="IP del engine")
    parser.add_argument("--engine-port", type=int, default=5002, help="Puerto del engine")

    args = parser.parse_args()

    monitor = Monitor(
        cp_id=args.id,
        central_host=args.central_host,
        central_port=args.central_port,
        engine_host=args.engine_host,
        engine_port=args.engine_port
    )

    monitor.auth_cp()
    monitor.update_status(intervalo=1)
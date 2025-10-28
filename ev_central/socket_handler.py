import json
import socket
import threading

from charging_point import EstadoCP

class Socket_Handler:
    def __init__(self, gestor, host="0.0.0.0", port=6000):
        self.gestor = gestor
        self.host = host
        self.port = port
        self.server = None

    def start_listener(self):
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server.bind((self.host, self.port))
        self.server.listen(5)
        print(f"[SOCKET] Escuchando en {self.host}:{self.port}")

        try:
            while True:
                client, _ = self.server.accept()
                threading.Thread(
                    target=self._handle_client, args=(client,), daemon=True
                ).start()
        except Exception as e:
            print(f"[SOCKET] Error en el listener: {e}")
        finally:
            self.server.close()

    def _handle_client(self, client):
        with client:
            try:
                data = client.recv(4096)
                if not data:
                    return

                msg = json.loads(data.decode("utf-8"))
                msg_type = msg.get("type")

                if msg_type == "auth":
                    self._handle_auth(client, msg)
                elif msg_type == "status":
                    self._handle_status(client, msg)
                else:
                    client.send(b"ERROR - unknown msg")

            except Exception as e:
                print(f"[SOCKET] Error procesando mensaje: {e}")
                client.send(b"ERROR")

    def _handle_auth(self, client, msg):
        self.gestor.registrar_punto(msg["id"], msg)
        response = {"type": "auth", "id": msg["id"], "status": "OK"}
        client.send(json.dumps(response).encode("utf-8"))

    def _handle_status(self, client, msg):
        estado = EstadoCP[msg.get("status", "AVERIADO")]
        self.gestor.actualizar_estado(msg["id"], estado)
        response = {"type": "status", "id": msg["id"], "status": "OK"}
        client.send(json.dumps(response).encode("utf-8"))
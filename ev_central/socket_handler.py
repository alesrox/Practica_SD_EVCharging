import json
import socket
import threading

from charging_point import EstadoCP

class Socket_Handler:
    def __init__(self, gestor, host="localhost", port=5001):
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
                client, addr = self.server.accept()
                threading.Thread(target=self._handle_client, args=(client,), daemon=True).start()
        except Exception as e:
            print(f"[SOCKET] Error en el listener: {e}")
        finally:
            self.server.close()

    def _handle_client(self, client_socket):
        with client_socket:
            try:
                data = client_socket.recv(4096)
                if not data:
                    return
                mensaje = json.loads(data.decode("utf-8"))
                tipo = mensaje.get("type")

                if tipo == "auth":
                    self.gestor.registrar_punto(mensaje["id"], mensaje)
                    msg = {
                        "type": "auth",
                        "id": mensaje["id"],
                        "status": "OK",
                    }
                    client_socket.send(json.dumps(msg).encode("utf-8"))
                elif tipo == "status":
                    estado_str = mensaje.get("status", "AVERIADO")
                    estado = EstadoCP[estado_str]
                    self.gestor.actualizar_estado(mensaje["id"], estado)
                    msg = {
                        "type": "status",
                        "id": mensaje["id"],
                        "status": "OK",
                    }
                    client_socket.send(json.dumps(msg).encode("utf-8"))
                else:
                    client_socket.send(b"ERROR - unknown msg")
            except Exception as e:
                print(f"[SOCKET] Error procesando mensaje: {e}")
                client_socket.send(b"ERROR")
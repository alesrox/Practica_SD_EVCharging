import time
import uuid
import json
import socket
import random
import argparse
import threading
import tkinter as tk

from concurrent.futures import ThreadPoolExecutor
from confluent_kafka import Producer, Consumer, KafkaException, KafkaError

TOPIC = "central-request"

class Engine:
    def __init__(
        self, id: str, location: str = "Zone 0", price: float = 0.6,
        broker_host: str = "localhost", broker_port: int = 9092,
        port: int = 5002,
    ):
        self.id = id
        self.location = location
        self.price = price

        self.broker_host = broker_host
        self.broker_port = broker_port

        self.host = "0.0.0.0"
        self.port = port

        self._stop_event = threading.Event()

        self.ko_mode: bool = False
        self.can_supply: bool = False
        self.kwh: float = 0.0
        self.driver: str = None
        self.status: str = "ACTIVADO"

        self.consumer = Consumer({
            'bootstrap.servers': f"{broker_host}:{broker_port}",
            'group.id': f'engine-service-{self.id}',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True
        })

        self.producer = Producer({'bootstrap.servers': f"{broker_host}:{broker_port}"})

    def start(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((self.host, self.port))
        server.listen(5)
        server.settimeout(1.0)
        print(f"[SOCKET] Escuchando en {self.host}:{self.port}")

        try:
            while not self._stop_event.is_set():
                try:
                    client, addr = server.accept()
                except socket.timeout:
                    continue
                except Exception as e:
                    print(f"[SOCKET] accept error: {e}")
                    continue
                threading.Thread(target=self.handle_client, args=(client,), daemon=True).start()
        finally:
            try:
                server.close()
            except Exception:
                pass
            print("[SOCKET] Listener detenido")

    def handle_client(self, client_socket):
        with client_socket:
            try:
                data = client_socket.recv(4096)
                if not data:
                    return
                msg = json.loads(data.decode("utf-8"))
                if msg.get("type") == "check" and msg.get("id") == self.id:
                    if not self.ko_mode:
                        self.monitor_response(client_socket)
            except Exception as e:
                print("[SOCKET] Error procesando mensaje:", e)

    def monitor_response(self, client_socket):
        msg = {
            "type": "status",
            "id": self.id,
            "status": self.status,
            "location": self.location,
            "price": round(self.price, 2)
        }
        try:
            client_socket.send(json.dumps(msg).encode("utf-8"))
        except Exception as e:
            print("[SOCKET] Error enviando status:", e)

    def kafka_listener(self):
        self.consumer.subscribe([TOPIC])
        with ThreadPoolExecutor(max_workers=4) as executor:
            try:
                while not self._stop_event.is_set():
                    try:
                        msg = self.consumer.poll(1.0)
                    except Exception as e:
                        print(f"[KAFKA] Error en poll: {e}")
                        time.sleep(1)
                        continue

                    if msg is None:
                        continue
                    if msg.error():
                        if msg.error().code() != KafkaError._PARTITION_EOF:
                            print(f"[KAFKA] Error de mensaje: {msg.error()}")
                        continue
                    try:
                        data = json.loads(msg.value().decode("utf-8"))
                    except Exception as e:
                        print(f"[KAFKA] Mensaje no válido recibido: {e}")
                        continue

                    executor.submit(self._procesar_mensaje, data)
            except Exception as e:
                print(f"[KAFKA] Error en el consumidor Kafka: {e}")
            finally:
                try:
                    self.consumer.close()
                except Exception:
                    pass
                print("[KAFKA] Listener detenido")

    def _procesar_mensaje(self, data):
        if data.get("engine_id") == self.id and not self.ko_mode:
            t = data.get("type")
            if t == "engine_supply_response":
                status = data.get('status')
                print(f"[INFO] Respuesta recibida: {status}")
                if status == "approved": self.can_supply = True
            elif t == "supply_request":
                self.supply_request(data)
            elif t == "start_supply" and data.get("status") != "denegada":
                self.driver = data.get("driver_id")
                self.can_supply = True

    def supply_request(self, data):
        c_id = data.get("id")
        driver_id = data.get('driver_id')
        print(f"[INFO] Solicitud ({c_id}) de suministro para {driver_id}")

        accepted = not self.can_supply
        status = "aceptada" if accepted else "denegada"

        response = {
            "type": "supply_response",
            "engine_id": self.id,
            "driver_id": driver_id,
            "id": c_id,
            "status": status,
            "timestamp": time.time()
        }

        if accepted: self.can_supply = True
        print(f"[INFO] Solicitud ({c_id}): {status}")

        try:
            self.producer.produce(TOPIC, json.dumps(response).encode("utf-8"))
            self.producer.flush(timeout=5)
        except Exception as e:
            print(f"[KAFKA] Error enviando respuesta de supply: {e}")

    def suministrar(self, label: tk.Label):
        self.kwh = 0.0
        if not self.can_supply:
            print("[INFO] No se puede iniciar el suministro, no autorizado.")
            return

        if self.status == "SUMINISTRANDO":
            print("[INFO] Ya se está suministrando.")
            return

        self.status = "SUMINISTRANDO"
        print("[INFO] SUMINISTRANDO...")
        self.supply_msg("init_supply")

        try:
            while self.can_supply and not self._stop_event.is_set():
                while self.ko_mode and not self._stop_event.is_set():
                    time.sleep(0.5)

                increment = random.choice([x * 0.5 for x in range(16, 23)])
                self.kwh += increment

                _price = round(self.kwh * self.price, 2)
                # actualizar label de forma thread-safe
                if label:
                    label.after(0, lambda v=self.kwh, p=_price: label.config(text=f"Consumo: {v:.2f} kWh | {p:.2f}€"))

                msg = {
                    "type": "supply_info",
                    "engine_id": self.id,
                    "driver_id": self.driver,
                    "id": str(uuid.uuid4()),
                    "consumo": round(self.kwh, 2),
                    "timestamp": time.time()
                }

                print(f"[INFO] {self.kwh:.2f} kWh totales suministrados")

                try:
                    self.producer.produce(TOPIC, json.dumps(msg).encode("utf-8"))
                    self.producer.flush(timeout=5)
                except Exception as e:
                    print(f"[KAFKA] Error enviando supply_info: {e}")

                time.sleep(2)
        finally:
            try:
                self.supply_msg("end_supply")
            except Exception:
                pass
            print(f"[INFO] FINALIZADO (Total: {self.kwh:.2f} kWh)")
            self.status = "ACTIVADO"
            self.driver = None
            if label:
                label.after(0, lambda: label.config(text=f"Consumo: 0.00 kWh | 0.00€"))

    def supply_msg(self, msg_id: str = "init_supply"):
        msg = {
            "type": msg_id,
            "engine_id": self.id,
            "driver_id": self.driver,
            "consumo": round(self.kwh, 2),
            "timestamp": time.time()
        }
        try:
            self.producer.produce(TOPIC, json.dumps(msg).encode("utf-8"))
            self.producer.flush(timeout=5)
        except Exception as e:
            print(f"[KAFKA] Error en supply_msg: {e}")

    def solicitar_suministro(self):
        req_id = str(uuid.uuid4())
        msg = {
            "type": "engine_supply_request",
            "id": req_id,
            "engine_id": self.id,
            "timestamp": time.time(),
        }
        try:
            self.producer.produce(TOPIC, json.dumps(msg).encode("utf-8"))
            self.producer.flush(timeout=5)
            print(f"[INFO] Solicitud enviada al topic '{TOPIC}' (id={req_id})")
        except Exception as e:
            print(f"[KAFKA] Error enviando engine_supply_request: {e}")

    def stop(self):
        self._stop_event.set()
        try:
            self.consumer.close()
        except Exception:
            pass
        try:
            self.producer.flush(timeout=2)
        except Exception:
            pass
        print("[ENGINE] detenido")

def engine_ui(engine: Engine):
    def toggle_ko():
        engine.ko_mode = not engine.ko_mode
        ko_button.config(text=f"KO Mode: {'ON' if engine.ko_mode else 'OFF'}")

    def solicitar_suministro_ui():
        if engine.can_supply:
            return
        label_consumo.config(text="Consumo: 0.00 kWh | 0.00€")
        engine.solicitar_suministro()

    def conectar():
        if engine.can_supply:
            off_button.pack(pady=(5, 10))
            on_button.pack_forget()

        threading.Thread(target=engine.suministrar, args=(label_consumo,), daemon=True).start()

    def desconectar():
        on_button.pack(pady=(5, 10))
        off_button.pack_forget()
        engine.can_supply = False
        engine.driver = None

    def on_close():
        engine.stop()
        root.destroy()

    root = tk.Tk()
    root.title(f"Engine {engine.id}")

    label_consumo = tk.Label(root, text="Consumo: 0.00 kWh | 0.00€", font=("Arial", 14, "bold"))
    label_consumo.pack(pady=5)

    ko_button = tk.Button(root, text="KO Mode: OFF", width=20, command=toggle_ko)
    ko_button.pack(pady=(10, 5))

    supply_button = tk.Button(root, text="Solicitar Suministro", width=20, command=solicitar_suministro_ui)
    supply_button.pack(pady=5)

    on_button = tk.Button(root, text="Conectar", width=20, command=conectar)
    on_button.pack(pady=(5, 10))

    off_button = tk.Button(root, text="Desconectar", width=20, command=desconectar)

    root.protocol("WM_DELETE_WINDOW", on_close)
    root.mainloop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Engine de CP")
    parser.add_argument("id", help="ID del Charging Point")
    parser.add_argument("--broker-host", default="localhost", help="IP de la central")
    parser.add_argument("--broker-port", type=int, default=9092, help="Puerto de la central")
    parser.add_argument("--port", type=int, default=5002, help="Puerto de escucha del Engine")
    args = parser.parse_args()

    engine = Engine(
        id=args.id, port=args.port,
        broker_host=args.broker_host,
        broker_port=args.broker_port
    )

    threading.Thread(target=engine.start, args=(), daemon=True).start()
    threading.Thread(target=engine.kafka_listener, args=(), daemon=True).start()
    engine_ui(engine)
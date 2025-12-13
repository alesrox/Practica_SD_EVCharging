import time
import uuid
import json
import socket
import random
import argparse
import threading
import tkinter as tk

from cryptography.fernet import Fernet

from concurrent.futures import ThreadPoolExecutor
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException

class Engine:
    def __init__(
        self, id: str, location: str = "Zone 0", price: float = 0.6,
        broker: str = "localhost:9092", port: int = 6001,
    ):
        self.id = id
        self.location = location
        self.price = price

        self.broker = broker

        self.host = "0.0.0.0"
        self.port = port

        self.token = None

        self.ko_mode: bool = False
        self.can_supply: bool = False
        self.kwh: float = 0.0
        self.driver: str = None
        self.status: str = "ACTIVADO"

        self.ui_off_btn = None

        self._ticket_id = None
        self._last_ticket = None

        _topic = location.replace(" ", "").lower()
        self.consumer_topic = f"{_topic}-central-response"
        self.producer_topic = f"{_topic}-central-request"

        self.consumer = Consumer({
            'bootstrap.servers': self.broker,
            'group.id': f'engine-service-{self.id}',
            'auto.offset.reset': 'latest',
            'enable.auto.commit': True
        })
        self.consumer.subscribe([self.consumer_topic])

        print("[KAFKA] Iniciando consumidor...")
        for _ in range(500):
            self.consumer.poll(0.5)
            assignment = self.consumer.assignment()
            if assignment: 
                print(f"[KAFKA] Escuchando solicitudes...")
                break

        self.producer = Producer({'bootstrap.servers': self.broker})

    def start(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((self.host, self.port))
        server.listen(5)
        server.settimeout(1.0)
        print(f"[SOCKET] Escuchando en {self.host}:{self.port}")

        try:
            while True:
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
                elif msg.get("type") == "key" and msg.get("id") == self.id:
                    print("[INFO] Claves recibidas")
                    self.token = msg.get("key")
                    self.token = self.token
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
        with ThreadPoolExecutor(max_workers=4) as executor:
            try:
                while True:
                    msg = self.consumer.poll(1.0)
                    if msg is None:
                        continue
                    if msg.error():
                        if msg.error().code() != KafkaError._PARTITION_EOF:
                            raise KafkaException(msg.error())
                        continue
                    try:
                        executor.submit(self._procesar_mensaje, msg)
                    except Exception as e:
                        print(f"[KAFKA] Mensaje no válido recibido: {e}")
            except Exception as e:
                print(f"[KAFKA] Error en el consumidor Kafka: {e}")
            finally:
                self.consumer.close()

    def decode_message(self, msg):
        fernet = Fernet(self.token)
        decrypted_bytes = fernet.decrypt(msg.value())
        data = json.loads(decrypted_bytes.decode("utf-8"))
        return data

    def _procesar_mensaje(self, msg):
        if msg.key().decode("utf-8") == self.id and not self.ko_mode:
            data = self.decode_message(msg)

            t = data.get("type")
            if t == "engine_supply_response":
                status = data.get('status')
                print(f"[INFO] Respuesta recibida: {status}")
                if status == "approved": self.can_supply = True
            elif t == "supply_request":
                self.supply_request(data)
            elif t == "start_supply" and data.get("status") != "denegada":
                self.driver = data.get("driver")
                self.can_supply = True
                print(f"[INFO] Ya puede conectar el vehiculo de {self.driver}")
            elif t == "start_stop_services":
                if self.status == "PARADO":
                    self.status = "ACTIVADO"
                    print("[INFO] Restart services")
                else:
                    self.status = "PARADO"
                    self.can_supply = False
                    self.ui_off_btn()
                    print("[INFO] Stop services")
            elif t == "end_supply_registered":
                print("[INFO] Central recibió el ticket")
                self._loop_confirm_msg = False

    def send_kafka_msg(self, msg):
        fernet = Fernet(self.token)
        msg_json = json.dumps(msg).encode("utf-8")
        msg_encrypted = fernet.encrypt(msg_json)

        self.producer.produce(
            self.producer_topic, 
            key=self.id.encode("utf-8"),
            value=msg_encrypted,
        )
        self.producer.flush(timeout=5)

    def supply_request(self, data):
        c_id = data.get("id")
        driver = data.get('driver')
        print(f"[INFO] Solicitud de suministro para {driver} ({c_id})")

        _aux = self.can_supply and self.driver != driver
        accepted = not _aux
        status = "aceptada" if accepted else "denegada"

        response = {
            "id": c_id,
            "cp": self.id,
            "type": "supply_response",
            "driver": driver,
            "status": status,
            "zone": self.location,
            "timestamp": time.time()
        }

        if accepted: self.can_supply = True
        print(f"[INFO] Solicitud: {status} ({c_id})")
        self.send_kafka_msg(response)

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
        self._ticket_id = str(uuid.uuid4())
        self.supply_msg("init_supply")

        while self.can_supply and not self.ko_mode:
            increment = random.choice([x * 0.5 for x in range(16, 23)])
            self.kwh += increment
            _price = round(self.kwh * self.price, 2)

            # if label:
            #     label.after(0, lambda v=self.kwh, p=_price: label.config(text=f"Consumo: {v:.2f} kWh | {p:.2f}€"))
            label.config(text=f"Consumo: {self.kwh:.2f} kWh | {_price:.2f}€")

            msg = {
                "id": self._ticket_id,
                "type": "supply_info",
                "cp": self.id,
                "driver": self.driver,
                "zone": self.location,
                "consumo": self.kwh,
                "total": _price,
                "timestamp": time.time()
            }
            
            print(f"[INFO] Consumo: {self.kwh} kWh")
            self.send_kafka_msg(msg)
            self._last_ticket = (self._ticket_id, self.driver, self.kwh, _price)
            time.sleep(2)

        self.supply_msg("end_supply")
        if self.status != "PARADO": self.status = "ACTIVADO"
        print(f"[INFO] FINALIZADO (Total: {self.kwh:.2f} kWh)")
        label.config(text=f"Consumo: 0.00 kWh | 0.00€")
        self.end_supply()
        # if label:
        #     label.after(0, lambda: label.config(text=f"Consumo: 0.00 kWh | 0.00€"))

    def supply_msg(self, msg_id: str = "init_supply"):
        msg = {
            "id": self._ticket_id,
            "type": msg_id,
            "cp": self.id,
            "driver": self.driver,
            "zone": self.location,
            "consumo": round(self.kwh, 2),
            "timestamp": time.time()
        }

        self.send_kafka_msg(msg)

    def solicitar_suministro(self):
        req_id = str(uuid.uuid4())
        msg = {
            "id": req_id,
            "type": "cp_supply_request",
            "cp": self.id,
            "zone": self.location,
            "timestamp": time.time(),
        }
        self.send_kafka_msg(msg)
        print("[INFO] Solicitando suministraje por interfaz")

    def end_supply(self):
        self._loop_confirm_msg = True
        time.sleep(10)
        while self._loop_confirm_msg:
            print("[ERROR] No se puedo enviar el ticket a central... Reintentando...")

            msg = {
                "id": self._last_ticket[0],
                "type": "end_supply",
                "cp": self.id,
                "driver": self._last_ticket[1],
                "zone": self.location,
                "consumo": self._last_ticket[2],
                "total": self._last_ticket[3],
                "timestamp": time.time()
            }

            self.send_kafka_msg(msg)

            time.sleep(10)

        self._last_ticket = None

def engine_ui(engine: Engine):
    def toggle_ko():
        engine.ko_mode = not engine.ko_mode
        ko_button.config(text=f"KO Mode: {'ON' if engine.ko_mode else 'OFF'}")

    def solicitar_suministro_ui():
        if engine.can_supply: return
        # label_consumo.config(text="Consumo: 0.00 kWh | 0.00€")
        engine.driver = None
        engine.solicitar_suministro()

    def conectar():
        if engine.can_supply:
            off_button.pack(pady=(5, 10))
            on_button.pack_forget()

        threading.Thread(target=engine.suministrar, args=(label_consumo,), daemon=True).start()

    def desconectar():
        engine.can_supply = False
        quitar_btn_off()

    def quitar_btn_off():
        on_button.pack(pady=(5, 10))
        off_button.pack_forget()

    engine.ui_off_btn = quitar_btn_off

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

    root.mainloop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Engine de CP")
    parser.add_argument("id", help="ID del Charging Point")
    parser.add_argument("--location", default="Zone 0", help="Ubicación del CP")
    parser.add_argument("--price", type=float, default=0.6, help="Precio del kWh del CP")
    parser.add_argument("--broker", default="localhost:9092", help="IP de la central")
    parser.add_argument("--port", type=int, default=6001, help="Puerto de escucha del Engine")
    args = parser.parse_args()

    engine = Engine(
        id=args.id, 
        location=args.location,
        price=args.price,
        port=args.port,
        broker=args.broker,
    )

    threading.Thread(target=engine.start, args=(), daemon=True).start()
    threading.Thread(target=engine.kafka_listener, args=(), daemon=True).start()
    engine_ui(engine)

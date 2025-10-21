import time
import uuid
import json
import socket
import random
import argparse
import threading
import tkinter as tk

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

        self.host = "localhost"
        self.port = port

        self.ko_mode: bool = False
        self.can_supply: bool = False
        self.kwh: float = 0.0
        self.driver: str = None
        self.status: str = "ACTIVADO"

        self.consumer = Consumer({
            'bootstrap.servers': f"{broker_host}:{broker_port}",
            'group.id': f'engine-service-{self.id}',
            'auto.offset.reset': 'earliest'
        })

        self.consumer.subscribe([TOPIC])
        self.producer = Producer({'bootstrap.servers': f"{broker_host}:{broker_port}"})

    def start(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((self.host, self.port))
        server.listen(5)
        print(f"[SOCKET] Escuchando en {self.host}:{self.port}")

        while True:
            client, addr = server.accept()
            threading.Thread(target=self.handle_client, args=(client,), daemon=True).start()

    def handle_client(self, client_socket):
        with client_socket:
            try:
                data = client_socket.recv(4096)
                if not data: return
                msg = json.loads(data.decode("utf-8"))
                if msg.get("type") == "check" and msg.get("id") == self.id:
                    if not self.ko_mode: self.monitor_response(client_socket)
            except Exception as e:
                print("[SOCKET] Error procesando mensaje:", e)

    def monitor_response(self, client_socket):
        msg = {
            "type": "status",
            "id": self.id,
            "status": self.status,
            "location": self.location,
            "price": self.price
        }

        client_socket.send(json.dumps(msg).encode("utf-8"))

    def kafka_listener(self):
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None: continue
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF: 
                        raise KafkaException(msg.error())
                    continue
                try:
                    data = json.loads(msg.value().decode("utf-8"))
                    if data.get("engine_id") == self.id and not self.ko_mode:
                        if data.get("type") == "engine_supply_response":
                            status = data.get('status')
                            print(f"[INFO] Respuesta recibida: {status}")
                            if status == "approved": self.can_supply = True
                        elif data.get("type") == "supply_request":
                            self.supply_request(data)
                        elif data.get("type") == "start_supply":
                            self.driver = data.get("driver_id")
                            self.can_supply = True
                except Exception as e:
                    print(f"[KAFKA] Mensaje no válido recibido: {e}")
                    continue
        except Exception as e:
            print(f"[KAFKA] Error en el consumidor Kafka: {e}")
        finally:
            self.consumer.close()

    def supply_request(self, data):
        c_id = data.get("correlation_id")
        driver_id = data.get('driver_id')
        print(f"[INFO] Solicitud ({c_id}) de suministro para {driver_id}")
        msg = 'denegada' if self.can_supply else 'aceptada'

        response = {
            "type": "supply_response",
            "engine_id": data.get("engine_id"),
            "driver_id": driver_id,
            "correlation_id": c_id,
            "status": "OK" if not self.can_supply else "KO",
            "timestamp": time.time()
        } 

        self.can_supply = True
        print(f"[INFO] Solicitud ({c_id}): {msg}")

        self.producer.produce(TOPIC, json.dumps(response).encode("utf-8"))
        self.producer.flush()

    def suministrar(self):
        self.kwh = 0
        if not self.can_supply:
            print("[INFO] No se puede iniciar el suministro, no autorizado.")
            return

        if self.status == "SUMINISTRANDO":
            print("[INFO] Ya se esta suministrando.")
            return


        self.status = "SUMINISTRANDO"
        print("[INFO] SUMINISTRANDO...")
        self.supply_msg("init_supply")
        while self.can_supply:
            if self.ko_mode: return
            self.kwh += random.choice([x * 0.5 for x in range(16, 23)])

            msg = {
                "type": "supply_info",
                "engine_id": self.id,
                "driver_id": self.driver,
                "correlation_id": str(uuid.uuid4()),
                "consumo": self.kwh,
                "timestamp": time.time()
            }

            print(f"[INFO] {self.kwh} kWh totales suministrados")

            self.producer.produce(TOPIC, json.dumps(msg).encode("utf-8"))
            self.producer.flush()
            time.sleep(2)

        self.supply_msg("end_supply")
        print(f"[INFO] FINALIZADO (Total: {self.kwh} kWh)")

        self.status = "ACTIVADO"
        self.driver = None

    def supply_msg(self, msg_id: str = "init_supply"):
        msg = {
            "type": msg_id,
            "engine_id": self.id,
            "driver_id": self.driver,
            "consumo": self.kwh,
            "timestamp": time.time()
        } 

        self.producer.produce(TOPIC, json.dumps(msg).encode("utf-8"))
        self.producer.flush()

    def solicitar_suministro(self):
        if self.can_supply: return

        correlation_id = str(uuid.uuid4())

        msg = {
            "type": "engine_supply_request",
            "engine_id": self.id,
            "from": "ev_engine",
            "timestamp": time.time(),
            "correlation_id": correlation_id
        }
        
        self.producer.produce(TOPIC, json.dumps(msg).encode("utf-8"))
        self.producer.flush()
        print(f"[INFO] Solicitud enviada al topic '{TOPIC}'")

def engine_ui(engine: Engine):
    def toggle_ko():
        engine.ko_mode = not engine.ko_mode
        ko_button.config(text=f"KO Mode: {'ON' if engine.ko_mode else 'OFF'}")

    def solicitar_suministro_ui():
        engine.solicitar_suministro()
    
    def conectar():
        threading.Thread(target=engine.suministrar, args=(), daemon=True).start()

    def desconectar():
        engine.can_supply = False

    root = tk.Tk()
    root.title(f"Engine {engine.id}")

    ko_button = tk.Button(root, text="KO Mode: OFF", width=20, command=toggle_ko)
    ko_button.pack(pady=10)

    supply_button = tk.Button(root, text="Solicitar Suministro", width=20, command=solicitar_suministro_ui)
    supply_button.pack(pady=10)

    on_button = tk.Button(root, text="Conectar", width=20, command=conectar)
    on_button.pack(pady=10)

    off_button = tk.Button(root, text="Desconectar", width=20, command=desconectar)
    off_button.pack(pady=10)

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
    
    #engine.start()
    threading.Thread(target=engine.start, args=(), daemon=True).start()
    threading.Thread(target=engine.kafka_listener, args=(), daemon=True).start()
    engine_ui(engine)
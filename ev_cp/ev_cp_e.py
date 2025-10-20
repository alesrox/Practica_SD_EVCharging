import time
import json
import socket
import argparse
import threading

import tkinter as tk
from tkinter import messagebox

from confluent_kafka import Producer, Consumer, KafkaException, KafkaError

TOPIC = "central-request"
_INPUT_TOPIC = "engine-response"
_OUTPUT_TOPIC = "central-request"

class Engine:
    def __init__(self, id, broker_host="localhost", broker_port=9092, port=5002):
        self.id = id

        self.broker_host = broker_host
        self.broker_port = broker_port

        self.host = "0.0.0.0"
        self.port = port

        self.ko_mode = False
        self.status = "ACTIVO"

        self.consumer = Consumer({
            'bootstrap.servers': f"{broker_host}:{broker_port}",
            'group.id': f'engine-service-{self.id}',
            'auto.offset.reset': 'earliest'
        })
        self.consumer.subscribe([TOPIC])
        self.producer = Producer({'bootstrap.servers': f"{broker_host}:{broker_port}"})

    def start(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind((self.host, self.port))
        server.listen(5)
        print(f"Engine escuchando en {self.host}:{self.port}")

        while True:
            client, addr = server.accept()
            threading.Thread(target=self.handle_client, args=(client,), daemon=True).start()

    def handle_client(self, client_socket):
        with client_socket:
            try:
                data = client_socket.recv(4096)
                if not data: return
                mensaje = json.loads(data.decode("utf-8"))
                if mensaje.get("type") == "check" and not self.ko_mode:
                    client_socket.send(b"OK")
            except Exception as e:
                print("Error procesando mensaje:", e)

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
                    if data.get("engine_id") == self.id:
                        if data.get("type") == "request_start_supply_response":
                            msg = {
                                "type": "request_start_supply",
                                "engine_id": data.get("engine_id"),
                                "driver_id": data.get("driver_id"),
                                "correlation_id": data.get("correlation_id"),
                                "timestamp": time.time()
                            }

                            self.producer.produce(TOPIC, json.dumps(msg).encode("utf-8"))
                        elif data.get("type") == "supply_request_response":
                            print(f"💬 Respuesta recibida: {data.get('status')}")
                except Exception as e:
                    print(f"⚠️  Mensaje no válido recibido: {e}")
                    continue
        except Exception as e:
            print(f"❌ Error en el consumidor Kafka: {e}")
        finally:
            self.consumer.close()
    
    def solicitar_suministro(self):
        correlation_id = str(self.id)

        mensaje = {
            "type": "supply_request_engine",
            "engine_id": self.id,
            "from": "ev_engine",
            "timestamp": time.time(),
            "correlation_id": correlation_id
        }
        
        self.producer.produce(TOPIC, json.dumps(mensaje).encode("utf-8"))
        self.producer.flush()
        print(f"📤 Solicitud enviada al topic '{TOPIC}'")


def start_ui(engine: Engine):
    def toggle_ko():
        engine.ko_mode = not engine.ko_mode
        ko_button.config(text=f"KO Mode: {'ON' if engine.ko_mode else 'OFF'}")

    def solicitar_suministro_ui():
        engine.solicitar_suministro()

    root = tk.Tk()
    root.title(f"Engine {engine.id}")

    ko_button = tk.Button(root, text="KO Mode: OFF", width=20, command=toggle_ko)
    ko_button.pack(pady=10)

    supply_button = tk.Button(root, text="Solicitar Suministro", width=20, command=solicitar_suministro_ui)
    supply_button.pack(pady=10)

    root.mainloop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Engine de CP")

    parser.add_argument("id", help="ID del Charging Point")
    parser.add_argument("--broker-host", default="localhost", help="IP de la central")
    parser.add_argument("--broker-port", type=int, default=9092, help="Puerto de la central")
    parser.add_argument("--port", type=int, default=5002, help="Puerto de escucha del Engine")
    # parser.add_argument("--host", default="0.0.0.0", help="IP de EV_CP_M")
    args = parser.parse_args()

    engine = Engine(
        id=args.id, port=args.port,
        broker_host=args.broker_host, 
        broker_port=args.broker_port
    )
    
    #engine.start()
    threading.Thread(target=engine.start, args=(), daemon=True).start()
    threading.Thread(target=engine.kafka_listener, args=(), daemon=True).start()
    start_ui(engine)
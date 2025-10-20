import time
import json
import socket
import argparse
import threading

import tkinter as tk
from tkinter import messagebox

from confluent_kafka import Producer, Consumer, KafkaException, KafkaError

class Engine:
    def __init__(self, id, broker_host="localhost", broker_port=9092, port=5002):
        self.id = id

        self.broker_host = broker_host
        self.broker_port = broker_port

        self.host = "0.0.0.0"
        self.port = port

        self.ko_mode = False

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

    def solicitar_suministro(self):
        request_topic = "requests"
        response_topic = "responses"
        correlation_id = str(self.id)

        print(f"🛰️  Enviando solicitud de suministro con ID {correlation_id}")
        self._enviar_solicitud_kafka(request_topic, correlation_id)

        print(f"⌛ Esperando respuesta en topic '{response_topic}'...")
        status = self._esperar_respuesta_kafka(response_topic, correlation_id)

        if status:
            print(f"💬 Respuesta recibida: {status}")
        else:
            print("⏰ No se recibió respuesta del servidor en el tiempo esperado.")

        return status

    def _enviar_solicitud_kafka(self, topic, correlation_id):
        producer_conf = {
            'bootstrap.servers': f"{self.broker_host}:{self.broker_port}"
        }
        producer = Producer(producer_conf)

        mensaje = {
            "type": "supply_request",
            "engine_id": f"engine-{self.port}",
            "timestamp": time.time(),
            "correlation_id": correlation_id
        }

        try:
            producer.produce(topic, json.dumps(mensaje).encode("utf-8"))
            producer.flush()
            print(f"📤 Solicitud enviada al topic '{topic}'")
        except Exception as e:
            print(f"❌ Error enviando mensaje Kafka: {e}")

    def _esperar_respuesta_kafka(self, topic, correlation_id, timeout=10):
        consumer_conf = {
            'bootstrap.servers': f"{self.broker_host}:{self.broker_port}",
            'group.id': f'engine-{self.id}',
            'auto.offset.reset': 'earliest'
        }
        consumer = Consumer(consumer_conf)
        consumer.subscribe([topic])

        start_time = time.time()
        status = None

        try:
            while time.time() - start_time < timeout:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        raise KafkaException(msg.error())
                    continue

                try:
                    data = json.loads(msg.value().decode("utf-8"))
                    if data.get("correlation_id") == correlation_id:
                        status = data.get("status", "unknown")
                        break
                except json.JSONDecodeError:
                    continue
        except Exception as e:
            print(f"❌ Error recibiendo respuesta Kafka: {e}")
        finally:
            consumer.close()

        return status
    
def start_ui(engine: Engine):
    def toggle_ko():
        engine.ko_mode = not engine.ko_mode
        ko_button.config(text=f"KO Mode: {'ON' if engine.ko_mode else 'OFF'}")

    def solicitar_suministro_ui():
        status = engine.solicitar_suministro()
        messagebox.showinfo("Suministro", f"Respuesta del servidor: {status}")

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
    start_ui(engine)
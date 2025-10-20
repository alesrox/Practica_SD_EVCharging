import time
import uuid
import json
import socket
import argparse
import threading
import tkinter as tk

from confluent_kafka import Producer, Consumer, KafkaException, KafkaError

TOPIC = "central-request"

class Engine:
    def __init__(self, id, broker_host="localhost", broker_port=9092, port=5002):
        self.id = id

        self.broker_host = broker_host
        self.broker_port = broker_port

        self.host = "0.0.0.0"
        self.port = port

        self.ko_mode = False
        self.can_supply = False
        self.driver = None
        self.status = "ACTIVADO"

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
                    client_socket.send(self.status.encode())
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
                        if data.get("type") == "engine_supply_response":
                                print(f"💬 Respuesta recibida: {data.get('status')}")
                                self.can_supply = True
                        elif data.get("type") == "supply_request":
                            print(f"🔌 Solicitud de inicio de suministro recibida para {data.get('driver_id')}")
                            response = {
                                "type": "supply_response",
                                "engine_id": data.get("engine_id"),
                                "driver_id": data.get("driver_id"),
                                "correlation_id": data.get("correlation_id"),
                                "status": "OK" if not self.can_supply else "KO",
                                "timestamp": time.time()
                            } 

                            self.can_supply = True
                            self.producer.produce(TOPIC, json.dumps(response).encode("utf-8"))
                            self.producer.flush()
                        elif data.get("type") == "start_supply":
                            self.driver = data.get("driver_id")
                            self.can_supply = True
                except Exception as e:
                    print(f"⚠️  Mensaje no válido recibido: {e}")
                    continue
        except Exception as e:
            print(f"❌ Error en el consumidor Kafka: {e}")
        finally:
            self.consumer.close()

    def iniciar_suministro(self):
        if not self.can_supply:
            print("No se puede iniciar el suministro, no autorizado.")
            return

        if self.status == "SUMINISTRANDO":
            print("Ya se esta suministrando")
            return

        if not self.driver: pass

        self.status = "SUMINISTRANDO"
        print("SUMINISTRANDO...")
        self.driver_msg("init_supply")
        while self.can_supply:
            time.sleep(0.5)

        self.driver_msg("end_supply")
        self.status = "ACTIVADO"
        self.driver = None
        print("FINALIZADO")

    def driver_msg(self, msg_id: str = "init_supply"):
        if not self.driver: return
        msg = {
            "type": msg_id,
            "engine_id": self.id,
            "driver_id": self.driver,
            "timestamp": time.time()
        } 
        self.producer.produce(TOPIC, json.dumps(msg).encode("utf-8"))
        self.producer.flush()


    def solicitar_suministro(self):
        if self.can_supply: return

        correlation_id = str(uuid.uuid4())

        mensaje = {
            "type": "engine_supply_request",
            "engine_id": self.id,
            "from": "ev_engine",
            "timestamp": time.time(),
            "correlation_id": correlation_id
        }
        
        self.producer.produce(TOPIC, json.dumps(mensaje).encode("utf-8"))
        self.producer.flush()
        print(f"📤 Solicitud enviada al topic '{TOPIC}'")


def engine_ui(engine: Engine):
    def toggle_ko():
        engine.ko_mode = not engine.ko_mode
        ko_button.config(text=f"KO Mode: {'ON' if engine.ko_mode else 'OFF'}")

    def solicitar_suministro_ui():
        engine.solicitar_suministro()
    
    def conectar():
        threading.Thread(target=engine.iniciar_suministro, args=(), daemon=True).start()

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
    # parser.add_argument("--host", default="0.0.0.0", help="IP de EV_CP_M")
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
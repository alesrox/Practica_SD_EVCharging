import uuid
import time
import json
import argparse
import threading
from concurrent.futures import ThreadPoolExecutor
from confluent_kafka import Producer, Consumer, KafkaException, KafkaError

TOPIC = "central-request"

class Driver:
    def __init__(self, id: str, broker="localhost:9092", filename=None):
        self.id = id
        self.broker = broker
        self.filename = filename

        self.exit = False
        self.waiting = False
        self.unresponsed = {
            "driver_cp_info": [],
            "driver_supply_request": []
        }

        self.consumer = Consumer({
            'bootstrap.servers': self.broker,
            'group.id': f'driver-service-{self.id}',
            'auto.offset.reset': 'latest',
            'enable.auto.commit': True
        })
        self.consumer.subscribe([TOPIC])

        self.producer = Producer({'bootstrap.servers': self.broker})

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
                        data = json.loads(msg.value().decode("utf-8"))
                        executor.submit(self._procesar_driver_msg, data)
                    except Exception as e:
                        print(f"[KAFKA] Mensaje no válido recibido: {e}")
            except Exception as e:
                print(f"[KAFKA] Error en el consumidor Kafka: {e}")
            finally:
                self.consumer.close()
    
    def _procesar_driver_msg(self, data):
        if data.get("driver_id") != self.id: return
        t = data.get("type")

        check_id = data.get("id") in self.unresponsed["driver_cp_info"]
        if t == "driver_cp_info_resposne" and check_id:
                self.unresponsed["driver_cp_info"].clear()
                self.waiting = True
                self.show_cp(data)

        if not self.waiting: return

        if t == "supply_request" and data.get("id") in self.unresponsed["driver_supply_request"]:
            self.unresponsed["driver_supply_request"].clear()
            if data.get("status") == "aceptada":
                print(f"[INFO] Surtidor {data.get('engine_id')} disponible para suministro.")
            else:
                print(f"[INFO] Suministro con {data.get('engine_id')}: solicitud denegada")
        elif t == "start_supply":
            engine_id = data.get("engine_id")
            if data.get("status") == "aceptada":
                print(f"[INFO] Suministro con {engine_id} iniciado: ya puede enchufar el vehículo.")
            else:
                print(f"[INFO] Suministro con {engine_id}: solicitud denegada")
                self.ask_for_cp()
        elif t == "init_supply":
            cp_id = data.get("engine_id")
            print(f"[INFO] {cp_id} ha empezado a suministrar")
        elif t == "supply_info":
            kwh = float(data.get("consumo"))
            print(f"[INFO] Consumo: {kwh}")
        elif t == "end_supply":
            print("[INFO] Fin de suministro.")
            self.ask_for_cp()
    
    def start(self):
        threading.Thread(target=self.kafka_listener, daemon=True).start()
        self.ask_for_cp()
        time.sleep(10)
        while not self.exit: # Bucle principal
            if self.unresponsed["driver_cp_info"]:
                print("[ERROR] Tiempo de espera agotado (1)")
                self.ask_for_cp()
            time.sleep(10)

    def solicitar_carga(self, cp_id: str):
        print(f"[INFO] Solicitando carga con {cp_id}")
        req_id = str(uuid.uuid4())
        self.unresponsed["driver_supply_request"].append(req_id)
        mensaje = {
            "id": req_id,
            "type": "driver_supply_request",
            "driver_id": self.id,
            "engine_id": cp_id,
            "timestamp": time.time(),
        }

        self.producer.produce(TOPIC, json.dumps(mensaje).encode("utf-8"))
        self.producer.flush(timeout=5)

    def ask_for_cp(self):
        self.waiting = False
        print("[INFO] Solicitando a Central CP disponibles")
        req_id = str(uuid.uuid4())
        self.unresponsed["driver_cp_info"].append(req_id)
        msg = {
            "type": "driver_cp_info",
            "driver_id": self.id,
            "id": req_id,
            "timestamp": time.time(),
        }

        self.producer.produce(TOPIC, json.dumps(msg).encode("utf-8"))
        self.producer.flush(timeout=5)

    def show_cp(self, data):
        info = data.get("info", [])
        print("\nPuntos de carga disponibles:")
        for cp_id in info:
            print(f"  - CP: {cp_id}")
        
        self.get_cp()

    def get_cp(self):
        cmd = input("\nIntroduce CP (id), 'r' reintentar, 'q' salir: ").strip()
        if cmd == "q":
            self.exit = True
        elif cmd == "r":
            self.ask_for_cp()
        elif cmd:
            self.solicitar_carga(cmd)
            time.sleep(5)
            while self.unresponsed["driver_supply_request"] != []:
                print("[ERROR] Tiempo de espera agotado (2)")
                self.solicitar_carga(cmd)
                time.sleep(5)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="EV_DRIVER")
    parser.add_argument("id", help="ID del Driver")
    parser.add_argument("--broker", default="localhost:9092", help="IP del broker")
    parser.add_argument("--file", default=None, help="Fichero de operaciones")
    args = parser.parse_args()

    driver = Driver(
        id=args.id,
        broker=args.broker,
        filename=args.file
    )

    driver.start()
import uuid
import time
import json
import argparse
import threading
from confluent_kafka import Producer, Consumer, KafkaException, KafkaError

TOPIC = "central-request"

class Driver:
    def __init__(self, id: str, broker_host="localhost", broker_port=9092, filename=None):
        self.id = id
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.filename = filename
        
        self.still_asking = True

        self.consumer = Consumer({
            'bootstrap.servers': f"{broker_host}:{broker_port}",
            'group.id': f'driver-service-{self.id}',
            'auto.offset.reset': 'earliest'
        })

        self.consumer.subscribe([TOPIC])
        self.producer = Producer({'bootstrap.servers': f"{broker_host}:{broker_port}"})

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
                    if data.get("driver_id") == self.id:
                        if data.get("type") == "supply_request":
                            print(f"[INFO] Surtidor {data.get('engine_id')} disponible para suministro.")
                        elif data.get("type") == "start_supply":
                            engine_id = data.get("engine_id")
                            if data.get("status") == "OK":
                                print(f"[INFO] Suministro con {engine_id} iniciado ya puede enchufar el vehículo.")
                            else:
                                print(f"[INFO] Suministro con {engine_id}: solicitud denegada")
                                self.ask_for_cp()
                        elif data.get("type") == "init_supply":
                            print("[INFO] Suministrando...")
                        elif data.get("type") == "end_supply":
                            print("[INFO] Fin...")
                            self.ask_for_cp()
                        elif data.get("type") == "driver_cp_info_resposne":
                            self.show_cp(data)
                except Exception as e:
                    print(f"[KAFKA-CONSUMER] Mensaje no válido recibido: {e}")
                    continue
        except Exception as e:
            print(f"[KAFKA-CONSUMER] Error en el consumidor Kafka: {e}")
        finally:
            self.consumer.close()

    def solicitar_carga(self, cp_id: str):
        mensaje = {
            "type": "driver_supply_request",
            "driver_id": self.id,
            "engine_id": cp_id,
            "timestamp": time.time(),
            "id": str(uuid.uuid4())
        }
        
        self.producer.produce(TOPIC, json.dumps(mensaje).encode("utf-8"))
        self.producer.flush()
        print(f"[KAFKA-PRODUCER] Solicitud de carga enviada al topic '{TOPIC}'")

    def ask_for_cp(self):
        # self.still_asking = True
        print("[INFO] Solicitando CP a Central")
        # while self.still_asking:
        msg = {
            "type": "driver_cp_info",
            "driver_id": self.id,
            "id": str(uuid.uuid4()),
            "timestamp": time.time(),
        }

        self.producer.produce(TOPIC, json.dumps(msg).encode("utf-8"))
        self.producer.flush()
            # if self.still_asking: 
            #     print("[INFO] Tiempo de espera agotado volviendo a preguntar")
    
    def show_cp(self, data):
        if self.still_asking:
            self.still_asking = False
            info = data.get("info", [])
            print(f"\nPuntos de carga disponibles:")
            for cp_id in info:
                print(f"  - CP: {cp_id}")
            
            print()
            self.main()
    
    def main(self):
        cp_id = input("Introduce CP: ")
        if cp_id == "q": 
            exit(1)
        elif cp_id == "r":
            self.ask_for_cp()
        else:
            self.solicitar_carga(cp_id)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="EV_DRIVER")

    parser.add_argument("id", help="ID del Driver")
    parser.add_argument("--broker-host", default="localhost", help="IP de la central")
    parser.add_argument("--broker-port", type=int, default=9092, help="Puerto de la central")
    parser.add_argument("--file", default=None, help="Fichero de operaciones")
    args = parser.parse_args()

    driver = Driver(
        id=args.id,
        broker_host=args.broker_host, 
        broker_port=args.broker_port,
        filename=args.file
    )

    threading.Thread(target=driver.ask_for_cp, daemon=True).start()
    driver.kafka_listener()

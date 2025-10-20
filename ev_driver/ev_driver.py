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
                        elif data.get("type") == "init_supply":
                            print("[INFO] Suministrando...")
                        elif data.get("type") == "end_supply":
                            print("[INFO] Fin...")
                            exit(1)
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
            "correlation_id": str(uuid.uuid4())
        }
        
        self.producer.produce(TOPIC, json.dumps(mensaje).encode("utf-8"))
        self.producer.flush()
        print(f"[KAFKA-PRODUCER] Solicitud de carga enviada al topic '{TOPIC}'")


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

    threading.Thread(target=driver.solicitar_carga, args=('MAD1',), daemon=True).start()
    driver.kafka_listener()

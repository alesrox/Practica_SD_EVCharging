import json

from concurrent.futures import ThreadPoolExecutor
from confluent_kafka import Producer, Consumer, KafkaException, KafkaError

class Kafka_Handler:
    def __init__(self, gestor, broker="localhost:9092", topic="central-request"):
        self.gestor = gestor
        self.broker = broker
        self.topic = topic

        self.consumer = self._crear_consumidor()
        self.producer = self._crear_productor()

    def _crear_consumidor(self):
        conf = {
            'bootstrap.servers': self.broker,
            'group.id': 'central-service',
            'auto.offset.reset': 'latest',
            'enable.auto.commit': True
        }
        consumer = Consumer(conf)
        consumer.subscribe([self.topic])
        print(f"[KAFKA] Escuchando solicitudes en topic '{self.topic}'...")
        return consumer

    def _crear_productor(self):
        conf = {'bootstrap.servers': self.broker}
        producer = Producer(conf)
        return producer

    def start_listener(self):
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
                        executor.submit(self.procesar_msg, data)
                    except Exception as e:
                        print(f"[KAFKA] Mensaje no válido recibido: {e}")
            except Exception as e:
                print(f"[KAFKA] Error en el consumidor Kafka: {e}")
            finally:
                self.consumer.close()

    def procesar_msg(self, data):
        try:
            msg_type = data.get("type")

            if msg_type == "cp_supply_request":
                self.gestor.procesar_solicitud_cp(data)
            elif msg_type == "driver_supply_request":
                self.gestor.procesar_solicitud_driver(data)
            elif msg_type == "supply_response":
                self.gestor.supply_response(data)
            elif msg_type == "init_supply":
                cp_id = data.get("cp")
                driver = data.get("driver")
                driver_msg = f"a {driver}" if driver else "desde interfaz"
                print(f"[INFO] {cp_id} ha comenzado a suministrar {driver_msg}")
            elif msg_type == "supply_info":
                self.gestor.suministrando(data)
            elif msg_type == "end_supply":
                self.gestor.finalizar_suministro(data)
            elif msg_type == "driver_cp_info":
                self.gestor.share_cp(data)
        except Exception as e:
            print(e)

    def send_msg(self, msg):
        self.producer.produce(self.topic, json.dumps(msg).encode("utf-8"))
        self.producer.flush(timeout=5)
        # print(f"[KAFKA] Message sent ({msg['id']})")
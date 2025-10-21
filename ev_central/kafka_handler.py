import json
import time
from confluent_kafka import Producer, Consumer, KafkaException, KafkaError

class Kafka_Handler:
    def __init__(self, gestor, broker_host="localhost", broker_port=9092, topic="central-request"):
        self.gestor = gestor
        self.broker = f"{broker_host}:{broker_port}"
        self.topic = topic

        self.consumer = self._crear_consumidor()
        self.producer = self._crear_productor()

    def _crear_consumidor(self):
        conf = {
            'bootstrap.servers': self.broker,
            'group.id': 'central-service',
            'auto.offset.reset': 'earliest'
        }
        consumer = Consumer(conf)
        consumer.subscribe([self.topic])
        print(f"[KAFKA-CONSUMER] Escuchando solicitudes en topic '{self.topic}'...")
        return consumer

    def _crear_productor(self):
        conf = {'bootstrap.servers': self.broker}
        producer = Producer(conf)
        return producer

    def start_listener(self):
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
                    msg_type = data.get("type")
                    if msg_type == "driver_supply_request":
                        self._procesar_solicitud_driver(data)
                    elif msg_type == "engine_supply_request":
                        self._procesar_solicitud_engine(data)
                    elif msg_type == "supply_response":
                        self._response_driver(data, status=data.get("status", "KO"))
                    elif msg_type == "init_supply":
                        cp_id = data.get("engine_id")
                        driver_id = data.get("driver_id")
                        driver_msg = f"a {driver_id}" if driver_id else ""
                        print(f"[INFO] {cp_id} ha comenzado a suministrar {driver_msg}")
                    elif msg_type == "supply_info":
                        self.gestor.suministrando(data)
                    elif msg_type == "end_supply":
                        pass # TODO
                except Exception as e:
                    print(f"[KAFKA-CONSUMER] Mensaje no válido recibido: {e}")
        except Exception as e:
            print(f"[KAFKA-CONSUMER] Error en el consumidor Kafka: {e}")
        finally:
            self.consumer.close()

    def _procesar_solicitud_engine(self, data):
        try:
            engine_id = data.get("engine_id")
            correlation_id = data.get("correlation_id")

            print(f"[ENGINE] Solicitud recibida de {engine_id} (ID {correlation_id})")

            status = "approved" if self.gestor.can_supply(engine_id) else "denied"

            response = {
                "type": "engine_supply_response",
                "engine_id": engine_id,
                "correlation_id": correlation_id,
                "status": status,
                "timestamp": time.time()
            }

            self.producer.produce(self.topic, json.dumps(response).encode("utf-8"))
            self.producer.flush()

            print(f"[CENTRAL] Respuesta enviada a '{self.topic}': {status}")
        except Exception as e:
            print(f"[CENTRAL] Error procesando solicitud: {e}")

    def _procesar_solicitud_driver(self, data):
        driver_id = data.get("driver_id")
        engine_id = data.get("engine_id")
        correlation_id = data.get("correlation_id")

        print(f"[DRIVER] Solicitud de {driver_id} para usar en {engine_id} (ID {correlation_id})")
        if self.gestor.can_supply(engine_id):
            print(f"[CENTRAL] El CP {engine_id} está Operativo. Comprobando disponibilidad...")
            msg = {
                "type": "supply_request",
                "engine_id": engine_id,
                "driver_id": driver_id,
                "correlation_id": correlation_id,
                "timestamp": time.time()
            }

            self.producer.produce(self.topic, json.dumps(msg).encode("utf-8"))
            self.producer.flush()
        else:
            self._response_driver(data, status="KO")
            print(f"[CENTRAL] Suministro denegado para {driver_id} en {engine_id}")

    def _response_driver(self, data, status="KO"):
        engine_id = data.get("engine_id")
        driver_id = data.get("driver_id")
        correlation_id = data.get("correlation_id")

        msg_text = "denegada" if status == "KO" else "aceptada"
        print(f"[ENGINE] Solicitud de {engine_id} por {driver_id}: {msg_text}")

        response = {
            "type": "start_supply",
            "status": status,
            "driver_id": driver_id,
            "engine_id": engine_id,
            "correlation_id": correlation_id,
            "timestamp": time.time()
        }

        self.producer.produce(self.topic, json.dumps(response).encode("utf-8"))
        self.producer.flush()
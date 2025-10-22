import json
import time
from charging_point import EstadoCP
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
        print(f"[KAFKA] Escuchando solicitudes en topic '{self.topic}'...")
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
                    self.procesar_msg(data)
                except Exception as e:
                    print(f"[KAFKA] Mensaje no válido recibido: {e}")
        except Exception as e:
            print(f"[KAFKA] Error en el consumidor Kafka: {e}")
        finally:
            self.consumer.close()

    def procesar_msg(self, data):
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
            self.gestor.finalizar_suministro(data)
        elif msg_type == "driver_cp_info":
            self._share_cp(data)

    def _procesar_solicitud_engine(self, data):
        try:
            engine_id = data.get("engine_id")
            id = data.get("id")

            print(f"[INFO] Solicitud recibida de {engine_id} (ID {id})")

            status = "approved" if self.gestor.can_supply(engine_id) else "denied"

            response = {
                "type": "engine_supply_response",
                "engine_id": engine_id,
                "id": id,
                "status": status,
                "timestamp": time.time()
            }

            self.producer.produce(self.topic, json.dumps(response).encode("utf-8"))
            self.producer.flush()

            print(f"[INFO] Respuesta enviada a '{self.topic}': {status}")
        except Exception as e:
            print(f"[INFO] Error procesando solicitud: {e}")

    def _procesar_solicitud_driver(self, data):
        driver_id = data.get("driver_id")
        engine_id = data.get("engine_id")
        id = data.get("id")

        print(f"[INFO] Solicitud de {driver_id} para usar en {engine_id} (ID {id})")
        if self.gestor.can_supply(engine_id):
            print(f"[INFO] El CP {engine_id} está Operativo. Comprobando disponibilidad...")
            msg = {
                "type": "supply_request",
                "engine_id": engine_id,
                "driver_id": driver_id,
                "id": id,
                "timestamp": time.time()
            }

            self.producer.produce(self.topic, json.dumps(msg).encode("utf-8"))
            self.producer.flush()
        else:
            self._response_driver(data, status="KO")
            print(f"[INFO] Suministro denegado para {driver_id} en {engine_id}")

    def _response_driver(self, data, status="KO"):
        engine_id = data.get("engine_id")
        driver_id = data.get("driver_id")
        id = data.get("id")

        msg_text = "denegada" if status == "KO" else "aceptada"
        print(f"[INFO] Solicitud de {engine_id} por {driver_id}: {msg_text}")

        response = {
            "type": "start_supply",
            "status": status,
            "driver_id": driver_id,
            "engine_id": engine_id,
            "id": id,
            "timestamp": time.time()
        }

        self.producer.produce(self.topic, json.dumps(response).encode("utf-8"))
        self.producer.flush()

    def _share_cp(self, data):
        print("[INFO] 1")
        for_share_cp = [
            cp_id for cp_id, punto in self.gestor.charging_points.items()
            if punto.estado == EstadoCP.ACTIVADO
        ]

        response = {
            "type": "driver_cp_info_resposne",
            "driver_id": data.get("driver_id"),
            "id": data.get("id"),
            "info": for_share_cp,
            "timestamp": time.time(),
        }

        self.producer.produce(self.topic, json.dumps(response).encode("utf-8"))
        self.producer.flush()
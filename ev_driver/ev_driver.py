import uuid
import time
import json
import argparse
import threading
from concurrent.futures import ThreadPoolExecutor
from confluent_kafka import Producer, Consumer, KafkaException, KafkaError

TOPIC = "central-request"

class Driver:
    def __init__(self, id: str, broker_host="localhost", broker_port=9092, filename=None):
        self.id = id
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.filename = filename

        self._stop = threading.Event()
        self._cp_event = None
        self._current_cp_request_id = None
        self._last_cp_data = None
        self._last_cp_lock = threading.Lock()

        self.consumer = Consumer({
            'bootstrap.servers': f"{broker_host}:{broker_port}",
            'group.id': f'driver-service-{self.id}',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True
        })

        self.producer = Producer({'bootstrap.servers': f"{broker_host}:{broker_port}"})

    def start(self):
        t = threading.Thread(target=self.kafka_listener, daemon=True)
        t.start()
        
        # al iniciar, pedir CP una vez y entrar en la interfaz principal
        self.ask_for_cp()
        self.main()

    def kafka_listener(self):
        with ThreadPoolExecutor(max_workers=4) as executor:
            try:
                self.consumer.subscribe([TOPIC])
                while not self._stop.is_set():
                    try:
                        msg = self.consumer.poll(1.0)
                    except Exception as e:
                        print(f"[KAFKA] Error en poll: {e}")
                        time.sleep(1)
                        continue

                    if msg is None:
                        continue
                    if msg.error():
                        if msg.error().code() != KafkaError._PARTITION_EOF:
                            print(f"[KAFKA] Error de mensaje: {msg.error()}")
                        continue

                    try:
                        data = json.loads(msg.value().decode("utf-8"))
                    except Exception as e:
                        print(f"[KAFKA] Mensaje no válido recibido: {e}")
                        continue

                    executor.submit(self._procesar_driver_msg, data)
            except Exception as e:
                print(f"[KAFKA] Error en el consumidor Kafka: {e}")
            finally:
                try:
                    self.consumer.close()
                except Exception:
                    pass

    def _procesar_driver_msg(self, data):
        if data.get("driver_id") != self.id:
            return
        t = data.get("type")
        if t == "supply_request":
            print(f"[INFO] Surtidor {data.get('engine_id')} disponible para suministro.")
        elif t == "start_supply":
            engine_id = data.get("engine_id")
            if data.get("status") == "OK":
                print(f"[INFO] Suministro con {engine_id} iniciado: ya puede enchufar el vehículo.")
            else:
                print(f"[INFO] Suministro con {engine_id}: solicitud denegada")
                # pedir CP de nuevo (sin bloquear el listener)
                threading.Thread(target=self.ask_for_cp, daemon=True).start()
        elif t == "init_supply":
            print("[INFO] Suministrando...")
        elif t == "end_supply":
            print("[INFO] Fin de suministro.")
            threading.Thread(target=self.ask_for_cp, daemon=True).start()
        elif t == "driver_cp_info_resposne":
            resp_id = data.get("id")
            with self._last_cp_lock:
                # guardar para consulta por el hilo que está esperando
                self._last_cp_data = data
                if self._current_cp_request_id and resp_id == self._current_cp_request_id:
                    if self._cp_event:
                        self._cp_event.set()
            # mostrar info en consola (no bloquea)
            self.show_cp(data)

    def solicitar_carga(self, cp_id: str):
        mensaje = {
            "type": "driver_supply_request",
            "driver_id": self.id,
            "engine_id": cp_id,
            "timestamp": time.time(),
            "id": str(uuid.uuid4())
        }
        try:
            self.producer.produce(TOPIC, json.dumps(mensaje).encode("utf-8"))
            self.producer.flush(timeout=5)
            print(f"[KAFKA] Solicitud de carga enviada al topic '{TOPIC}'")
        except Exception as e:
            print(f"[KAFKA] Error enviando solicitud de carga: {e}")

    def ask_for_cp(self, retry_timeout=5, max_retries=None):
        retry = 0
        while not self._stop.is_set():
            req_id = str(uuid.uuid4())
            msg = {
                "type": "driver_cp_info",
                "driver_id": self.id,
                "id": req_id,
                "timestamp": time.time(),
            }

            event = threading.Event()
            with self._last_cp_lock:
                self._cp_event = event
                self._current_cp_request_id = req_id
                self._last_cp_data = None
            try:
                self.producer.produce(TOPIC, json.dumps(msg).encode("utf-8"))
                self.producer.flush(timeout=5)
            except Exception as e:
                print(f"[WARN] Error enviando petición a central: {e}")
                backoff = min(10, 1 + retry)
                time.sleep(backoff)
                retry += 1
                if max_retries and retry >= max_retries:
                    print("[ERROR] Máximos reintentos alcanzados al enviar a central.")
                    return None
                continue

            print(f"[INFO] Solicitando CP disponibles a Central (req_id={req_id})")

            received = event.wait(timeout=retry_timeout)
            if received:
                with self._last_cp_lock:
                    data = self._last_cp_data
                if data:
                    return data
                else:
                    # Esto no debería ocurrir, pero reintentar por seguridad
                    print("[WARN] Evento activado pero sin datos; reintentando...")
            else:
                retry += 1
                print("[WARN] No se recibió respuesta de CP en el timeout. Reintentando...")
                backoff = min(10, 1 + retry)
                time.sleep(backoff)
                if max_retries and retry >= max_retries:
                    print("[ERROR] Máximos reintentos alcanzados esperando respuesta de central.")
                    return None
        return None

    def show_cp(self, data):
        info = data.get("info", [])
        print("\nPuntos de carga disponibles:")
        for cp_id in info:
            print(f"  - CP: {cp_id}")

    def main(self):
        while not self._stop.is_set():
            cmd = input("\nIntroduce CP (id), 'r' reintentar, 'q' salir: ").strip()
            if cmd == "q":
                exit()
                break
            elif cmd == "r":
                self.ask_for_cp()
            elif cmd:
                self.solicitar_carga(cmd)

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

    driver.start()
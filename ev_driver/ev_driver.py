import uuid
import time
import json
import argparse
import threading
from concurrent.futures import ThreadPoolExecutor
from confluent_kafka import Producer, Consumer, KafkaException, KafkaError

class Driver:
    def __init__(self, id: str, location="Zone 0", broker="127.0.0.1:9092", filename=None):
        self.id = id
        self.broker = broker
        self.filename = filename
        self.services = []

        self.exit = False
        self.waiting = False
        self.unresponsed = {
            "driver_cp_info": [],
            "driver_supply_request": []
        }

        self.location = location
        _topic = location.replace(" ", "").lower()
        self.consumer_topic = f"{_topic}-central-response"
        self.producer_topic = f"{_topic}-central-request"

        self.consumer = Consumer({
            'bootstrap.servers': self.broker,
            'group.id': f'driver-service-{self.id}',
            'auto.offset.reset': 'latest',
            'enable.auto.commit': True
        })
        self.consumer.subscribe([self.consumer_topic])

        print("[KAFKA] Iniciando consumidor...")
        for _ in range(500):
            self.consumer.poll(0.5)
            assignment = self.consumer.assignment()
            if assignment: 
                print(f"[KAFKA] Escuchando solicitudes...")
                break

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
                        pass
                        # print(f"[KAFKA] Mensaje no válido recibido: {e}")
            except Exception as e:
                print(f"[KAFKA] Error en el consumidor Kafka: {e}")
            finally:
                self.consumer.close()
    
    def _procesar_driver_msg(self, data):
        if data.get("driver") != self.id: return
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
                print(f"[INFO] Surtidor {data.get('cp')} disponible para suministro.")
            else:
                print(f"[INFO] Suministro con {data.get('cp')}: solicitud denegada")
                self.ask_for_cp()
        elif t == "start_supply":
            cp = data.get("cp")
            if data.get("status") == "aceptada":
                print(f"[INFO] Suministro con {cp} iniciado: ya puede enchufar el vehículo.")
            else:
                print(f"[INFO] Suministro con {cp}: solicitud denegada")
                self._continue()
        elif t == "init_supply":
            cp_id = data.get("cp")
            print(f"[INFO] {cp_id} ha empezado a suministrar")
        elif t == "supply_info":
            kwh = float(data.get("consumo"))
            print(f"[INFO] Consumo: {kwh} kWh")
        elif t == "ticket":
            print("[INFO] Fin de suministro.")
            consumo = data.get('consumo')
            total = data.get('total')
            print(f"[TICKET] Consumo: {consumo} kWh - Total: {total}€")
            self._continue()
        elif t == "ticket_history_response":
            self.show_ticket_history(data)
            self.ask_for_cp()
    
    def start(self):
        threading.Thread(target=self.kafka_listener, daemon=True).start()
        if self.filename:
            self.waiting = True
            self.cargar_lista()
            self.file_main()
            while not self.exit: time.sleep(1)
        else:
            self.manual_main()

    def cargar_lista(self):
        with open(self.filename, "r", encoding="utf-8") as f:
            self.services = [linea.strip() for linea in f if linea.strip()]

    def file_main(self):
        if self.services:
            service = self.services.pop(0)
            self.solicitar_carga(service)
        else:
            self.exit = True
            exit()

    def manual_main(self):
        self.ask_for_cp()
        time.sleep(10)
        while not self.exit: # Bucle principal
            if self.unresponsed["driver_cp_info"]:
                print("[ERROR] Tiempo de espera agotado (1)")
                self.ask_for_cp()
            time.sleep(10)

    def _continue(self):
        time.sleep(4)
        if self.filename:
            self.file_main()
        else:
            self.ask_for_cp()

    def solicitar_carga(self, cp_id: str):
        print(f"[INFO] Solicitando carga con {cp_id}")
        req_id = str(uuid.uuid4())
        self.unresponsed["driver_supply_request"].append(req_id)
        mensaje = {
            "id": req_id,
            "type": "driver_supply_request",
            "driver": self.id,
            "cp": cp_id,
            "zone": self.location,
            "timestamp": time.time(),
        }

        self.producer.produce(self.producer_topic, json.dumps(mensaje).encode("utf-8"))
        self.producer.flush(timeout=5)

    def ask_for_cp(self):
        self.waiting = False
        print("[INFO] Solicitando a Central CP disponibles")
        req_id = str(uuid.uuid4())
        self.unresponsed["driver_cp_info"].append(req_id)
        msg = {
            "id": req_id,
            "type": "driver_cp_info",
            "driver": self.id,
            "zone": self.location,
            "timestamp": time.time(),
        }

        self.producer.produce(self.producer_topic, json.dumps(msg).encode("utf-8"))
        self.producer.flush(timeout=5)

    def show_cp(self, data):
        info = data.get("info", [])
        
        while True:
            if not info:
                print(f"\nNo hay puntos de carga disponibles en {self.location}.")
            else:
                print(f"\nPuntos de carga disponibles en {self.location}:")
                for cp_id in info:
                    print(f"  - CP: {cp_id}")
            
            cmd = input(
                "\nIntroduce CP (id), 'r' recargar, 'h' historial, 'q' salir: "
            )

            # --- Salir ---
            if cmd == "q":
                self.exit = True
                break

            # --- Recargar vista ---
            if cmd == "r":
                self.ask_for_cp()
                break

            # --- Historial ---
            if cmd == "h":
                self.get_history()
                break

            # --- Solicitar carga ---
            if cmd:
                self._handle_charge_request(cmd)
                break

    def _handle_charge_request(self, cp_id):
        print(f"\nSolicitando carga en CP: {cp_id}...")
        self.solicitar_carga(cp_id)
        time.sleep(5)

        # Reintentos si sigue sin respuesta
        retries = 0
        max_retries = 3

        while self.unresponsed["driver_supply_request"]:
            retries += 1
            if retries > max_retries:
                print("[ERROR] Tiempo de espera agotado. No se obtuvo respuesta del servidor.")
                return

            print(f"[Aviso] Sin respuesta, reintentando... ({retries}/{max_retries})")
            self.solicitar_carga(cp_id)
            time.sleep(5)

    def get_history(self):
        msg = {
            "id": str(uuid.uuid4()),
            "type": "ticket_history",
            "driver": self.id,
            "zone": self.location,
            "timestamp": time.time()
        }

        self.producer.produce(self.producer_topic, json.dumps(msg).encode("utf-8"))
        self.producer.flush(timeout=5)

    def show_ticket_history(self, data):
        driver = data.get("driver")
        tickets = data.get("tickets", [])

        print(f"\n[INFO] Historial de tickets recibido para {driver}:")
        if not tickets:
            print("  - No hay tickets registrados.")
            return

        for fecha, punto_carga, total in tickets:
            print(f"  • Fecha: {fecha} | Punto de carga: {punto_carga} | Total: {total}€")

        print(f"[INFO] Total de tickets: {len(tickets)}\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="EV_DRIVER")
    parser.add_argument("id", help="ID del Driver")
    parser.add_argument("--location", default="Zone 0", help="Ubicación")
    parser.add_argument("--broker", default="127.0.0.1:9092", help="IP del broker")
    parser.add_argument("--file", default=None, help="Fichero de operaciones")
    args = parser.parse_args()

    driver = Driver(
        id=args.id,
        location=args.location,
        broker=args.broker,
        filename=args.file
    )

    driver.start()
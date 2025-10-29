from typing import Dict

import uuid
import time
import argparse
import threading

import db
from charging_point import EV_CP, EstadoCP
from ev_central_gui import EV_Central_UI
from kafka_handler import Kafka_Handler
from socket_handler import Socket_Handler

class EV_Central:
    def __init__(
        self, ui_callback = None, 
        broker: str = "localhost:9092", 
        port: int = 6000
    ):
        self.db = db.DataBase()
        self.last_msg: Dict[str, float] = {}
        self.ui_callback = ui_callback

        self.charging_points: Dict[str, EV_CP] = self.db.load_charging_points()
        self.drivers: Dict[str, str] = {}

        self.socket_handler = Socket_Handler(self, port=port)
        self.kafka_handler = Kafka_Handler(self, broker)

    def _notificar_ui(self):
        if self.ui_callback:
            self.ui_callback(self.charging_points)

    def check_timeouts(self, timeout=5):
        while True:
            now = time.time()
            for id, last in self.last_msg.items():
                if now - last > timeout:
                    if self.charging_points[id].estado != EstadoCP.DESCONECTADO:
                        self.charging_points[id].estado = EstadoCP.DESCONECTADO
                        self._notificar_ui()
            time.sleep(1)

    def registrar_punto(self, id: str, msg: dict):
        punto = EV_CP(id, msg["location"], msg["price"], EstadoCP.DESCONECTADO)

        if id not in self.charging_points:
            self.charging_points[id] = punto
            self.db.save_charging_points(punto)
            gestor.last_msg[id] = time.time()

        self.charging_points[id].estado = EstadoCP.ACTIVADO
        self._notificar_ui()

    def actualizar_estado(self, id: str, nuevo_estado: EstadoCP):
        if id in self.charging_points:
            cond_av = nuevo_estado == EstadoCP.AVERIADO
            cond_su = self.charging_points[id].estado == EstadoCP.SUMINISTRANDO
            if cond_av and cond_su:
                print(f"[ERROR] {id} ha caído mientras suministraba")
                cp = self.charging_points[id]
                data = {
                    "cp": id,
                    "driver": cp.driver,
                    "consumo": cp.kwh
                }

                self.finalizar_suministro(data, True)

            self.charging_points[id].estado = nuevo_estado
            gestor.last_msg[id] = time.time()
            self._notificar_ui()

    def topic(self, zone):
        _zone = zone.replace(" ", "").lower()
        return f"{_zone}-central-response"
    
    def procesar_solicitud_cp(self, data):
        id = data.get("id")
        cp_id = data.get("cp")
        zone=data.get("zone")
        
        print(f"[INFO] Solicitud de suministro recibida de {cp_id} ({id})")
        cp = self.charging_points[cp_id]
        status = "approved" if cp.can_supply() else "denied"

        response = {
            "id": id,
            "type": "engine_supply_response",
            "cp": cp_id,
            "status": status,
            "zone": zone,
            "timestamp": time.time()
        }

        print(f"[INFO] Solicitud {id}: {status}")
        self.kafka_handler.send_msg(response, self.topic(zone))

    def procesar_solicitud_driver(self, data):
        id = data.get("id")
        cp_id = data.get("cp")
        driver = data.get("driver")
        zone = data.get("zone")
        self.drivers[driver] = zone

        print(f"[INFO] {driver} ha solicitado recargar en {cp_id} ({id})")
        cp = self.charging_points.get(cp_id, None)
        msg = {
            "id": id,
            "type": "start_supply",
            "status": "denegada",
            "driver": driver,
            "cp": cp_id,
            "zone": zone,
            "timestamp": time.time()
        }

        if not cp:
            print(f"[INFO] {cp_id} no está registrado")
        elif cp.can_supply():
            print(f"[INFO] El CP {cp_id} está Operativo. Comprobando disponibilidad...")
            msg = {
                "id": id,
                "type": "supply_request",
                "status": "aceptada",
                "cp": cp_id,
                "driver": driver,
                "zone": zone,
                "timestamp": time.time()
            }

            if zone != cp.location:
                self.kafka_handler.send_msg(msg, self.topic(cp.location))
        else:
            print(f"[INFO] {cp_id} no disponible: Solicitud denegada ({id})")
        
        self.kafka_handler.send_msg(msg, self.topic(zone))

    def supply_response(self, data):
        id = data.get("id")
        cp_id = data.get("cp")
        driver = data.get("driver")
        status = data.get("status", "denegada")
        zone = data.get("zone")

        print(f"[INFO] Solicitud {id}: {status}")

        response = {
            "id": id,
            "type": "start_supply",
            "cp": cp_id,
            "driver": driver,
            "status": status,
            "zone": zone,
            "timestamp": time.time()
        }

        cp = self.charging_points[cp_id]
        if zone != cp.location:
            self.kafka_handler.send_msg(response, self.topic(cp.location))

        self.kafka_handler.send_msg(response, self.topic(zone))

    def share_cp(self, data):
        id = data.get("id")
        driver = data.get("driver")
        zone = data.get("zone")

        print(f"[INFO] {driver} ha solicitado los CP disponibles en {zone} ({id})")

        for_share_cp = [
            cp_id for cp_id, punto in self.charging_points.items()
            if punto.estado == EstadoCP.ACTIVADO and punto.location == zone
        ]

        response = {
            "id": id,
            "type": "driver_cp_info_resposne",
            "driver": driver,
            "info": for_share_cp,
            "zone": zone,
            "timestamp": time.time(),
        }

        print(f"[INFO] Enviando CPs disponibles a {driver} ({id})")
        self.kafka_handler.send_msg(response, self.topic(zone))

    def parar_cp(self, cp_id):
        if self.charging_points[cp_id].estado == EstadoCP.PARADO:
            print(f"[INFO] Restableciendo {cp_id}")
        else:
            print(f"[INFO] Parando {cp_id}")

        self.actualizar_estado(cp_id, EstadoCP.PARADO)
        zone = self.charging_points[cp_id].location
        msg = {
            "id": str(uuid.uuid4()),
            "type": "start_stop_services",
            "cp": cp_id,
            "zone": zone,
            "timestamp": time.time()
        }
        
        zone = zone.replace(" ", "").lower()
        self.kafka_handler.send_msg(msg, self.topic(zone))

    def suministrando(self, data):
        cp_id = data.get("cp")
        driver = data.get("driver")
        kwh = float(data.get("consumo"))
        price = self.charging_points[cp_id].price

        self.charging_points[cp_id].driver = driver
        self.charging_points[cp_id].kwh = kwh
        self.charging_points[cp_id].ticket = round(kwh * price, 2)

        driver_msg = f"a {driver}" if driver else ""
        print(f"[INFO] {cp_id} ha suministrado {kwh} kWh {driver_msg}")
        if driver: self.kafka_handler.send_msg(data, self.topic(self.drivers[driver]))
        self._notificar_ui()

    def finalizar_suministro(self, data, error=False):
        cp_id = data.get("cp")
        driver = data.get("driver", None)
        kwh = data.get("consumo")
        price = self.charging_points[cp_id].price

        total_ticket = round(kwh * price, 2)
        if error:
            print(f"[INFO] {cp_id} ha finalizado debido a una averia ({kwh} kWh): {total_ticket}€")
        else:
            print(f"[INFO] {cp_id} ha finalizado ({kwh} kWh): {total_ticket}€")

        if driver:
            zone = self.drivers[driver]

            ticket = {
                "id": str(uuid.uuid4()),
                "type": "ticket",
                "driver": driver,
                "consumo": kwh,
                "zone": zone,
                "total": total_ticket
            }

            print(f"[INFO] Enviando ticket a {driver}")
            self.kafka_handler.send_msg(ticket, self.topic(zone))
        
        self.db.guardar_ticket(driver, cp_id, total_ticket)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="EV_CENTRAL")
    parser.add_argument("--broker", default="localhost:9092", help="Dirección del broker")
    parser.add_argument("--port", type=int, default=6000, help="Puerto de escucha")
    args = parser.parse_args()

    gestor = EV_Central(
        broker = args.broker,
        port = args.port
    )

    ui = EV_Central_UI(gestor)

    threading.Thread(target=gestor.check_timeouts, daemon=True).start()
    threading.Thread(target=gestor.socket_handler.start_listener, daemon=True).start()
    threading.Thread(target=gestor.kafka_handler.start_listener, daemon=True).start()

    ui.run()
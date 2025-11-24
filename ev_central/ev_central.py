from typing import Dict

import uuid
import time
import argparse
import threading

from db import EVCentralAPI
from charging_point import EstadoCP
from ev_central_gui import EV_Central_UI
from kafka_handler import Kafka_Handler
from socket_handler import Socket_Handler

DB_URL = "http://localhost:9000"

class EV_Central:
    def __init__(
        self, ui_callback = None, 
        broker: str = "localhost:9092", 
        port: int = 6000
    ):
        self.db = EVCentralAPI(DB_URL)
        self.db.reset_all_charging_points()
        self.last_msg: Dict[str, float] = {}
        self.ui_callback = ui_callback

        self.tickets = []

        self.socket_handler = Socket_Handler(self, port=port)
        self.kafka_handler = Kafka_Handler(self, broker)

    def _notificar_ui(self):
        if self.ui_callback:
            self.ui_callback(self.db.load_charging_points())

    def check_timeouts(self, timeout=5):
        while True:
            now = time.time()
            for cp_id, last in self.last_msg.items():
                if now - last > timeout:
                    cp = self.db.get_charging_point(cp_id)
                    if cp is None:
                        continue
                    
                    if cp["estado"] != EstadoCP.DESCONECTADO:
                        self.db.update_estado(cp_id, EstadoCP.DESCONECTADO.value)
                        self._notificar_ui()
            time.sleep(1)

    def registrar_punto(self, id: str, msg: dict):
        punto = {
            "id": id,
            "location": msg["location"],
            "price": msg["price"],
            "estado": EstadoCP.DESCONECTADO.value,
            "driver": None,
            "kwh": 0,
            "time": None,
            "auth_key": None,
            "session_key": None
        }

        self.db.save_charging_point(punto)
        gestor.last_msg[id] = time.time()
        self.db.update_estado(punto["id"], EstadoCP.ACTIVADO.value)
        
        self._notificar_ui()

    def actualizar_estado(self, id: str, nuevo_estado: EstadoCP):
        cp = self.db.get_charging_point(id)
        if cp is None: return

        cond_av = nuevo_estado == EstadoCP.AVERIADO
        cond_su = cp["estado"] == EstadoCP.SUMINISTRANDO

        if cond_av and cond_su:
            data = {
                "cp": id,
                "driver": cp["driver"],
                "consumo": cp["kwh"],
                "zone": cp["location"]
            }
            self.finalizar_suministro(data, True)

        cond_init_su = nuevo_estado == EstadoCP.SUMINISTRANDO and not cp["time"]
        if cond_init_su:
            cp["time"] = time.time()
            self.db.start_time(id, cp["time"])
        elif nuevo_estado == EstadoCP.ACTIVADO:
            cp["time"] = None
            self.db.start_time(id, None)

        self.db.update_estado(cp["id"], nuevo_estado.value)
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
        cp = self.db.get_charging_point(cp_id)
        status = "approved" if cp["estado"] == EstadoCP.ACTIVADO else "denied"

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

        self.db.save_driver(driver, zone)

        print(f"[INFO] {driver} ha solicitado recargar en {cp_id} ({id})")

        cp = self.db.get_charging_point(cp_id)

        msg = {
            "id": id,
            "type": "supply_request",
            "status": "denegada",
            "driver": driver,
            "cp": cp_id,
            "zone": zone,
            "timestamp": time.time()
        }

        if cp is None:
            print(f"[INFO] {cp_id} no está registrado")
        elif cp["estado"] == EstadoCP.ACTIVADO:
            print(f"[INFO] El CP {cp_id} está Operativo. Comprobando disponibilidad...")
            msg["status"] = "aceptada"

            if zone != cp["location"]:
                self.kafka_handler.send_msg(msg, self.topic(cp["location"]))
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

        cp = self.db.get_charging_point(cp_id)
        if zone != cp["location"]:
            self.kafka_handler.send_msg(response, self.topic(cp["location"]))

        self.kafka_handler.send_msg(response, self.topic(zone))

    def share_cp(self, data):
        id = data.get("id")
        driver = data.get("driver")
        zone = data.get("zone")

        print(f"[INFO] {driver} ha solicitado los CP disponibles en {zone} ({id})")

        all_cps = self.db.load_charging_points()
        for_share_cp = [
            cp_id for cp_id, cp in all_cps.items()
            if cp["estado"] == EstadoCP.ACTIVADO and cp["location"] == zone
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
        cp = self.db.get_charging_point(cp_id)
        if cp is None:
            print(f"[ERROR] Punto de carga {cp_id} no encontrado")
            return

        if cp["estado"] == EstadoCP.PARADO:
            print(f"[INFO] Restableciendo {cp_id}")
        else:
            print(f"[INFO] Parando {cp_id}")

        self.actualizar_estado(cp_id, EstadoCP.PARADO)

        zone = cp["location"]
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

        cp = self.db.get_charging_point(cp_id)
        if cp is None:
            print(f"[ERROR] Punto de carga {cp_id} no encontrado")
            return

        cp["driver"] = driver
        cp["kwh"] = kwh
        cp["ticket"] = round(kwh * cp["price"], 2)

        self.db.set_driver(cp_id, driver)
        self.db.update_kwh(cp_id, kwh)

        driver_msg = f"a {driver}" if driver else ""
        print(f"[INFO] {cp_id} ha suministrado {kwh} kWh {driver_msg}")

        if driver:
            zone = self.db.load_drivers().get(driver, None)
            if zone:
                self.kafka_handler.send_msg(data, self.topic(zone))

        self._notificar_ui()

    def finalizar_suministro(self, data, error=False):
        id = data.get("id")

        if id in self.tickets:
            return
        self.tickets.append(id)

        cp_id = data.get("cp")
        driver = data.get("driver", None)
        kwh = data.get("consumo")

        cp = self.db.get_charging_point(cp_id)
        if cp is None:
            print(f"[ERROR] Punto de carga {cp_id} no encontrado")
            return

        total_ticket = round(kwh * cp["price"], 2)
        if error:
            print(f"[INFO] {cp_id} ha finalizado debido a una averia ({kwh} kWh): {total_ticket}€")
        else:
            print(f"[INFO] {cp_id} ha finalizado ({kwh} kWh): {total_ticket}€")

        if driver:
            drivers_zone = self.db.load_drivers()
            zone = drivers_zone.get(driver, None)

            ticket = {
                "id": id,
                "type": "ticket",
                "driver": driver,
                "consumo": kwh,
                "zone": zone,
                "total": total_ticket,
                "timestamp": time.time()
            }

            print(f"[INFO] Enviando ticket a {driver}")
            if zone:
                self.kafka_handler.send_msg(ticket, self.topic(zone))

        self.db.guardar_ticket(driver, cp_id, total_ticket)

        if not error:
            msg = {
                "id": id,
                "type": "end_supply_registered",
                "cp": cp_id,
                "timestamp": time.time()
            }

            self.kafka_handler.send_msg(msg, self.topic(data.get("zone")))

    def ticket_history(self, data):
        id = data.get("id")
        driver = data.get("driver")
        zone = data.get("zone")

        print(f"[INFO] {driver} ha solicitado su historial de tickets")

        tickets = self.db.get_tickets_by_driver(driver)

        msg = {
            "id": id,
            "type": "ticket_history_response",
            "driver": driver,
            "zone": zone,
            "tickets": tickets,
            "timestamp": time.time()
        }

        self.kafka_handler.send_msg(msg, self.topic(zone))
        print(f"[INFO] Enviando historial de tickets de {driver}")

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

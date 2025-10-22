from typing import Dict

import time
import threading

import db
from charging_point import EV_CP, EstadoCP
from ev_central_gui import EV_Central_UI
from kafka_handler import Kafka_Handler
from socket_handler import Socket_Handler

class EV_Central:
    def __init__(self, ui_callback=None):
        self.bd = db.DataBase()
        self.charging_points: Dict[str, EV_CP] = self.bd.load_charging_points()
        self.last_msg: Dict[str, float] = {}
        self.ui_callback = ui_callback

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
            self.bd.save_charging_points(punto)
            gestor.last_msg[id] = time.time()

        self.charging_points[id].estado = EstadoCP.ACTIVADO
        self._notificar_ui()

    def actualizar_estado(self, id: str, nuevo_estado: EstadoCP):
        if id in self.charging_points:
            cond_av = nuevo_estado == EstadoCP.AVERIADO
            cond_su = self.charging_points[id].estado == EstadoCP.SUMINISTRANDO
            if cond_av and cond_su:
                print(f"[ERROR] {id} ha caído mientras suministraba")
            #     restored_data = {
            #         "engine_id": id,
            #         "driver_id": self.charging_points[id].driver,
            #         "consumo": self.charging_points[id].kwh
            #     }

            #     self.finalizar_suministro(restored_data, True)

            self.charging_points[id].estado = nuevo_estado
            gestor.last_msg[id] = time.time()
            self._notificar_ui()

    def can_supply(self, id: str) -> bool:
        return self.charging_points[id].estado == EstadoCP.ACTIVADO
    
    def suministrando(self, data):
        cp_id = data.get("engine_id")
        driver_id = data.get("driver_id")
        kwh = float(data.get("consumo"))
        price = self.charging_points[cp_id].price

        self.charging_points[cp_id].driver = driver_id
        self.charging_points[cp_id].kwh = kwh
        self.charging_points[cp_id].ticket = round(kwh * price, 2)

        driver_msg = f"a {driver_id}" if driver_id else ""
        print(f"[INFO] {cp_id} ha suministrado {kwh} kWh {driver_msg}")
        self._notificar_ui()

    def finalizar_suministro(self, data, error=False):
        cp_id = data.get("engine_id")
        # driver_id = data.get("driver_id")
        kwh = data.get("consumo")
        price = self.charging_points[cp_id].price

        error_msg = "debido a una averia" if error else ""
        ticket = round(kwh * price, 2)
        print(f"[INFO] {cp_id} ha finalizado {error_msg} ({kwh} kWh): {ticket}€")

if __name__ == "__main__":
    gestor = EV_Central()
    ui = EV_Central_UI(gestor)
    kafka_handler = Kafka_Handler(gestor)
    socket_handler = Socket_Handler(gestor)

    threading.Thread(target=gestor.check_timeouts, daemon=True).start()
    threading.Thread(target=socket_handler.start_listener, daemon=True).start()
    threading.Thread(target=kafka_handler.start_listener, daemon=True).start()

    ui.run()
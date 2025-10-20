import tkinter as tk
from typing import Dict

from confluent_kafka import Producer, Consumer, KafkaException, KafkaError

import time
import json
import socket
import threading

import db
from charging_point import EV_CP, EstadoCP

class EV_Central:
    def __init__(self, ui_callback=None):
        self.bd = db.DataBase()
        self.charching_points: Dict[str, EV_CP] = self.bd.load_charching_points()
        self.last_msg: Dict[str, float] = {}
        self.ui_callback = ui_callback

    def check_timeouts(self, timeout=3):
        while True:
            now = time.time()
            for id, last in self.last_msg.items():
                if now - last > timeout:
                    if self.charching_points[id].estado != EstadoCP.DESCONECTADO:
                        self.charching_points[id].estado = EstadoCP.DESCONECTADO
                        self._notificar_ui()
            time.sleep(1)

    def registrar_punto(self, id: str, ubicacion: str):
        if id not in self.charching_points:
            punto = EV_CP(id, ubicacion, EstadoCP.ACTIVADO)
            self.charching_points[id] = punto
            self.bd.save_charching_points(punto)
            gestor.last_msg[id] = time.time()

        self.charching_points[id].estado = EstadoCP.ACTIVADO
        self._notificar_ui()

    def actualizar_estado(self, id: str, nuevo_estado: EstadoCP):
        if id in self.charching_points:
            self.charching_points[id].estado = nuevo_estado
            gestor.last_msg[id] = time.time()
            self._notificar_ui()

    def can_supply(self, id: str) -> bool:
        return self.charching_points[id].estado == EstadoCP.ACTIVADO

    def _notificar_ui(self):
        if self.ui_callback:
            self.ui_callback(self.charching_points)

# =============================================================
# INTERFAZ GRÁFICA
# =============================================================

colores = {
    "ACTIVADO": "#2e7d32",
    "SUMINISTRANDO": "#2e7d32",
    "PARADO": "#ef6c00",
    "AVERIADO": "#c62828",
    "DESCONECTADO": "#616161"
}

class EV_Central_UI:
    def __init__(self, gestor):
        self.gestor = gestor
        self.gestor.ui_callback = self.update_ev_cp

        self.root = tk.Tk()
        self.root.title("EV_CENTRAL")

        self.contenedor = tk.Frame(self.root)
        self.contenedor.pack(fill=tk.BOTH, expand=True, padx=15, pady=15)

        self.update_ev_cp(self.gestor.charching_points)

    def update_ev_cp(self, charching_points):
        self.contenedor.destroy()
        self.contenedor = tk.Frame(self.root)
        self.contenedor.pack(fill=tk.BOTH, expand=True, padx=15, pady=15)

        for i, punto in enumerate(charching_points.values()):
            bg_color = colores[punto.estado.name]

            frame = tk.Frame(
                self.contenedor,
                width=100,
                height=50,
                relief=tk.RAISED,
                borderwidth=2,
                bg=bg_color
            )

            frame.grid(row=i // 4, column=i % 4, padx=10, pady=10)
            # frame.grid_propagate(False)
            frame.pack_propagate(False)

            label_id = tk.Label(
                frame, 
                text=f"ID: {punto.id}",
                font=("Arial", 10, "bold"), 
                fg="white", 
                bg=bg_color
            )

            label_id.pack(pady=(5, 2))

            label_estado = tk.Label(
                frame, 
                text=punto.estado.name,
                font=("Arial", 9, "italic"), 
                fg="white", 
                bg=bg_color
            )

            label_estado.pack(pady=(3, 4))

        self.root.update_idletasks()

    def run(self):
        for i in range(4):
            self.contenedor.grid_columnconfigure(i, weight=1)
        self.root.mainloop()

# =============================================================
# SOCKETS
# =============================================================
def socket_listener(gestor: EV_Central, host="0.0.0.0", port=5001):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((host, port))
    server.listen(5)
    print(f"Socket escuchando en {host}:{port}")

    while True:
        client, addr = server.accept()
        threading.Thread(target=handle_client, args=(client, gestor), daemon=True).start()

def handle_client(client_socket, gestor: EV_Central):
    with client_socket:
        data = client_socket.recv(4096)
        if not data: return

        try:
            mensaje = json.loads(data.decode("utf-8"))
            tipo = mensaje.get("type")

            if tipo == "auth":
                gestor.registrar_punto(mensaje["id"], mensaje["ubicacion"])
                client_socket.send(b"OK - auth")
            elif tipo == "status":
                estado_str = mensaje.get("status", "KO")
                estado = EstadoCP.ACTIVADO if estado_str == "OK" else EstadoCP.AVERIADO
                gestor.actualizar_estado(mensaje["id"], estado)
                client_socket.send(b"OK - status")
            else:
                client_socket.send(b"ERROR - unknown msg")

        except Exception as e:
            print("Error procesando mensaje:", e)
            client_socket.send(b"ERROR")

# =============================================================
# KAFKA BROKER
# =============================================================
BROKER_HOST = "localhost"
BROKER_PORT = 9092
INPUT_TOPIC = "requests"
OUTPUT_TOPIC = "responses"

def crear_consumidor():
    conf = {
        'bootstrap.servers': f"{BROKER_HOST}:{BROKER_PORT}",
        'group.id': 'central-service',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(conf)
    consumer.subscribe([INPUT_TOPIC])
    print(f"🏭 Central escuchando solicitudes en topic '{INPUT_TOPIC}'...")
    return consumer

def crear_productor():
    conf = {'bootstrap.servers': f"{BROKER_HOST}:{BROKER_PORT}"}
    producer = Producer(conf)
    return producer

def procesar_solicitud(data, producer, gestor: EV_Central):
    try:
        correlation_id = data["correlation_id"]
        engine_id = data.get("engine_id", "unknown")

        print(f"📥 Solicitud recibida de {engine_id} (ID {correlation_id})")

        status = "approved" if gestor.can_supply(correlation_id) else "denied"

        response = {
            "correlation_id": correlation_id,
            "status": status,
            "timestamp": time.time()
        }

        producer.produce(OUTPUT_TOPIC, json.dumps(response).encode("utf-8"))
        producer.flush()

        print(f"📤 Respuesta enviada a '{OUTPUT_TOPIC}': {status}")
    except Exception as e:
        print(f"❌ Error procesando solicitud: {e}")

def kafka_listener(gestor: EV_Central):
    consumer = crear_consumidor()
    producer = crear_productor()

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    raise KafkaException(msg.error())
                continue

            try:
                data = json.loads(msg.value().decode("utf-8"))
                if data.get("type") == "supply_request":
                    procesar_solicitud(data, producer, gestor)
            except json.JSONDecodeError:
                print("⚠️  Mensaje no válido recibido.")
                continue
    except KeyboardInterrupt:
        print("\n🛑 Central detenida por el usuario.")
    finally:
        consumer.close()

# =============================================================
# EJECUCIÓN PRINCIPAL
# =============================================================

gestor = EV_Central()
ui = EV_Central_UI(gestor)

threading.Thread(target=gestor.check_timeouts, daemon=True).start()
threading.Thread(target=socket_listener, args=(gestor,), daemon=True).start()
threading.Thread(target=kafka_listener, args=(gestor,), daemon=True).start()

ui.run()
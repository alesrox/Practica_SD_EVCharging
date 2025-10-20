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
        if not hasattr(self, "frames"):
            self.frames = {}

        for i, punto in enumerate(charching_points.values()):
            bg_color = colores[punto.estado.name]

            if punto.id in self.frames:
                frame, label_id, label_estado = self.frames[punto.id]
                frame.config(bg=bg_color)
                label_id.config(bg=bg_color, text=f"ID: {punto.id}")
                label_estado.config(bg=bg_color, text=punto.estado.name)
            else:
                frame = tk.Frame(
                    self.contenedor,
                    width=120,
                    height=50,
                    relief=tk.RAISED,
                    borderwidth=2,
                    bg=bg_color
                )
                frame.grid(row=i // 4, column=i % 4, padx=10, pady=10)
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

                self.frames[punto.id] = (frame, label_id, label_estado)

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
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
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
                estado_str = mensaje.get("status", "AVERIADO")
                estado = EstadoCP[estado_str]
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

TOPIC = "central-request"

def crear_consumidor():
    conf = {
        'bootstrap.servers': f"{BROKER_HOST}:{BROKER_PORT}",
        'group.id': 'central-service',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(conf)
    consumer.subscribe([TOPIC])
    print(f"🏭 Central escuchando solicitudes en topic '{TOPIC}'...")
    return consumer

def crear_productor():
    conf = {'bootstrap.servers': f"{BROKER_HOST}:{BROKER_PORT}"}
    producer = Producer(conf)
    return producer

CONSUMER = crear_consumidor()
PRODUCER = crear_productor()

def kafka_listener(gestor: EV_Central):
    try:
        while True:
            msg = CONSUMER.poll(1.0)
            if msg is None: continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF: 
                    raise KafkaException(msg.error())
                continue
            try:
                data = json.loads(msg.value().decode("utf-8"))
                if data.get("type") == "driver_supply_request":
                    procesar_solicitud_driver(data, gestor)
                elif data.get("type") == "engine_supply_request":
                    procesar_solicitud_engine(data, gestor)
                elif data.get("type") == "supply_response":
                    response_driver(data, status=data.get("status", "KO"))
            except Exception as e:
                print(f"⚠️  Mensaje no válido recibido: {e}")
                continue
    except Exception as e:
        print(f"❌ Error en el consumidor Kafka: {e}")
    finally:
        CONSUMER.close()

def procesar_solicitud_engine(data, gestor: EV_Central):
    try:
        engine_id = data.get("engine_id")
        correlation_id = data.get("correlation_id")

        print(f"📥 Solicitud recibida de {engine_id} (ID {correlation_id})")

        status = "approved" if gestor.can_supply(correlation_id) else "denied"

        response = {
            "type": "engine_supply_response",
            "engine_id": engine_id,
            "correlation_id": correlation_id,
            "status": status,
            "timestamp": time.time()
        }

        PRODUCER.produce(TOPIC, json.dumps(response).encode("utf-8"))
        PRODUCER.flush()

        print(f"📤 Respuesta enviada a '{TOPIC}': {status}")
    except Exception as e:
        print(f"❌ Error procesando solicitud: {e}")

def procesar_solicitud_driver(data, gestor: EV_Central):
    driver_id = data.get("driver_id")
    engine_id = data.get("engine_id")
    correlation_id = data.get("correlation_id")

    print(f"📥 Solicitud recibida de {driver_id} para suministrarse en {engine_id} (ID {correlation_id})")
    if gestor.can_supply(engine_id):
        print(f"El CP {engine_id} está Operativo. Comprobando disponibilidad...")
        msg = {
            "type": "supply_request",
            "engine_id": engine_id,
            "driver_id": driver_id,
            "correlation_id": correlation_id,
            "timestamp": time.time()
        }

        PRODUCER.produce(TOPIC, json.dumps(msg).encode("utf-8"))
        PRODUCER.flush()
    else:
        response_driver(data, status="KO")
        print(f"❌ Suministro denegado para {driver_id} en {engine_id}")

def response_driver(data, status: str = "KO"):
    engine_id = data.get("engine_id")
    driver_id = data.get("driver_id")
    correlation_id = data.get("correlation_id")

    msg = "denegada" if status == "KO" else "aceptada"
    print(f"Solicitud de {engine_id} por {driver_id}: {msg}")

    response = {
        "type": "start_supply",
        "status": status,
        "driver_id": driver_id,
        "engine_id": engine_id,
        "correlation_id": correlation_id,
        "timestamp": time.time()
    }

    PRODUCER.produce(TOPIC, json.dumps(response).encode("utf-8"))
    PRODUCER.flush()

# =============================================================
# EJECUCIÓN PRINCIPAL
# =============================================================

gestor = EV_Central()
ui = EV_Central_UI(gestor)

threading.Thread(target=gestor.check_timeouts, daemon=True).start()
threading.Thread(target=socket_listener, args=(gestor,), daemon=True).start()
threading.Thread(target=kafka_listener, args=(gestor,), daemon=True).start()

ui.run()
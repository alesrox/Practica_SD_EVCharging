import sqlite3
from typing import Dict
from datetime import datetime
from charging_point import EV_CP, EstadoCP

DB_PATH = "EV_DATABASE.db"

class DataBase:
    def __init__(self, db_name=DB_PATH):
        self.db_name = db_name
        self.crear_tabla()

    def crear_tabla(self):
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS puntos_carga (
                id TEXT PRIMARY KEY,
                location TEXT NOT NULL,
                price FLOAT NOT NULL
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tickets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                driver_id TEXT,
                fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                punto_carga TEXT NOT NULL,
                total FLOAT NOT NULL,
                FOREIGN KEY (punto_carga) REFERENCES puntos_carga(id)
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS drivers (
                id TEXT PRIMARY KEY,
                location TEXT NOT NULL
            )
        """)

        conn.commit()
        conn.close()

    def save_charging_points(self, punto: EV_CP):
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        cursor.execute(
            "INSERT OR IGNORE INTO puntos_carga (id, location, price) VALUES (?, ?, ?)",
            (punto.id, punto.location, punto.price)
        )
        conn.commit()
        conn.close()

    def load_charging_points(self) -> Dict[str, EV_CP]:
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        cursor.execute("SELECT id, location, price FROM puntos_carga")
        rows = cursor.fetchall()
        conn.close()
        puntos = {}
        for row in rows:
            puntos[row[0]] = EV_CP(row[0], row[1], row[2], estado=EstadoCP.DESCONECTADO)
        return puntos
    
    def save_driver(self, driver_id, location):
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        cursor.execute(
            "INSERT OR IGNORE INTO drivers (id, location) VALUES (?, ?)",
            (driver_id, location)
        )
        conn.commit()
        conn.close()

    def load_drivers(self) -> Dict[str, EV_CP]:
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        cursor.execute("SELECT id, location FROM drivers")
        rows = cursor.fetchall()
        conn.close()

        drivers = {}
        for row in rows:
            drivers[row[0]] = row[1]

        return drivers
    
    def guardar_ticket(self, driver, cp_id, total_ticket):
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        cursor.execute("""
            INSERT INTO tickets (driver_id, punto_carga, total, fecha)
            VALUES (?, ?, ?, ?)
        """, (driver, cp_id, total_ticket, fecha))

        conn.commit()
        ticket = f"(Driver: {driver or 'N/A'}, CP: {cp_id}, Total: {total_ticket}€)"
        print(f"[DB] Ticket guardado {ticket}")

    def get_tickets_by_driver(self, driver_id: str):
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT fecha, punto_carga, total
            FROM tickets
            WHERE driver_id = ?
            ORDER BY fecha DESC
        """, (driver_id,))

        tickets = cursor.fetchall()
        conn.close()

        return tickets
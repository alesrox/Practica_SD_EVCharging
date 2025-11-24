import sqlite3
from typing import Dict
from datetime import datetime
from db.charging_point import EV_CP, EstadoCP

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
                price REAL NOT NULL,
                estado TEXT NOT NULL DEFAULT 'DESCONECTADO'
                    CHECK (estado IN ('ACTIVADO', 'PARADO', 'SUMINISTRANDO', 'AVERIADO', 'DESCONECTADO')),
                driver TEXT,
                kwh REAL,
                time REAL,
                auth_key TEXT,
                session_key TEXT,
                FOREIGN KEY (driver) REFERENCES drivers(id)
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

    def reset_all_charging_points(self):
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE puntos_carga
            SET estado = 'DESCONECTADO',
                driver = NULL,
                kwh = 0,
                time = NULL
        """)
        conn.commit()
        conn.close()

    def save_charging_points(self, punto: EV_CP, auth_key: str = None, session_key: str = None):
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO puntos_carga
            (id, location, price, estado, driver, kwh, time, auth_key, session_key)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            punto.id, punto.location, punto.price,
            punto.estado.value, punto.driver,
            punto.kwh, punto.time,
            auth_key, session_key
        ))
        conn.commit()
        conn.close()

    def get_charging_point(self, cp_id) -> EV_CP | None:
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, location, price, estado, driver, kwh, time, auth_key, session_key
            FROM puntos_carga
            WHERE id = ?
        """, (cp_id,))
        row = cursor.fetchone()
        conn.close()
        if row is None:
            return None
        estado_enum = EstadoCP(row[3])
        cp = EV_CP(
            id=row[0],
            location=row[1],
            price=row[2],
            estado=estado_enum,
            driver=row[4],
            kwh=row[5] if row[5] is not None else 0,
            ticket=0,
            auth_key=row[7],
            session_key=row[8]
        )
        cp.time = row[6]
        return cp

    def load_charging_points(self) -> Dict[str, EV_CP]:
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, location, price, estado, driver, kwh, time, auth_key, session_key 
            FROM puntos_carga
        """)
        rows = cursor.fetchall()
        conn.close()
        puntos = {}
        for row in rows:
            cp = EV_CP(
                id=row[0],
                location=row[1],
                price=row[2],
                estado=EstadoCP(row[3]),
                driver=row[4],
                kwh=row[5] if row[5] is not None else 0,
                auth_key=row[7],
                session_key=row[8]
            )
            cp.time = row[6]
            puntos[row[0]] = cp
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

    def load_drivers(self) -> Dict[str, str]:
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        cursor.execute("SELECT id, location FROM drivers")
        rows = cursor.fetchall()
        conn.close()
        drivers = {row[0]: row[1] for row in rows}
        return drivers

    def update_estado(self, cp: EV_CP | str, nuevo_estado: EstadoCP):
        cp_id = cp.id if isinstance(cp, EV_CP) else cp
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        cursor.execute("UPDATE puntos_carga SET estado = ? WHERE id = ?", (nuevo_estado.value, cp_id))
        conn.commit()
        conn.close()
        if isinstance(cp, EV_CP):
            cp.estado = nuevo_estado

    def set_driver(self, cp_id: str, driver: str | None):
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        cursor.execute("UPDATE puntos_carga SET driver = ? WHERE id = ?", (driver, cp_id))
        conn.commit()
        conn.close()

    def update_kwh(self, cp_id: str, kwh: float):
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        cursor.execute("UPDATE puntos_carga SET kwh = ? WHERE id = ?", (kwh, cp_id))
        conn.commit()
        conn.close()

    def start_time(self, cp_id: str, timestamp: float | None):
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        cursor.execute("UPDATE puntos_carga SET time = ? WHERE id = ?", (timestamp, cp_id))
        conn.commit()
        conn.close()

    def guardar_ticket(self, driver, cp_id, total_ticket):
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cursor.execute("""
            INSERT INTO tickets (driver_id, punto_carga, total, fecha)
            VALUES (?, ?, ?, ?)
        """, (driver, cp_id, total_ticket, fecha))
        conn.commit()
        print(f"[DB] Ticket guardado (Driver: {driver or 'N/A'}, CP: {cp_id}, Total: {total_ticket}€)")
        conn.close()

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
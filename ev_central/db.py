import sqlite3
from typing import Dict
from charging_point import EV_CP, EstadoCP

DB_PATH = "EV_CP.db"

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
        conn.commit()
        conn.close()

    def save_charching_points(self, punto: EV_CP):
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        cursor.execute(
            "INSERT OR IGNORE INTO puntos_carga (id, location, price) VALUES (?, ?, ?)",
            (punto.id, punto.location, punto.price)
        )
        conn.commit()
        conn.close()

    def load_charching_points(self) -> Dict[str, EV_CP]:
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        cursor.execute("SELECT id, location, price FROM puntos_carga")
        rows = cursor.fetchall()
        conn.close()
        puntos = {}
        for row in rows:
            puntos[row[0]] = EV_CP(row[0], row[1], row[2], estado=EstadoCP.DESCONECTADO)
        return puntos
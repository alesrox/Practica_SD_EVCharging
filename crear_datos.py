import requests

# URL de la Base de Datos (Puerto 9000, según tu server.py)
DB_URL = "http://localhost:9000/charging_point/save"

# Datos adaptados a tu CREATE TABLE
payload = {
    "id": "MAD1",                 # TEXT PRIMARY KEY
    "location": "Madrid",        # TEXT NOT NULL
    "price": 0.30,               # REAL NOT NULL
    "estado": "ACTIVADO",        # TEXT CHECK (...) -> Importante: 'ACTIVADO' para que ev_w lo lea
    "driver": None,              # TEXT (FK) -> Ponemos None para evitar error de clave foránea si no hay drivers
    "kwh": 0.0,                  # REAL
    "time": None,                # REAL
    "token": "token_prueba_123"  # TEXT
}

try:
    print(f"📡 Insertando CP de prueba en: {DB_URL}")
    response = requests.post(DB_URL, json=payload)
    
    if response.status_code == 200:
        print("✅ ¡CP1 Creado correctamente!")
        print("   Estado: ACTIVADO (Listo para que ev_w lo detecte)")
    else:
        print(f"❌ Error {response.status_code}:")
        print(response.text)

except Exception as e:
    print(f"❌ Error de conexión: {e}")
    print("   ASEGÚRATE DE QUE LA TERMINAL 1 (db.server) ESTÁ CORRIENDO EN EL PUERTO 9000")
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI
import uvicorn

from db import EVCentralAPI

app = FastAPI(title="api_central")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------
# Configuración BD
# -----------------------------
DB_IP = "localhost" # if len(sys.argv) != 2 else sys.argv[1]
DB_URL = f"http://{DB_IP}:9000"

DB = EVCentralAPI(DB_URL)

@app.get("/charging_points")
def get_all_cps():
    return DB.load_charging_points()

@app.get("/charging_point/{cp_id}")
def get_cp(cp_id: str):
    return DB.get_charging_point(cp_id)

@app.get("/pause/{cp_id}")
def pause_cp(cp_id: str):
    DB.update_estado(cp_id, "PARADO")

@app.get("/unpause/{cp_id}")
def unpause_cp(cp_id: str):
    DB.update_estado(cp_id, "DESCONECTADO")

@app.get("/revoke/{cp_id}")
def revoke_cp(cp_id: str):
    cp = DB.get_charging_point(cp_id)
    cp["token"] = None
    DB.save_charging_point(cp)

# TODO: Añadir certificación
if __name__ == "__main__":
    uvicorn.run(
        "api_central:app", 
        host="0.0.0.0", 
        port=7500, 
        reload=True,
        ssl_keyfile="certs/api-central/clave_privada_servidor.pem",
        ssl_certfile="certs/api-central/certificado_servidor.crt"
    )
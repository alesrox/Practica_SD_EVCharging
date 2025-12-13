from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import uvicorn

from db import EVCentralAPI

app = FastAPI(title="api_central")

# -----------------------------
# Configuración BD
# -----------------------------
DB_IP = "localhost" # if len(sys.argv) != 2 else sys.argv[1]
DB_URL = f"http://{DB_IP}:9000"

DB = EVCentralAPI(DB_URL)

@app.get("/charging_points")
def get_all_cps():
    return DB.load_charging_points()

@app.get("/pause/{cp_id}")
def get_all_cps(cp_id: str):
    DB.update_estado(cp_id, "PARADO")

# TODO: Añadir certificación
if __name__ == "__main__":
    uvicorn.run(
        "api_central:app", 
        host="0.0.0.0", 
        port=7500, 
        reload=True
    )
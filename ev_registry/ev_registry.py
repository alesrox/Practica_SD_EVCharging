from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import uvicorn
import requests
import secrets
import sys
import json

app = FastAPI(title="EV_Registry")

# -----------------------------
# Configuración BD
# -----------------------------
BD_IP = "localhost" if len(sys.argv) != 2 else sys.argv[1]
BD_URL = f"http://{BD_IP}:9000"

# -----------------------------
# Modelos y Funciones auxiliares
# -----------------------------
class CPRegistration(BaseModel):
    cp_id: str
    location: str
    price: float

def generate_keys_for_cp():
    return {
        "auth_key": secrets.token_hex(32),
        "session_key": secrets.token_hex(32)
    }

# -----------------------------
# Endpoints
# -----------------------------
@app.put("/alta")
def alta_cp(data: CPRegistration):
    cp_id = data.cp_id
    location = data.location
    price = data.price

    keys = generate_keys_for_cp()

    payload = {
        "id": cp_id,
        "location": location,
        "price": price,
        "estado": "DESCONECTADO",
        "driver": None,
        "kwh": 0,
        "time": None,
        "auth_key": keys["auth_key"],
        "session_key": keys["session_key"]
    }

    try:
        r = requests.post(f"{BD_URL}/charging_point/save", json=payload)
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(f"Error HTTP: {e}")
        print("-" * 30)
        print("LO QUE DICE EL SERVIDOR (El error real):")
        # Esto imprimirá exactamente qué campo falta o está mal
        print(json.dumps(r.json(), indent=2)) 
        print("-" * 30)
        raise e

    return JSONResponse({
        "status": "CP registrado correctamente",
        "keys_for_EV_Central": keys
    })


@app.delete("/baja/{cp_id}")
def baja_cp(cp_id: str):
    res = requests.get(f"{BD_URL}/charging_point/{cp_id}")
    if res.status_code != 200:
        raise HTTPException(status_code=404, detail="CP no encontrado")

    r = requests.delete(f"{BD_URL}/charging_point/{cp_id}")
    if r.status_code != 200:
        raise HTTPException(status_code=500, detail="Error al eliminar en BD")

    return JSONResponse({"status": "CP dado de baja correctamente"})


# -----------------------------
# Inicio del servidor
# -----------------------------
if __name__ == "__main__":
    uvicorn.run(
        "ev_registry:app", 
        host="0.0.0.0", 
        port=8000, 
        reload=True,
        ssl_keyfile="registry_key.pem",
        ssl_certfile="resgistry_cert.pem"
    )


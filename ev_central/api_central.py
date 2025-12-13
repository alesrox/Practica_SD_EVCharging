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

"""
# --- 1. MODELO DE DATOS PARA LA ALERTA ---
class WeatherAlert(BaseModel):
    cp_id: str
    temp: float
    ciudad: str

# --- 2. ENDPOINT PARA COMUNICACIÓN CON EV_W ---
@app.post("/api/weather-alert")
def receive_weather_alert(alert: WeatherAlert):
    # print(f" Alerta recibida de EV_W | CP: {alert.cp_id} | Temp: {alert.temp}ºC")
    
    try:
        # 1. Recuperamos el estado actual
        cp_actual = DB.get_charging_point(alert.cp_id)
        if not cp_actual:
            return {"status": "ignored", "detail": "CP not found"}

        estado_actual = cp_actual.get("estado")

        # --- LÓGICA DE SEGURIDAD UNIDIRECCIONAL ---
        
        # SOLO actuamos si hace FRÍO (< 0ºC)
        if alert.temp < 0:
            # Si está funcionando o listo para funcionar, LO PARAMOS
            if estado_actual in ["ACTIVADO", "SUMINISTRANDO"]:
                # print(f"ALERTA CRÍTICA: Parando {alert.cp_id} por congelación ({alert.temp}ºC)")
                DB.update_estado(alert.cp_id, "PARADO")
                return {"status": "STOPPED", "reason": "Low temperature"}
            
            # Si ya estaba parado, averiado, etc., no hacemos nada
            return {"status": "no_change", "reason": f"Already {estado_actual}"}

        # SI HACE BUENO (>= 0ºC)
        # NO HACEMOS NADA. El CP se queda como esté.
        # Si se paró por frío, se queda parado hasta que alguien lo revise/active.
        else:
            return {"status": "ok", "action": "none"}

    except Exception as e:
        print(f" Error en gestión de alerta: {e}")
        raise HTTPException(status_code=500, detail=str(e))
"""

# TODO: Añadir certificación
if __name__ == "__main__":
    uvicorn.run(
        "api_central:app", 
        host="0.0.0.0", 
        port=7500, 
        reload=True
    )
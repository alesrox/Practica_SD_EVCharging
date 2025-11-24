from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional, Dict, Any
from db.database import DataBase
from db.charging_point import EV_CP, EstadoCP

app = FastAPI()
db = DataBase()

# --- Helpers ---
def cp_to_dict(cp: EV_CP) -> Dict[str, Any]:
    return {
        "id": cp.id,
        "location": cp.location,
        "price": cp.price,
        "estado": cp.estado.value,
        "driver": cp.driver,
        "kwh": cp.kwh,
        "time": cp.time,
        "auth_key": cp.auth_key,
        "session_key": cp.session_key
    }

# --- Pydantic Models ---
class ChargingPointModel(BaseModel):
    id: str
    location: str
    price: float
    estado: str
    driver: Optional[str] = None
    kwh: Optional[float] = 0
    time: Optional[float] = None

class EstadoModel(BaseModel):
    cp_id: str
    estado: str

class DriverModel(BaseModel):
    id: str
    location: str

class SetDriverModel(BaseModel):
    cp_id: str
    driver: Optional[str] = None

class KwhModel(BaseModel):
    cp_id: str
    kwh: float

class StartTimeModel(BaseModel):
    cp_id: str
    time: Optional[float] = None

class TicketModel(BaseModel):
    driver: Optional[str] = None
    cp_id: str
    total: float

# --- Endpoints ---
@app.get("/charging_points")
def get_all_charging_points():
    puntos = db.load_charging_points()
    return {cp_id: cp_to_dict(cp) for cp_id, cp in puntos.items()}

@app.get("/charging_point/{cp_id}")
def get_charging_point(cp_id: str):
    cp = db.get_charging_point(cp_id)
    if cp is None:
        raise HTTPException(status_code=404, detail="Charging point not found")
    return cp_to_dict(cp)

@app.post("/charging_point/save")
def save_charging_point(data: ChargingPointModel):
    punto = EV_CP(
        id=data.id,
        location=data.location,
        price=data.price,
        estado=EstadoCP(data.estado),
        driver=data.driver,
        kwh=data.kwh,
        ticket=0
    )
    punto.time = data.time
    db.save_charging_points(punto)
    return {"ok": True}

@app.post("/charging_points/reset")
def reset_all():
    db.reset_all_charging_points()
    return {"ok": True}

@app.post("/charging_point/estado")
def update_estado(data: EstadoModel):
    db.update_estado(data.cp_id, EstadoCP(data.estado))
    return {"ok": True}

@app.post("/charging_point/set_driver")
def set_driver(data: SetDriverModel):
    db.set_driver(data.cp_id, data.driver)
    return {"ok": True}

@app.post("/charging_point/kwh")
def update_kwh(data: KwhModel):
    db.update_kwh(data.cp_id, data.kwh)
    return {"ok": True}

@app.post("/charging_point/start_time")
def set_start_time(data: StartTimeModel):
    db.start_time(data.cp_id, data.time)
    return {"ok": True}

@app.post("/driver/save")
def save_driver(data: DriverModel):
    db.save_driver(data.id, data.location)
    return {"ok": True}

@app.get("/drivers")
def load_drivers():
    return db.load_drivers()

@app.post("/ticket/save")
def save_ticket(data: TicketModel):
    db.guardar_ticket(data.driver, data.cp_id, data.total)
    return {"ok": True}

@app.get("/tickets/{driver_id}")
def tickets_by_driver(driver_id: str):
    tickets = db.get_tickets_by_driver(driver_id)
    return [
        {"fecha": t[0], "punto_carga": t[1], "total": t[2]}
        for t in tickets
    ]
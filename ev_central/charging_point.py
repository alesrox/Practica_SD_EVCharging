from enum import Enum

class EstadoCP(Enum):
    ACTIVADO = "ACTIVADO"
    PARADO = "PARADO"
    SUMINISTRANDO = "SUMINISTRANDO"
    AVERIADO = "AVERIADO"
    DESCONECTADO = "DESCONECTADO"

class EV_CP:
    def __init__(
            self, id: str, 
            location: str, price: float, 
            estado: EstadoCP = EstadoCP.DESCONECTADO,
            driver: str = None, kwh: float = 0,
            ticket: float = 0, auth_key: str = None, session_key: str = None
    ):
        self.id = id
        self.location = location
        self.price = price
        self.estado = estado
        self.driver = driver
        self.kwh = kwh
        self.ticket = ticket
        self.time = None
        self.auth_key = auth_key
        self.session_key = session_key

    def can_supply(self) -> bool:
        return self.estado == EstadoCP.ACTIVADO
    
    def as_dict(self) -> dict:
        return {
            "id": self.id,
            "location": self.location,
            "price": self.price,
            "estado": self.estado.value,
            "driver": self.driver,
            "kwh": self.kwh,
            "ticket": self.ticket,
            "time": self.time,
            "auth_key": self.auth_key,
            "session_key": self.session_key
        }
    

def dict_to_ev_cp(data) -> EV_CP:
    cp = EV_CP(
        id=data["id"],
        location=data["location"],
        price=data["price"],
        estado=EstadoCP(data["estado"]),
        driver=data.get("driver"),
        kwh=data.get("kwh", 0),
        ticket=data.get("ticket", 0),
        auth_key=data.get("auth_key"),
        session_key=data.get("session_key")
    )

    cp.time = data.get("time")
    return cp
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
            ticket: float = 0,
    ):
        self.id = id
        self.location = location
        self.price = price
        self.estado = estado
        self.driver = driver
        self.kwh = kwh
        self.ticket = ticket
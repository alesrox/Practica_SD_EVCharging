from enum import Enum

class EstadoCP(Enum):
    ACTIVADO = "ACTIVADO"
    PARADO = "PARADO"
    SUMINISTRANDO = "SUMINISTRANDO"
    AVERIADO = "AVERIADO"
    DESCONECTADO = "DESCONECTADO"

class EV_CP:
    def __init__(
            self, identificador: str, ubicacion: str, estado: EstadoCP = EstadoCP.DESCONECTADO
    ):
        self.id = identificador
        self.ubicacion = ubicacion
        self.estado = estado
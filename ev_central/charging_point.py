from enum import Enum

class EstadoCP(Enum):
    ACTIVADO = "Activado"
    PARADO = "Parado"
    SUMINISTRANDO = "Suministrando"
    AVERIADO = "Averiado"
    DESCONECTADO = "Desconectado"

class EV_CP:
    def __init__(
            self, identificador: str, ubicacion: str, estado: EstadoCP = EstadoCP.DESCONECTADO
    ):
        self.id = identificador
        self.ubicacion = ubicacion
        self.estado = estado
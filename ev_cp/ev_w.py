import asyncio
import logging
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from typing import Dict, Optional
import threading

# Framework del servidor
from fastapi import FastAPI, HTTPException, BackgroundTasks

# Validación de datos (Esencial en FastAPI)
from pydantic import BaseModel

# Cliente HTTP Asíncrono (El sustituto de requests para FastAPI)
import httpx

# Bloqueo para acceso seguro a datos compartidos
data_lock = threading.Lock()

# Diccionario para almacenar las asociaciones de Charging Points y ciudades
ciudades_cp: Dict[str, str] = {}

def cambiar_ciudad():
    print("-- Menú de control de ciudades de Charging Points --")

    charging_point = input("Introduce el ID del Charging Point que deseas modificar: ")
    nueva_ciudad = input("Introduce la nueva ciudad para el Charging Point: ")

    if charging_point in ciudades_cp:
        with data_lock:
            ciudades_cp[charging_point] = nueva_ciudad
        print(f"Ciudad del Charging Point {charging_point} actualizada a {nueva_ciudad}.")
    else:
        print(f"Charging Point {charging_point} no registrado, registe la asociación.")

def anadir_asoc(cp: str, ciudad: str):
    with data_lock:
        ciudades_cp[cp] = ciudad

def cargar_ciudades_de_txt():
    try:
        with open("../ciudades_cp.txt", "r") as archivo:
            for linea in archivo:
                cp, ciudad = linea.strip().split(" = ")
                anadir_asoc(cp, ciudad)
        print("Ciudades de Charging Points cargadas correctamente.")
    except FileNotFoundError:
        pass

def menu():
    print("\n--- Menú de Control de Ciudades de Charging Points ---")
    print("1. Cambiar ciudad de un Charging Point")
    print("2. Añadir nueva asociación de Charging Point y ciudad")
    print("3. Salir")

    opcion = input("Selecciona una opción: ")

    if opcion == "1":
        cambiar_ciudad()
    elif opcion == "2":
        cp = input("Introduce el ID del nuevo Charging Point: ")
        ciudad = input("Introduce la ciudad asociada: ")
        anadir_asoc(cp, ciudad)
        print(f"Asociación añadida: {cp} -> {ciudad}")
    elif opcion == "3":
        return False
    else:
        print("Opción no válida. Por favor, intenta de nuevo.")

    return True

if __name__ == "__main__":

    cargar_ciudades_de_txt() # carga ciudades desde el txt, sino existe, no hace nada

    continua : bool = True
    while continua:
        continua = menu()
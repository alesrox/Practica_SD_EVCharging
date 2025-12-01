import os
from dotenv import load_dotenv

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

# Caché para almacenar datos meteorológicos y evitar llamadas repetidas
weather_cache: Dict[str, str] = {}

async def obtener_grados(ciudad: str, api_key: str, api_url: str) -> Optional[float]:
    params = {
        "q": ciudad,
        "appid": api_key,
        "units": "metric"
    }
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(api_url, params=params)
            response.raise_for_status()
            data = response.json()
            return data["main"]["temp"]
        except httpx.HTTPError as e:
            logging.error(f"Error al obtener datos meteorológicos para {ciudad}: {e}")
            return None
        
async def bucle_clima(env_vars: Dict[str, str]):
    api_key = env_vars['WEATHER_API_KEY']
    api_url = env_vars['WEATHER_API_URL']

    print("\n-- Iniciando bucle de actualización meteorológica --")

    ciudades = []
    
    while True:
        with data_lock:
            ciudades = list(set(ciudades_cp.values())) 
        
        tareas = [obtener_grados(ciudad, api_key, api_url) for ciudad in ciudades]

        # Ejecuta todas las tareas a la vez
        resultados = await asyncio.gather(*tareas) # Espera a que todas las tareas se completen

        # Actualiza la caché con los nuevos datos
        with data_lock:
            for ciudad, grados in zip(ciudades, resultados):
                if grados is not None:
                    weather_cache[ciudad] = grados

        await asyncio.sleep(4)  # Espera 4 segundos antes de la siguiente actualización


def cambiar_ciudad():
    print("\n-- Menú de cambio de ciudades de Charging Points --")

    charging_point = input("Introduce el ID del Charging Point que deseas modificar: ")
    nueva_ciudad = input("Introduce la nueva ciudad para el Charging Point: ")

    if charging_point in ciudades_cp:
        with data_lock:
            ciudades_cp[charging_point] = nueva_ciudad
        print(f"\nCiudad del Charging Point {charging_point} actualizada a {nueva_ciudad}.")
    else:
        print(f"Charging Point {charging_point} no registrado, registe la asociación.")

def anadir_asoc(cp: str, ciudad: str):
    if cp in ciudades_cp:
        return False
    
    with data_lock:
        ciudades_cp[cp] = ciudad
    return True

def cargar_ciudades_de_txt():
    try:
        carpeta_actual = os.path.dirname(__file__)
        ruta_txt = os.path.join(carpeta_actual, "..", "ciudades_cp.txt")
        
        # Abre el archivo usando la ruta absoluta calculada
        with open(ruta_txt, "r") as archivo:
            for linea in archivo:
                cp, ciudad = linea.strip().split(" = ")
                anadir_asoc(cp.strip(), ciudad.strip())
        print(f"Ciudades cargadas correctamente")

    except FileNotFoundError:
        print(f"Error: No se encuentra el archivo.")

def listar_asociaciones():
    print("\nAsociaciones actuales de Charging Points y ciudades:")
    with data_lock:
        for cp, ciudad in ciudades_cp.items():
            print(f"{cp} -> {ciudad}")

def get_env():
    env_vars = {}
    try:
        load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'))
        env_vars = {
            'WEATHER_API_KEY': os.getenv('WEATHER_API_KEY'),
            'WEATHER_API_URL': os.getenv('WEATHER_API_URL')
        }
        if not all(env_vars.values()):
            print("Faltan variables de entorno necesarias.")
            exit(1)
    except Exception as e:
        print(f"Error loading environment variables: {e}")
        exit(1)

    # Ahora la ip de central
    try:
        load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))
        env_vars.update( {
            'CENTRAL_IP': os.getenv('HOST_IP'),
        })
        if not env_vars['CENTRAL_IP']:
            print("Falta la variable de entorno CENTAL_IP.")
            exit(1)
    except Exception as e:
        print(f"Error loading central environment variables: {e}")
        exit(1)
    return env_vars

#############################################################################


def menu():
    print("\n--- Menú de Control de Ciudades de Charging Points ---")
    print("1. Cambiar ciudad de un Charging Point")
    print("2. Añadir nueva asociación de Charging Point y ciudad")
    print("3. Listar asociaciones actuales")
    print("4. Salir")

    opcion = input("Selecciona una opción: ")

    match opcion:
        case "1":
            cambiar_ciudad()
        case "2":
            cp = input("Introduce el ID del nuevo Charging Point: ")
            ciudad = input("Introduce la ciudad asociada: ")
            check : bool = anadir_asoc(cp, ciudad)
            if check:
                print(f"Asociación añadida: {cp} -> {ciudad}")
            else:
                print(f"La asociación para el Charging Point {cp} ya está registrada.")
        case "3":
            listar_asociaciones()
        case "4":
            return False
        case _:
            print("Opción no válida. Por favor, intenta de nuevo.")

    return True

if __name__ == "__main__":

    variables_entorno = get_env()
    cargar_ciudades_de_txt() # carga ciudades desde el txt, sino existe, no hace nada

    # Inicia el bucle de actualización meteorológica en segundo plano
    def iniciar_bucle_clima():
        asyncio.run(bucle_clima(variables_entorno))
    threading.Thread(target=iniciar_bucle_clima, daemon=True).start()

    continua : bool = True
    while continua:
        continua = menu()
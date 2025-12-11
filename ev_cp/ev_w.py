import os
import sys

# --- TRUCO PARA IMPORTAR DESDE LA CARPETA RAÍZ ---
# Obtenemos la ruta de la carpeta superior (..)
ruta_raiz = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
# La añadimos al "path" de Python para poder hacer imports
sys.path.append(ruta_raiz)
# AHORA SÍ PODEMOS IMPORTAR EL MÓDULO QUE ESTÁ FUERA
try:
    from descifrar_certf import obtener_secreto
except ImportError:
    print("Error: No encuentro 'descifrar_certf.py' en la carpeta raíz.")
    exit(1)

import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
from dotenv import load_dotenv

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, Optional
import threading

# Framework del servidor
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
import httpx

# Bloqueo para acceso seguro a datos compartidos
data_lock = threading.Lock()

#Nombre certificado API Key
CERT_API_NAME = "API_OpenWeather.pem"

# Datos compartidos
ciudades_cp: Dict[str, str] = {}
weather_cache: Dict[str, dict] = {} # Ajustado type hint
CACHE_DURATION = timedelta(minutes=1)

# --- Backend Asíncrono ---

async def obtener_grados(ciudad: str, api_key: str, api_url: str) -> Optional[float]:
    now = datetime.now()
    
    # Check Caché
    if ciudad in weather_cache:
        d = weather_cache[ciudad]
        if now - d['time'] < CACHE_DURATION:
            return d['temp']

    params = {"q": ciudad, "appid": api_key, "units": "metric"}
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(api_url, params=params)
            response.raise_for_status()
            data = response.json()
            t = data["main"]["temp"]
            # Guardamos temp y hora
            weather_cache[ciudad] = {"temp": t, "time": now} 
            return t
        except Exception:
            return None

async def notificar_central(ciudad: str, temp: float, central_ip: str, gui_app=None):
    pass

# --- BUCLE PRINCIPAL ASÍNCRONO --- (Consulta clima y notifica cada 4 segundos)
async def bucle_clima(env_vars: Dict[str, str], gui_app):
    """
    Recibe gui_app para pasárselo a las notificaciones
    """
    api_key = env_vars['WEATHER_API_KEY']
    api_url = env_vars['WEATHER_API_URL']
    central_ip = env_vars['CENTRAL_IP']

    # Mensaje inicial en la ventana
    gui_app.agregar_log("Motor de clima iniciado en segundo plano...", "sistema")

    while True:
        ciudades = []
        with data_lock:
            ciudades = list(set(ciudades_cp.values())) 
        
        if ciudades:
            tareas = [obtener_grados(ciudad, api_key, api_url) for ciudad in ciudades]
            resultados = await asyncio.gather(*tareas)

            tareas_notif = []
            with data_lock:
                for ciudad, grados in zip(ciudades, resultados):
                    if grados is not None:
                        # Pasamos gui_app aquí
                        tareas_notif.append(notificar_central(ciudad, grados, central_ip, gui_app))
            
            if tareas_notif:
                await asyncio.gather(*tareas_notif)

        await asyncio.sleep(4)

# --- GESTIÓN DE ARCHIVOS ---
def cargar_ciudades_de_txt():
    try:
        carpeta_actual = os.path.dirname(__file__)
        ruta_txt = os.path.join(carpeta_actual, "..", "ciudades_cp.txt")
        with open(ruta_txt, "r") as archivo:
            for linea in archivo:
                if "=" in linea:
                    cp, ciudad = linea.strip().split("=")
                    with data_lock:
                        ciudades_cp[cp.strip()] = ciudad.strip()
    except FileNotFoundError:
        pass

def get_api():
    """
    Busca el certificado en la raíz, pide contraseña y devuelve la API Key desencriptada.
    """
    carpeta_actual = os.path.dirname(__file__)
    # La ruta cambia aquí porque el p12 está en la raíz (..)
    ruta_p12 = os.path.join(carpeta_actual, "..", CERT_API_NAME)

    if not os.path.exists(ruta_p12):
        print(f"Error crítico: No encuentro '{CERT_API_NAME}' en: {ruta_p12}")
        print("Asegúrate de haber ejecutado el generador en la carpeta raíz.")
        exit(1)

    try:
        # Llamamos a tu módulo importado de la raíz
        api_key = obtener_secreto(ruta_p12)
        return api_key
    except Exception as e:
        print(f"Error con el certificado: {e}")
        exit(1)

def get_env():
    env_vars = {}
    try:
        env_vars['WEATHER_API_KEY'] = get_api()

        load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'))
        env_vars['WEATHER_API_URL'] = os.getenv('WEATHER_API_URL')
        load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))
        env_vars['CENTRAL_IP'] = os.getenv('HOST_IP')
        return env_vars
    except Exception:
        exit(1)

# --- INTERFAZ GRÁFICA ---

class WeatherAppGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Weather Control Office")
        self.root.geometry("700x550")

        # 1. Panel de Gestión (Arriba)
        frame_input = tk.LabelFrame(self.root, text="Gestión Manual", padx=10, pady=10)
        frame_input.pack(fill="x", padx=10, pady=5)

        tk.Label(frame_input, text="ID CP:").grid(row=0, column=0, padx=5)
        self.entry_cp = tk.Entry(frame_input, width=10)
        self.entry_cp.grid(row=0, column=1, padx=5)

        tk.Label(frame_input, text="Ciudad:").grid(row=0, column=2, padx=5)
        self.entry_ciudad = tk.Entry(frame_input, width=15)
        self.entry_ciudad.grid(row=0, column=3, padx=5)

        tk.Button(frame_input, text="Guardar", command=self.guardar_asociacion, bg="#4CAF50", fg="white").grid(row=0, column=4, padx=10)

        # 2. Tabla de Estado (Centro)
        frame_tabla = tk.LabelFrame(self.root, text="Estado Actual", padx=10, pady=10)
        frame_tabla.pack(fill="both", expand=True, padx=10, pady=5)

        self.tree = ttk.Treeview(frame_tabla, columns=("cp", "ciudad", "temp"), show="headings", height=6)
        self.tree.heading("cp", text="ID CP")
        self.tree.heading("ciudad", text="Ciudad")
        self.tree.heading("temp", text="Temp (ºC)")
        self.tree.pack(fill="both", expand=True)

        # 3. Consola de Logs (Abajo)
        frame_log = tk.LabelFrame(self.root, text="Logs de API (Tiempo Real)", padx=10, pady=10)
        frame_log.pack(fill="both", expand=True, padx=10, pady=5)

        self.console_log = scrolledtext.ScrolledText(frame_log, height=10, state='disabled', bg="black", fg="#00FF00", font=("Consolas", 9))
        self.console_log.pack(fill="both", expand=True)
        
        # Colores para los logs
        self.console_log.tag_config("alerta", foreground="red")
        self.console_log.tag_config("info", foreground="#00FF00") # Verde
        self.console_log.tag_config("error", foreground="yellow")
        self.console_log.tag_config("sistema", foreground="cyan")

        # Refresco automático
        self.actualizar_tabla()

    def guardar_asociacion(self):
        cp = self.entry_cp.get().strip()
        ciudad = self.entry_ciudad.get().strip()
        if cp and ciudad:
            with data_lock:
                ciudades_cp[cp] = ciudad
            self.agregar_log(f"✏️ GESTIÓN: Asociado {cp} -> {ciudad}", "sistema")
            self.entry_cp.delete(0, tk.END)
            self.entry_ciudad.delete(0, tk.END)

    def agregar_log(self, mensaje, etiqueta):
        """
        Esta función es llamada desde el HILO ASÍNCRONO.
        Usamos root.after para que sea seguro pintar en la ventana.
        """
        def _pintar():
            self.console_log.config(state='normal') # Desbloquear para escribir
            self.console_log.insert(tk.END, mensaje + "\n", etiqueta)
            self.console_log.see(tk.END)            # Auto-scroll al final
            self.console_log.config(state='disabled') # Bloquear para que no borres
        
        self.root.after(0, _pintar)

    def actualizar_tabla(self):
        # Limpiar y repintar tabla
        for item in self.tree.get_children():
            self.tree.delete(item)
        
        with data_lock:
            mis_datos = ciudades_cp.copy()
            mi_cache = weather_cache.copy()

        for cp, ciudad in mis_datos.items():
            dato = mi_cache.get(ciudad)
            temp_str = f"{dato['temp']}ºC" if dato else "..."
            self.tree.insert("", tk.END, values=(cp, ciudad, temp_str))
        
        self.root.after(1000, self.actualizar_tabla)

# --- MAIN ---

if __name__ == "__main__":
    variables = get_env()
    cargar_ciudades_de_txt()

    # Arrancamos la ventana
    root = tk.Tk()
    app = WeatherAppGUI(root)

    # Inyectamos la 'app' en el bucle para que pueda escribir logs
    def iniciar_motor():
        asyncio.run(bucle_clima(variables, app))
    
    hilo = threading.Thread(target=iniciar_motor, daemon=True)
    hilo.start()

    root.mainloop()
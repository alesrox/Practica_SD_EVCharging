import os
import sys

# --- TRUCO PARA IMPORTAR DESDE LA CARPETA RAÍZ ---
ruta_raiz = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(ruta_raiz)
try:
    from descifrar_certf import obtener_secreto
except ImportError:
    print("Error: No encuentro 'descifrar_certf.py' en la carpeta raíz.")
    exit(1)

import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
from dotenv import load_dotenv

import asyncio
from datetime import datetime, timedelta
from typing import Dict, Optional
import threading
import httpx

# Bloqueo para acceso seguro a datos compartidos
data_lock = threading.Lock()

# --- CONFIGURACIÓN DE CERTIFICADOS ---
# Solo usamos esto para sacar la API Key de OpenWeather localmente
CERT_API_NAME = "API_OpenWeather.pem" 
NOMBRE_CERTIFICADO = "certServ.pem"

# --- DATOS COMPARTIDOS ---
ciudades_cp: Dict[str, str] = {}
weather_cache: Dict[str, dict] = {} 
CACHE_DURATION = timedelta(minutes=1)

# CACHÉ DE ESTADOS (Polling)
estados_cps_cache: Dict[str, str] = {} 

# --- FUNCIONES AUXILIARES ---

async def obtener_grados(ciudad: str, api_key: str, api_url: str) -> Optional[float]:
    now = datetime.now()
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
            weather_cache[ciudad] = {"temp": t, "time": now} 
            return t
        except Exception:
            return None

# --- TAREA DE FONDO: ACTUALIZAR ESTADOS (HTTP SIN SSL) ---
async def bucle_actualizar_estados(central_ip: str, gui_app=None):

    url = f"http://{central_ip}:7500/charging_points"
    
    if gui_app: gui_app.agregar_log("🔄 Sincronizando estados (HTTP)...", "sistema")

    while True:
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(url)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    with data_lock:
                        estados_cps_cache.clear()
                        for cp_id, cp_data in data.items():
                            estados_cps_cache[cp_id] = cp_data.get("estado", "DESCONECTADO")

        except httpx.ConnectError:
            with data_lock: estados_cps_cache.clear()
        except Exception as e:
            print(f"Error polling estados: {e}")

        await asyncio.sleep(1)



# --- COMUNICACIÓN CON API CENTRAL (HTTP SIN SSL) ---
async def notificar_central(ciudad: str, temp: float, central_ip: str, gui_app=None):
    cps_en_ciudad = []
    with data_lock:
        for cp_id, ciudad_cp in ciudades_cp.items():
            if ciudad_cp.lower() == ciudad.lower():
                cps_en_ciudad.append(cp_id)

    if not cps_en_ciudad:
        return

    # Si hace frío (< 0ºC)
    if temp < 0:
        # CAMBIO 3: Cliente sin certificado
        async with httpx.AsyncClient() as client:
            for cp in cps_en_ciudad:
                
                # 1. Chequeo de caché
                with data_lock:
                    estado_actual = estados_cps_cache.get(cp)

                # 2. Filtro: Solo paramos si está funcionando
                if not estado_actual or estado_actual not in ["ACTIVADO", "SUMINISTRANDO"]:
                    continue

                url = f"http://{central_ip}:7500/pause/{cp}"
                try:
                    await client.get(url)
                    
                    if gui_app:
                        gui_app.agregar_log(f"❄️ ALERTA ({temp}ºC): {cp} ({estado_actual}) -> PAUSADO", "alerta")
                        
                        # Actualización optimista local
                        with data_lock:
                            estados_cps_cache[cp] = "PARADO"

                except Exception as e:
                    if gui_app: gui_app.agregar_log(f"❌ Error al pausar {cp}: {e}", "error")

# --- BUCLE CLIMA ---
async def bucle_clima(env_vars: Dict[str, str], gui_app):
    api_key = env_vars['WEATHER_API_KEY']
    api_url = env_vars['WEATHER_API_URL']
    central_ip = env_vars['CENTRAL_IP']

    gui_app.agregar_log("Motor clima ON (Modo HTTP)", "sistema")

    while True:
        ciudades = []
        with data_lock:
            ciudades = list(set(ciudades_cp.values())) 
        
        if ciudades:
            tareas = [obtener_grados(ciudad, api_key, api_url) for ciudad in ciudades]
            resultados = await asyncio.gather(*tareas)

            tareas_notif = []
            for ciudad, grados in zip(ciudades, resultados):
                if grados is not None:
                    tareas_notif.append(notificar_central(ciudad, grados, central_ip, gui_app))
            
            if tareas_notif:
                await asyncio.gather(*tareas_notif)

        await asyncio.sleep(4) # actiualización cada 4 segundos de temperatura

# --- COORDINADOR ---
async def main_async(env_vars, gui_app):
    central_ip = env_vars['CENTRAL_IP']
    
    t1 = asyncio.create_task(bucle_clima(env_vars, gui_app))
    t2 = asyncio.create_task(bucle_actualizar_estados(central_ip, gui_app))
    
    await asyncio.gather(t1, t2)

# --- BOILERPLATE ---
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
    # Mantenemos esto para leer la API KEY localmente del archivo
    carpeta_actual = os.path.dirname(__file__)
    ruta_pem = os.path.join(carpeta_actual, "..", CERT_API_NAME)
    if not os.path.exists(ruta_pem):
        ruta_pem = os.path.join(carpeta_actual, "..", NOMBRE_CERTIFICADO)
        if not os.path.exists(ruta_pem):
            print(f"Error crítico: No encuentro certificado en: {ruta_pem}")
            exit(1)
    try:
        return obtener_secreto(ruta_pem)
    except Exception as e:
        print(f"Error descifrando API Key: {e}")
        exit(1)

def get_env():
    env_vars = {}
    try:
        env_vars['WEATHER_API_KEY'] = get_api()
        load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'))
        env_vars['WEATHER_API_URL'] = os.getenv('WEATHER_API_URL')
        load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))
        env_vars['CENTRAL_IP'] = os.getenv('CENTRAL_IP')
        return env_vars
    except Exception:
        exit(1)

class WeatherAppGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Weather Control Office (EV_W) - HTTP MODE")
        self.root.geometry("700x550")

        frame_input = tk.LabelFrame(self.root, text="Gestión Manual", padx=10, pady=10)
        frame_input.pack(fill="x", padx=10, pady=5)
        tk.Label(frame_input, text="ID CP:").grid(row=0, column=0, padx=5)
        self.entry_cp = tk.Entry(frame_input, width=10)
        self.entry_cp.grid(row=0, column=1, padx=5)
        tk.Label(frame_input, text="Ciudad:").grid(row=0, column=2, padx=5)
        self.entry_ciudad = tk.Entry(frame_input, width=15)
        self.entry_ciudad.grid(row=0, column=3, padx=5)
        tk.Button(frame_input, text="Guardar", command=self.guardar_asociacion, bg="#4CAF50", fg="white").grid(row=0, column=4, padx=10)

        frame_tabla = tk.LabelFrame(self.root, text="Estado Actual", padx=10, pady=10)
        frame_tabla.pack(fill="both", expand=True, padx=10, pady=5)
        self.tree = ttk.Treeview(frame_tabla, columns=("cp", "ciudad", "temp"), show="headings", height=6)
        self.tree.heading("cp", text="ID CP")
        self.tree.heading("ciudad", text="Ciudad")
        self.tree.heading("temp", text="Temp (ºC)")
        self.tree.pack(fill="both", expand=True)

        frame_log = tk.LabelFrame(self.root, text="Logs de API", padx=10, pady=10)
        frame_log.pack(fill="both", expand=True, padx=10, pady=5)
        self.console_log = scrolledtext.ScrolledText(frame_log, height=10, state='disabled', bg="black", fg="#00FF00", font=("Consolas", 9))
        self.console_log.pack(fill="both", expand=True)
        
        self.console_log.tag_config("alerta", foreground="red")
        self.console_log.tag_config("info", foreground="#00FF00")
        self.console_log.tag_config("error", foreground="yellow")
        self.console_log.tag_config("sistema", foreground="cyan")
        self.actualizar_tabla()

    def guardar_asociacion(self):
        cp = self.entry_cp.get().strip()
        ciudad = self.entry_ciudad.get().strip()
        if cp and ciudad:
            with data_lock: ciudades_cp[cp] = ciudad
            self.agregar_log(f"✏️ Asociado {cp} -> {ciudad}", "sistema")
            self.entry_cp.delete(0, tk.END)
            self.entry_ciudad.delete(0, tk.END)

    def agregar_log(self, mensaje, etiqueta):
        def _pintar():
            self.console_log.config(state='normal')
            self.console_log.insert(tk.END, mensaje + "\n", etiqueta)
            self.console_log.see(tk.END)
            self.console_log.config(state='disabled')
        self.root.after(0, _pintar)

    def actualizar_tabla(self):
        for item in self.tree.get_children(): self.tree.delete(item)
        with data_lock:
            mis_datos = ciudades_cp.copy()
            mi_cache = weather_cache.copy()
        for cp, ciudad in mis_datos.items():
            dato = mi_cache.get(ciudad)
            temp_str = f"{dato['temp']}ºC" if dato else "..."
            self.tree.insert("", tk.END, values=(cp, ciudad, temp_str))
        self.root.after(1000, self.actualizar_tabla)

if __name__ == "__main__":
    variables = get_env()
    cargar_ciudades_de_txt()
    root = tk.Tk()
    app = WeatherAppGUI(root)
    
    def iniciar_motor(): 
        asyncio.run(main_async(variables, app))

    hilo = threading.Thread(target=iniciar_motor, daemon=True)
    hilo.start()
    
    root.mainloop()
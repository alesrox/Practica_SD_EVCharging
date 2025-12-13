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
import ssl 

# Bloqueo para acceso seguro a datos compartidos
data_lock = threading.Lock()

# --- CONFIGURACIÓN DE CERTIFICADOS ---
CERT_API_NAME = "API_OpenWeather.pem" # Para la API Key local

# --- CONFIGURACIÓN HTTPS ---
# Usamos el certificado de la Autoridad (CA) para validar al servidor
NOMBRE_CA = "certificado_CA.crt"

# Buscamos en la ruta: ../certs/api-central/certificado_CA.crt
RUTA_CA = os.path.abspath(os.path.join(
    os.path.dirname(__file__), "..", "certs", "api-central", NOMBRE_CA
))

# Si no está ahí, buscamos en la raíz por si acaso
if not os.path.exists(RUTA_CA):
    RUTA_CA = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", NOMBRE_CA))


# --- DATOS COMPARTIDOS ---
ciudades_cp: Dict[str, str] = {}
weather_cache: Dict[str, dict] = {} 
CACHE_DURATION = timedelta(minutes=1)
DEFAULT_CITY = "Madrid"

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

def auto_descubrir_cps(data_servidor: dict, gui_app=None):
    """
    Lógica de descubrimiento de nuevos CPs.
    """
    for cp_id, cp_data in data_servidor.items():
        es_conocido = False
        with data_lock:
            if cp_id in ciudades_cp:
                es_conocido = True
        
        if es_conocido:
            continue

        estado = cp_data.get("estado", "DESCONECTADO")
        ubicacion = cp_data.get("location")

        # CONDICIÓN: Tiene ubicación Y no está desconectado
        if ubicacion and estado not in ["DESCONECTADO"]:
            # Si empieza por "zone" (independiente de mayúsculas), asignamos Default
            if ubicacion.lower().startswith("zone"):
                with data_lock: ciudades_cp[cp_id] = DEFAULT_CITY
            else:
                with data_lock: ciudades_cp[cp_id] = ubicacion
            
            if gui_app: gui_app.agregar_log(f"🔎 Auto-Add: {cp_id} en {ciudades_cp.get(cp_id)}", "sistema")
        
        elif not ubicacion and estado not in ["DESCONECTADO"]:
            with data_lock: ciudades_cp[cp_id] = DEFAULT_CITY

# --- TAREA DE FONDO: ACTUALIZAR ESTADOS (HTTPS) ---
async def bucle_actualizar_estados(central_ip: str, gui_app=None):

    # 1. Ajuste de seguridad para la IP
    if not central_ip or central_ip == "0.0.0.0" or central_ip == "None":
        central_ip = "127.0.0.1"

    # 2. URL con HTTPS
    url = f"https://{central_ip}:7500/charging_points"
    
    # 3. Aviso visual si falta el cert CA
    if not os.path.exists(RUTA_CA):
        if gui_app: gui_app.agregar_log(f"⚠️ ERROR: No encuentro CA en {RUTA_CA}", "error")

    if gui_app: gui_app.agregar_log(f"🔄 Sincronizando (HTTPS) con {url}...", "sistema")

    while True:
        try:
            # 4. Cliente HTTPS Verificado
            # verify=False salta la comprobación de seguridad (SOLO PARA PROBAR)
            async with httpx.AsyncClient(verify=False) as client:
                response = await client.get(url)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    with data_lock:
                        estados_cps_cache.clear()
                        for cp_id, cp_data in data.items():
                            estados_cps_cache[cp_id] = cp_data.get("estado", "DESCONECTADO")
                    
                    # Llamada a la función de descubrimiento
                    auto_descubrir_cps(data, gui_app)

        except httpx.ConnectError:
            with data_lock: estados_cps_cache.clear()
            msg = f"No se puede conectar a API Central en {central_ip}:7500"
            print(msg)
            if gui_app: gui_app.agregar_log(msg, "error")
            # Quitamos log continuo para no saturar
        
        except httpx.ssl.SSLError as e:
            msg = f"Error SSL: {e}"
            print(msg)
            if gui_app: gui_app.agregar_log(msg, "error")
            
        except Exception as e:
            print(f"Error polling estados: {e}")

        await asyncio.sleep(2)


# --- COMUNICACIÓN CON API CENTRAL (HTTPS) ---
async def notificar_central(ciudad: str, temp: float, central_ip: str, gui_app=None):
    if not central_ip or central_ip == "0.0.0.0" or central_ip == "None":
        central_ip = "127.0.0.1"

    cps_en_ciudad = []
    with data_lock:
        for cp_id, ciudad_cp in ciudades_cp.items():
            if ciudad_cp.lower() == ciudad.lower():
                cps_en_ciudad.append(cp_id)

    if not cps_en_ciudad:
        return

    if temp < 0:
        # Cliente HTTPS para enviar PAUSE
        async with httpx.AsyncClient(verify=False) as client:
            for cp in cps_en_ciudad:
                with data_lock:
                    estado_actual = estados_cps_cache.get(cp)

                if not estado_actual or estado_actual not in ["ACTIVADO", "SUMINISTRANDO"]:
                    continue

                url = f"https://{central_ip}:7500/pause/{cp}"
                try:
                    await client.get(url)
                    
                    if gui_app:
                        gui_app.agregar_log(f"❄️ ALERTA ({temp}ºC): {cp} -> PAUSADO", "alerta")
                        with data_lock: estados_cps_cache[cp] = "PARADO"

                except httpx.ssl.SSLError as e:
                    if gui_app: gui_app.agregar_log(f"🔒 Error SSL Pause: {e}", "error")
                except Exception as e:
                    if gui_app: gui_app.agregar_log(f"❌ Error al pausar {cp}: {e}", "error")

# --- BUCLE CLIMA ---
async def bucle_clima(env_vars: Dict[str, str], gui_app):
    api_key = env_vars['WEATHER_API_KEY']
    api_url = env_vars['WEATHER_API_URL']
    central_ip = env_vars['CENTRAL_IP']

    gui_app.agregar_log(f"Motor clima ON (HTTPS). CA: {os.path.basename(RUTA_CA)}", "sistema")

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

        await asyncio.sleep(4)

# --- COORDINADOR ---
async def main_async(env_vars, gui_app):
    central_ip = env_vars['CENTRAL_IP']
    
    t1 = asyncio.create_task(bucle_clima(env_vars, gui_app))
    t2 = asyncio.create_task(bucle_actualizar_estados(central_ip, gui_app))
    
    await asyncio.gather(t1, t2)

# --- BOILERPLATE Y CARGA ENV ---
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
    carpeta_actual = os.path.dirname(__file__)
    # Buscamos el PEM para OpenWeather (no tiene que ver con HTTPS central)
    ruta_pem = os.path.join(carpeta_actual, "..", CERT_API_NAME)
    
    if not os.path.exists(ruta_pem):
         # Intento alternativo
         ruta_pem = os.path.join(carpeta_actual, "..", "API_OpenWeather.pem")

    if not os.path.exists(ruta_pem):
         print(f"Error crítico: No encuentro certificado para API Key en: {ruta_pem}")
         # Retornamos error string para que no explote aquí mismo si no quieres
         return "NO_CERT_FOUND"

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
        # Leemos IP
        ip = os.getenv('CENTRAL_IP')
        
        # Fallback a localhost si falla el .env (para evitar ConnectError directo)
        if not ip: 
            print("⚠️ AVISO: No IP en .env, saliendo...")
            exit(1)

        env_vars['CENTRAL_IP'] = ip
        return env_vars
    except Exception:
        exit(1)

class WeatherAppGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Weather Control Office (EV_W) - HTTPS MODE")
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
        cp = self.entry_cp.get().upper().strip()
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
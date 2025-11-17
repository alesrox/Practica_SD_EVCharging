import tkinter as tk
from datetime import datetime
from charging_point import EstadoCP

colores = {
    "ACTIVADO": "#2e7d32",
    "SUMINISTRANDO": "#2e7d32",
    "PARADO": "#ef6c00",
    "AVERIADO": "#c62828",
    "DESCONECTADO": "#616161"
}

class EV_Central_UI:
    def __init__(self, gestor):
        self.gestor = gestor
        self.gestor.ui_callback = self.update_ev_cp
        # self.gestor.ui_callback_message = self.add_message

        self.root = tk.Tk()
        self.root.title("EV_CENTRAL")

        # --- Contenedor principal ---
        self.main_frame = tk.Frame(self.root)
        self.main_frame.pack(fill=tk.BOTH, expand=True, padx=15, pady=15)

        # --- Contenedor superior: puntos de carga ---
        self.contenedor = tk.Frame(self.main_frame)
        self.contenedor.pack(fill=tk.BOTH, expand=True)

        # --- Contenedor inferior: mensajes ---
        self.info_frame = tk.Frame(self.main_frame)
        self.info_frame.pack(fill=tk.X, pady=(10, 0))

        # --- Contenedor inferior: mensajes de suministraje ---
        tk.Label(
            self.info_frame, 
            text="ON_GOING DRIVERS REQUEST (DATE | TIME | CP | DRIVER)", 
            font=("Arial", 10, "bold")
        ).pack(anchor="w")

        self.on_going_info = tk.Text(
            self.info_frame, height=6, state="disabled", wrap="word", bg="#616161",
            relief="solid", highlightbackground="white", highlightcolor="white" 
        )
        self.on_going_info.pack(fill=tk.BOTH, expand=True)
        
        # --- Contenedor inferior: mensajes de información ---
        tk.Label(
            self.info_frame, text="APPLICATION MESSAGES", font=("Arial", 10, "bold")
        ).pack(anchor="w", pady=(10, 0))

        self.text_info = tk.Text(
            self.info_frame, height=6, state="disabled", wrap="word", bg="#616161",
            relief="solid", highlightbackground="white", highlightcolor="white" 
        )
        self.text_info.pack(fill=tk.BOTH, expand=True)

        # --- Diccionario de frames de CPs ---
        self.frames = {}
        self.update_ev_cp(self.gestor.db.load_charging_points())

    # ------------------- ACTUALIZACIÓN DE CPs -------------------
    def update_ev_cp(self, charging_points):
        self.clear_messages()
        self.clear_on_going_messages()
        for i, punto in enumerate(charging_points.values()):
            bg_color = colores[punto.estado.name]
            self._update_or_create_point_frame(i, punto, bg_color)
            if punto.estado.name == "PARADO":
                self.add_message(f"[{punto.id}] Out of order")
            elif punto.estado.name == "SUMINISTRANDO":
                date_formated = datetime.fromtimestamp(punto.time)
                date_formated = date_formated.strftime("%d/%m/%Y | %H:%M:%S")
                self.add_on_going_message(f"{date_formated} | {punto.id} | {punto.driver or 'N/A'}")
        self.root.update_idletasks()

    def _update_or_create_point_frame(self, index, punto, bg_color):
        if punto.id not in self.frames:
            self._create_point_frame(index, punto, bg_color)
        else:
            self._update_point_frame(punto, bg_color)

    def _create_point_frame(self, index, punto, bg_color):
        frame = tk.Frame(
            self.contenedor,
            width=150,
            height=150,
            relief=tk.RAISED,
            borderwidth=2,
            bg=bg_color
        )
        
        frame.grid(row=index // 4, column=index % 4, padx=10, pady=10)
        frame.pack_propagate(False)

        # Frame clickeable
        frame.bind("<Button-1>", lambda e, pid=punto.id: self.on_click(pid))

        label_status = tk.Label(frame, text=punto.estado.name, font=("Arial", 9, "italic"), fg="white", bg=bg_color)
        label_status.pack(pady=(3, 3))

        label_id = tk.Label(frame, text=f"ID: {punto.id}", font=("Arial", 10, "bold"), fg="white", bg=bg_color)
        label_id.pack(pady=(2, 2))
        
        label_location = tk.Label(frame, text=punto.location, font=("Arial", 10), fg="white", bg=bg_color)
        label_location.pack(pady=(2, 2))

        label_price = tk.Label(frame, text=f"{punto.price}€/kWh", font=("Arial", 10), fg="white", bg=bg_color)
        label_price.pack(pady=(2, 2))

        label_driver = tk.Label(frame, font=("Arial", 10), fg="white", bg=bg_color)
        label_kwh = tk.Label(frame, font=("Arial", 10), fg="white", bg=bg_color)
        label_ticket = tk.Label(frame, font=("Arial", 10), fg="white", bg=bg_color)

        label_driver.pack(pady=(0, 0))
        label_kwh.pack(pady=(0, 0))
        label_ticket.pack(pady=(0, 0))

        self.frames[punto.id] = {
            "frame": frame,
            "id": label_id,
            "status": label_status,
            "location": label_location,
            "price": label_price,
            "extras": [label_driver, label_kwh, label_ticket]
        }

    def on_click(self, punto_id):
        self.gestor.parar_cp(punto_id)

    def _update_point_frame(self, punto, bg_color):
        frame_data = self.frames[punto.id]

        frame, label_id, label_status, label_location, label_price = (
            frame_data["frame"],
            frame_data["id"],
            frame_data["status"],
            frame_data["location"],
            frame_data["price"]
        )

        frame.config(bg=bg_color)
        label_id.config(bg=bg_color, text=f"ID: {punto.id}")
        label_status.config(bg=bg_color, text=punto.estado.name)
        label_location.config(bg=bg_color, text=punto.location)
        label_price.config(bg=bg_color, text=f"{punto.price}€/kWh")
        
        for lbl in frame_data["extras"]:
            lbl.config(bg=bg_color)

        if punto.estado == EstadoCP.SUMINISTRANDO:
            self._update_supply_info(punto)
        else:
            self._clear_extras(punto.id)

    def _update_supply_info(self, punto):
        label_driver, label_kwh, label_ticket = self.frames[punto.id]["extras"]
        label_driver.config(text=f"Driver: {punto.driver or 'N/A'}")
        label_kwh.config(text=f"Consumo: {punto.kwh} kWh")
        label_ticket.config(text=f"Importe: {punto.ticket} €")

    def _clear_extras(self, punto_id):
        for lbl in self.frames[punto_id]["extras"]:
            lbl.config(text="")

    # ------------------- MENSAJES DE INFORMACIÓN -------------------
    def add_message(self, msg):
        self.text_info.config(state="normal")
        self.text_info.insert(tk.END, f"• {msg}\n")
        self.text_info.see(tk.END)
        self.text_info.config(state="disabled")

    def clear_messages(self):
        self.text_info.config(state="normal")
        self.text_info.delete(1.0, tk.END)
        self.text_info.config(state="disabled")

    def add_on_going_message(self, msg):
        self.on_going_info.config(state="normal")
        self.on_going_info.insert(tk.END, f"• {msg}\n")
        self.on_going_info.see(tk.END)
        self.on_going_info.config(state="disabled")

    def clear_on_going_messages(self):
        self.on_going_info.config(state="normal")
        self.on_going_info.delete(1.0, tk.END)
        self.on_going_info.config(state="disabled")

    # ------------------- EJECUCIÓN -------------------
    def run(self):
        for i in range(4):
            self.contenedor.grid_columnconfigure(i, weight=1)
        self.root.mainloop()
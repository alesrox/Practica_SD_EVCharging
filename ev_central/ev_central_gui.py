import tkinter as tk
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

        self.root = tk.Tk()
        self.root.title("EV_CENTRAL")

        self.contenedor = tk.Frame(self.root)
        self.contenedor.pack(fill=tk.BOTH, expand=True, padx=15, pady=15)

        self.frames = {}
        self.update_ev_cp(self.gestor.charging_points)

    def update_ev_cp(self, charging_points):
        for i, punto in enumerate(charging_points.values()):
            bg_color = colores[punto.estado.name]
            self._update_or_create_point_frame(i, punto, bg_color)
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

        label_status = tk.Label(
            frame, text=punto.estado.name, font=("Arial", 9, "italic"), fg="white", bg=bg_color
        )
        label_status.pack(pady=(3, 3))

        label_id = tk.Label(
            frame, text=f"ID: {punto.id}", font=("Arial", 10, "bold"), fg="white", bg=bg_color
        )
        label_id.pack(pady=(2, 2))
        
        label_location = tk.Label(
            frame, text=punto.location, font=("Arial", 10), fg="white", bg=bg_color
        )
        label_location.pack(pady=(2, 2))

        label_price = tk.Label(
            frame, text=f"{punto.price}€/kWh", font=("Arial", 10), fg="white", bg=bg_color
        )
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
        if punto.driver: label_driver.config(text=f"Driver: {punto.driver}")
        label_kwh.config(text=f"Consumo: {punto.kwh} kWh")
        label_ticket.config(text=f"Importe: {punto.ticket} €")

    def _clear_extras(self, punto_id):
        for lbl in self.frames[punto_id]["extras"]:
            lbl.config(text="")

    def run(self):
        for i in range(4):
            self.contenedor.grid_columnconfigure(i, weight=1)
        self.root.mainloop()
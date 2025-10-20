from ev_cp_e import *
from ev_cp_m import *

import threading
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Charging Point")
    parser.add_argument("id", help="ID del Charging Point")
    parser.add_argument("--host", default="localhost", help="IP del engine")
    parser.add_argument("--port", type=int, default=5002, help="Puerto de escucha del Engine")
    parser.add_argument("--broker-host", default="localhost", help="IP de la central")
    parser.add_argument("--broker-port", type=int, default=9092, help="Puerto de la central")
    parser.add_argument("--central-host", default="localhost", help="IP de la central")
    parser.add_argument("--central-port", type=int, default=5001, help="Puerto de la central")
    args = parser.parse_args()

    engine = Engine(
        id=args.id, port=args.port,
        broker_host=args.broker_host, 
        broker_port=args.broker_port
    )

    monitor = Monitor(
        cp_id=args.id,
        central_host=args.central_host,
        central_port=args.central_port,
        engine_host=args.host,
        engine_port=args.port
    )

    monitor.auth_cp()
    threading.Thread(target=engine.start, args=(), daemon=True).start()
    engine.start()

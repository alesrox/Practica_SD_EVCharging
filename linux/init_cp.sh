#!/bin/bash
set -e

# ===== Cargar variable CENTRAL_IP desde .env =====
if [ ! -f .env ]; then
    echo "[ERROR] No se encontró el archivo .env"
    exit 1
fi

# Cargar la variable CENTRAL_IP
export $(grep -E '^CENTRAL_IP=' .env)

echo "CENTRAL_IP cargado: $CENTRAL_IP"

if [ -z "$CENTRAL_IP" ]; then
    echo "[ERROR] No se encontró CENTRAL_IP en el archivo .env"
    exit 1
fi

# ===== Instalar dependencias =====
echo "Instalando dependencias..."
pip install -r requirements.txt
clear

# ===== Pedir ID =====
read -p "Introduce el ID (por ejemplo MAD1) [MAD1]: " ID
ID=${ID:-MAD1}

read -p "Introduce el puerto de escucha [6001]: " PORT
PORT=${PORT:-6001}

echo "Usando:"
echo "  ID: $ID"
echo "  PORT: $PORT"
echo "  CENTRAL_IP: $CENTRAL_IP"
echo

# ===== Ejecutar scripts en ventanas nuevas =====

# Función para abrir en nueva terminal según disponibilidad
run_in_new_terminal() {
    CMD=$1

    if [[ "$OSTYPE" == "darwin"* ]]; then
        CURRENT_DIR=$(pwd)
        osascript <<EOF
tell application "Terminal"
    do script "cd \"$CURRENT_DIR\"; $CMD"
    activate
end tell
EOF
    else
        echo "[AVISO] No es macOS. Ejecutando en background."
        bash -c "$CMD" &
    fi
}

echo "Iniciando ev_cp_m.py en una nueva ventana..."
run_in_new_terminal "python3.11 ev_cp/ev_cp_m.py $ID --central $CENTRAL_IP:6000 --engine 127.0.0.1:$PORT --registry $CENTRAL_IP:8000"

echo "Iniciando ev_cp_e.py..."
python3.11 ev_cp/ev_cp_e.py $ID --broker $CENTRAL_IP:9092 --port $PORT

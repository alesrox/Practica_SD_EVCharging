#!/bin/bash
set -e

# ===== Cargar variable HOST_IP desde .env =====
if [ ! -f .env ]; then
    echo "[ERROR] No se encontró el archivo .env"
    exit 1
fi

# Cargar la variable HOST_IP
export $(grep -E '^HOST_IP=' .env)

echo "HOST_IP cargado: $HOST_IP"

if [ -z "$HOST_IP" ]; then
    echo "[ERROR] No se encontró HOST_IP en el archivo .env"
    exit 1
fi

# ===== Instalar dependencias =====
echo "Instalando dependencias..."
pip install -r requirements.txt

# ===== Pedir ID =====
read -p "Introduce el ID (por ejemplo MAD1) [MAD1]: " ID
ID=${ID:-MAD1}

read -p "Introduce el puerto de escucha [6001]: " PORT
PORT=${PORT:-6001}

echo "Usando:"
echo "  ID: $ID"
echo "  PORT: $PORT"
echo "  HOST_IP: $HOST_IP"
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
run_in_new_terminal "python3.11 ev_cp/ev_cp_m.py $ID --central $HOST_IP:6000 --engine localhost:$PORT"

echo "Iniciando ev_cp_e.py..."
python3.11 ev_cp/ev_cp_e.py $ID --broker $HOST_IP:9092 --port $PORT

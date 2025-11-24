#!/bin/bash
set -e

# Construir y levantar contenedores
docker compose up -d --build

# Instalar dependencias
pip3.11 install -r requirements.txt

# Ejecutar topics.py
python3.11 topics.py

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

# Ejecutar ev_central con el broker
run_in_new_terminal "uvicorn db.server:app --host 0.0.0.0 --port 9000 --reload"
# SERVER_PID=$!

echo "Esperando a que el servidor FastAPI esté listo..."
while ! curl -s http://localhost:9000/charging_points > /dev/null; do
    sleep 0.5
done

python3.11 ev_central/ev_central.py --broker localhost:9092
# kill $SERVER_PID
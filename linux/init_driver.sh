#!/bin/bash
set -e

# ===== Cargar CENTRAL_IP desde .env =====
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
read -p "Introduce el ID (por ejemplo DRI1) [DRI1]: " ID
ID=${ID:-DRI1}

# ===== Pedir FILE =====
read -p "Introduce el nombre del archivo (por ejemplo data.json) [data.json]: " FILE
FILE=${FILE:-data.json}


echo
echo "Usando configuración:"
echo "  ID: $ID"
echo "  FILE: $FILE"
echo "  CENTRAL_IP: $CENTRAL_IP"
echo

# ===== Ejecutar el script driver =====
if [ -n "$FILE" ]; then
    echo "Iniciando ev_driver.py con archivo en nueva ventana..."
    python3.11 ev_driver/ev_driver.py $ID --broker $CENTRAL_IP:9092 --file $FILE
else
    echo "Iniciando ev_driver.py sin archivo en nueva ventana..."
    python3.11 ev_driver/ev_driver.py $ID --broker $CENTRAL_IP:9092
fi
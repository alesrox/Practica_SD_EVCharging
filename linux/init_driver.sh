#!/bin/bash
set -e

# ===== Cargar HOST_IP desde .env =====
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
read -p "Introduce el ID (por ejemplo DRI1) [DRI1]: " ID
ID=${ID:-DRI1}

# ===== Pedir FILE =====
read -p "Introduce el nombre del archivo (por ejemplo data.json) [data.json]: " FILE
FILE=${FILE:-data.json}


echo
echo "Usando configuración:"
echo "  ID: $ID"
echo "  FILE: $FILE"
echo "  HOST_IP: $HOST_IP"
echo

# ===== Ejecutar el script driver =====
if [ -n "$FILE" ]; then
    echo "Iniciando ev_driver.py con archivo en nueva ventana..."
    python3.11 ev_driver/ev_driver.py $ID --broker $HOST_IP:9092 --file $FILE
else
    echo "Iniciando ev_driver.py sin archivo en nueva ventana..."
    python3.11 ev_driver/ev_driver.py $ID --broker $HOST_IP:9092
fi
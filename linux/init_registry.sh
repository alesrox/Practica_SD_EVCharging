#!/bin/bash
set -e

if [ ! -f .env ]; then
    echo "[ERROR] No se encontró el archivo .env"
    exit 1
fi

export $(grep -E '^REGISTRY_IP=' .env)

# Instalar dependencias
pip3.11 install -r requirements.txt
clear

echo $REGISTRY_IP
# Generar certificados SSL para el registry
openssl req -x509 -newkey rsa:4096 -keyout registry_key.pem -out registry_cert.pem -days 365 -nodes -subj "/CN=$REGISTRY_IP" -addext "subjectAltName = DNS:localhost,IP:127.0.0.1,IP:0.0.0.0"

uvicorn ev_registry.ev_registry:app --host 0.0.0.0 --port 8000 --ssl-keyfile registry_key.pem --ssl-certfile registry_cert.pem
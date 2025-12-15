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

# openssl req -x509 -nodes -days 365 -newkey rsa:2048 -keyout certServ.pem -out certServ.pem
python3.11 ev_registry/ev_registry.py
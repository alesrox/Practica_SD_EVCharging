#!/bin/bash
set -e

# Construir y levantar contenedores
docker compose up -d --build

# Instalar dependencias
pip3.11 install -r requirements.txt

# Ejecutar topics.py
python3.11 topics.py

# Ejecutar ev_central con el broker
python3.11 ev_central/ev_central.py --broker localhost:9092
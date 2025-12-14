@echo off
cd ..
docker compose up -d --build
pip install -r requirements.txt
python topics.py
cls
start "BASE DE DATOS" cmd /k "python db/server.py"
start "API CENTRAL" cmd /k "python ev_central/api_central.py"
python ev_central/ev_central.py --broker 127.0.0.1:9092

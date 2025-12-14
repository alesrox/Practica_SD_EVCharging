@echo off
cd ..
docker compose up -d --build
pip install -r requirements.txt
python topics.py
cls
start "BASE DE DATOS" cmd /k "python3 db/server.py"
start "API CENTRAL" cmd /k "python3 ev_central/api_central.py"
python ev_central/ev_central.py --broker 127.0.0.1:9092

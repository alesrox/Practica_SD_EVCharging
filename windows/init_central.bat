@echo off
cd ..
docker compose up -d --build
pip install -r requirements.txt
python topics.py
start "BASE DE DATOS" cmd /k "uvicorn db.server:app --host 0.0.0.0 --port 9000 --reload"
python ev_central/ev_central.py --broker localhost:9092

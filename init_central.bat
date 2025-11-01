docker compose up -d --build
pip install -r requirements.txt
python topics.py
python ev_central/ev_central.py --broker localhost:9092

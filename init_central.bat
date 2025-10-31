docker compose up -d --build
pip install -r requiriments.txt
python topics.py
python ev_central/ev_central.py --broker localhost:9092

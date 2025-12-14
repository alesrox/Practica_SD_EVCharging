import time
from confluent_kafka.admin import AdminClient, NewTopic

# Ajusta la dirección del broker según tu Docker Compose
BROKER = '127.0.0.1:9092'

# Esperar a que Kafka esté disponible
while True:
    try:
        admin = AdminClient({'bootstrap.servers': BROKER})
        admin.list_topics(timeout=5)  # Solo para probar conexión
        print("Kafka disponible")
        break
    except Exception:
        print("Esperando a Kafka...")
        time.sleep(2)

# Definir topics
topics = [
    NewTopic("zone0-central-request", num_partitions=3, replication_factor=1),
    NewTopic("zone1-central-request", num_partitions=3, replication_factor=1),
    NewTopic("zone2-central-request", num_partitions=3, replication_factor=1),

    NewTopic("zone0-central-response", num_partitions=3, replication_factor=1),
    NewTopic("zone1-central-response", num_partitions=3, replication_factor=1),
    NewTopic("zone2-central-response", num_partitions=3, replication_factor=1),
]

# Crear topics
fs = admin.create_topics(topics)

# Imprimir resultados
for topic, f in fs.items():
    try:
        f.result()  # Espera a que la creación termine
        print(f"Tópico {topic} creado")
    except Exception as e:
        print(f"Tópico {topic} ya existe o error: {e}")
from confluent_kafka.admin import AdminClient, NewTopic

admin = AdminClient({'bootstrap.servers': 'broker:29092'})

topic0 = NewTopic("central-request", num_partitions=3, replication_factor=1)
topic1 = NewTopic("engine-response", num_partitions=3, replication_factor=1)
topic2 = NewTopic("driver-response", num_partitions=3, replication_factor=1)
fs = admin.create_topics([topic0, topic1, topic2])

for topic, f in fs.items():
    try:
        f.result()
        print(f"Tópico {topic} creado")
    except Exception as e:
        print(f"Tópico {topic} ya existe o error: {e}")
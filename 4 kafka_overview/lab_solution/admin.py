from confluent_kafka.admin import AdminClient, NewTopic

config = {
    "bootstrap.servers": "localhost:9092"
}

admin_client = AdminClient(config)
topic = "gutenberg_book"

futures = admin_client.create_topics([
    NewTopic(topic, num_partitions=1, replication_factor=1)
])

for topic_name, future in futures.items():
    try:
        future.result()
        print(f"Topic '{topic_name}' créé avec succès.")
    except Exception as e:
        print(f"Résultat pour '{topic_name}' : {e}")

metadata = admin_client.list_topics(timeout=10)
print("Topics disponibles :", list(metadata.topics.keys()))

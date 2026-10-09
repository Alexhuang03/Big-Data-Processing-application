from confluent_kafka import Producer
from pathlib import Path

# Connexion au serveur Kafka
config = {
    "bootstrap.servers": "localhost:9092"
}

producer = Producer(config)
topic = "gutenberg_book"

# Chemin du livre, dans le même dossier que ce script
book_path = Path(__file__).parent / "book.txt"

# Envoyer chaque ligne du livre dans Kafka
with open(book_path, "r", encoding="utf-8") as book:
    for line_number, line in enumerate(book, start=1):
        line = line.rstrip("\r\n")
        producer.produce(topic, value=line.encode("utf-8"))
        producer.poll(0)
        print(f"Ligne {line_number} envoyée")

# Attendre que tous les messages soient envoyés
remaining = producer.flush(30)

if remaining == 0:
    print("Livre envoyé avec succès dans Kafka !")
else:
    print(f"Attention : {remaining} message(s) n'ont pas été envoyés.")

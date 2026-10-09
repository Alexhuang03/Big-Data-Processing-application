from confluent_kafka import Consumer
from pathlib import Path
import re

config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "gutenberg-consumer-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}

consumer = Consumer(config)
topic = "gutenberg_book"
consumer.subscribe([topic])

output_path = Path(__file__).parent / "book_clean.txt"
messages = []
empty_polls = 0

try:
    with open(output_path, "w", encoding="utf-8") as output:
        print("Lecture des messages Kafka...")

        while empty_polls < 5:
            msg = consumer.poll(1.0)

            if msg is None:
                empty_polls += 1
                continue

            if msg.error():
                print("Erreur Kafka :", msg.error())
                continue

            empty_polls = 0
            text = msg.value().decode("utf-8")

            # Nettoyer les espaces inutiles
            text = re.sub(r"\s+", " ", text).strip()

            if text:
                output.write(text + "\n")
                messages.append(text)

    consumer.commit(asynchronous=False)
    print(f"Terminé : {len(messages)} messages écrits dans {output_path.name}")

finally:
    consumer.close()

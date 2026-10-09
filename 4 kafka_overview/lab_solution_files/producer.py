# =============================================================================
# KAFKA LAB - producer.py
# =============================================================================
# Ce fichier lit un livre téléchargé depuis Project Gutenberg ligne par ligne
# et envoie chaque ligne comme un message dans le topic Kafka 'book_lines'.
#
# Concept Kafka - Producteur :
#   Le producteur est l'entité qui ENVOIE des messages vers un topic.
#   Il ne sait pas qui va lire ces messages : c'est le rôle du consommateur.
#   Cette séparation est ce qui rend Kafka scalable et découplé.
#
# PRÉREQUIS :
#   - Avoir lancé admin.py pour créer le topic 'book_lines'
#   - Avoir téléchargé le livre sur https://www.gutenberg.org/ebooks/103
#     et l'avoir sauvegardé sous le nom 'around_the_world_in_80_days.txt'
#     dans le même dossier que ce fichier.
# =============================================================================

import socket
from confluent_kafka import Producer

# -----------------------------------------------------------------------------
# CONFIGURATION DU PRODUCTEUR
# -----------------------------------------------------------------------------
# 'bootstrap.servers' : adresse du broker Kafka
# 'client.id'         : identifiant unique de ce producteur
#                       socket.gethostname() utilise le nom de la machine
# -----------------------------------------------------------------------------
conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': socket.gethostname()
}

# Création de l'instance Producer avec la configuration
producer = Producer(conf)

# -----------------------------------------------------------------------------
# TOPIC CIBLE
# -----------------------------------------------------------------------------
# Le nom du topic doit correspondre exactement à celui créé dans admin.py
# et à celui écouté dans consumer.py.
# -----------------------------------------------------------------------------
TOPIC_NAME = 'book_lines'

# Chemin vers le livre téléchargé depuis Project Gutenberg
# Livre : "Around the World in Eighty Days" par Jules Verne (déjà utilisé
# dans le lab word_count avec PySpark - ebook #103 sur gutenberg.org)
BOOK_FILE = 'around_the_world_in_80_days.txt'


# -----------------------------------------------------------------------------
# FONCTION : delivery_callback
# -----------------------------------------------------------------------------
# Callback appelée automatiquement par Kafka après chaque tentative d'envoi.
# Elle permet de confirmer que le message a bien été livré au broker,
# ou de signaler une erreur si l'envoi a échoué.
#
# Paramètres :
#   err : None si succès, objet KafkaError sinon
#   msg : l'objet Message envoyé, avec métadonnées (topic, partition, offset)
# -----------------------------------------------------------------------------
def delivery_callback(err, msg):
    if err:
        # L'envoi a échoué : on affiche l'erreur
        print(f"[ERREUR] Echec de livraison: {err}")
    else:
        # L'envoi a réussi : on peut tracer la partition et l'offset
        # (utile pour debug, peut être commenté en production)
        # print(f"[OK] Message livré -> topic={msg.topic()}, partition={msg.partition()}, offset={msg.offset()}")
        pass


# -----------------------------------------------------------------------------
# LECTURE DU LIVRE ET ENVOI LIGNE PAR LIGNE
# -----------------------------------------------------------------------------
# On ouvre le fichier en lecture avec l'encodage UTF-8 (format Gutenberg).
# Chaque ligne est envoyée comme un message Kafka indépendant.
# -----------------------------------------------------------------------------
print(f"Lecture du fichier '{BOOK_FILE}' et envoi vers le topic '{TOPIC_NAME}'...")

lines_sent = 0  # Compteur de lignes envoyées

try:
    with open(BOOK_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            # On supprime les espaces/retours à la ligne en début et fin
            line = line.strip()

            # On ignore les lignes vides pour ne pas encombrer le topic
            if not line:
                continue

            # -----------------------------------------------------------------
            # producer.produce() : envoi asynchrone du message
            # -----------------------------------------------------------------
            # Paramètres :
            #   topic    : nom du topic Kafka cible
            #   value    : contenu du message (doit être bytes ou str)
            #   callback : fonction appelée après confirmation de livraison
            # L'envoi est asynchrone : il est mis en file d'attente interne.
            # -----------------------------------------------------------------
            producer.produce(
                topic=TOPIC_NAME,
                value=line,
                callback=delivery_callback
            )

            lines_sent += 1

            # -----------------------------------------------------------------
            # producer.poll(0) : traitement des événements internes
            # -----------------------------------------------------------------
            # Appel non-bloquant (timeout=0) qui déclenche les callbacks
            # de livraison en attente. Sans cela, les callbacks ne seraient
            # appelées qu'au moment du flush() final.
            # On l'appelle à chaque ligne pour éviter de saturer le buffer.
            # -----------------------------------------------------------------
            producer.poll(0)

except FileNotFoundError:
    print(f"[ERREUR] Fichier '{BOOK_FILE}' introuvable.")
    print("Téléchargez-le depuis : https://www.gutenberg.org/ebooks/103")
    exit(1)

# -----------------------------------------------------------------------------
# FLUSH : ENVOI DE TOUS LES MESSAGES EN ATTENTE
# -----------------------------------------------------------------------------
# flush() est BLOQUANT : il attend que tous les messages dans le buffer interne
# soient effectivement envoyés au broker Kafka avant de continuer.
# C'est essentiel pour s'assurer qu'aucun message n'est perdu à la fin du script.
# -----------------------------------------------------------------------------
print(f"\nEnvoi en cours... ({lines_sent} lignes dans la file)")
producer.flush()
print(f"[OK] Tous les messages ont été envoyés avec succès !")
print(f"     Total lignes envoyées : {lines_sent}")

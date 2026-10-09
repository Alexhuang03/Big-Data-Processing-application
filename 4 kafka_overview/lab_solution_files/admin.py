# =============================================================================
# KAFKA LAB - admin.py
# =============================================================================
# Ce fichier est responsable de la création et de la gestion des topics Kafka.
# Un "topic" est comme une file d'attente nommée dans laquelle les producteurs
# écrivent des messages et depuis laquelle les consommateurs lisent.
#
# ORDRE D'EXÉCUTION :
#   1. admin.py   -> crée le topic Kafka
#   2. producer.py -> lit le livre et envoie chaque ligne dans le topic
#   3. consumer.py -> lit les messages du topic, les nettoie, et les sauvegarde
# =============================================================================

from confluent_kafka.admin import AdminClient, NewTopic

# -----------------------------------------------------------------------------
# CONFIGURATION DU CLIENT ADMIN
# -----------------------------------------------------------------------------
# On configure la connexion au broker Kafka.
# 'bootstrap.servers' : adresse du serveur Kafka (ici en local via Docker)
# Le port 9092 est le port par défaut de Kafka.
# -----------------------------------------------------------------------------
config = {
    'bootstrap.servers': 'localhost:9092',
}

# Création du client administrateur Kafka avec la config ci-dessus
admin_client = AdminClient(config)

# -----------------------------------------------------------------------------
# CRÉATION DU TOPIC
# -----------------------------------------------------------------------------
# On définit le nom du topic sur lequel on va envoyer les lignes du livre.
# Ce nom doit être le MÊME dans producer.py et consumer.py.
#
# Paramètres de NewTopic :
#   - nom du topic : 'book_lines'
#   - num_partitions : nombre de partitions (1 suffit pour ce lab)
#   - replication_factor : nombre de replicas (1 car on a 1 seul broker Docker)
# -----------------------------------------------------------------------------
TOPIC_NAME = 'book_lines'

print(f"Creation du topic '{TOPIC_NAME}'...")

# create_topics() retourne un dict {topic_name: Future}
# Un Future représente une opération asynchrone en cours
result = admin_client.create_topics(
    [NewTopic(TOPIC_NAME, num_partitions=1, replication_factor=1)]
)

# On attend que la création soit terminée et on affiche le résultat
for topic, future in result.items():
    try:
        future.result()  # Bloque jusqu'à la fin de l'opération
        print(f"  [OK] Topic '{topic}' cree avec succes.")
    except Exception as e:
        # Si le topic existe déjà, une erreur est levée mais ce n'est pas bloquant
        print(f"  [!] Impossible de creer le topic '{topic}': {e}")

# -----------------------------------------------------------------------------
# VÉRIFICATION : LISTE DES TOPICS EXISTANTS
# -----------------------------------------------------------------------------
# On liste tous les topics pour confirmer que notre topic a bien été créé.
# list_topics() retourne un objet ClusterMetadata contenant tous les topics.
# -----------------------------------------------------------------------------
print("\nTopics actuellement disponibles sur le broker :")
metadata = admin_client.list_topics(timeout=10)
for t in metadata.topics.keys():
    print(f"  - {t}")

# -----------------------------------------------------------------------------
# SUPPRESSION (optionnelle - décommenter si besoin de repartir à zéro)
# -----------------------------------------------------------------------------
# admin_client.delete_topics([TOPIC_NAME])

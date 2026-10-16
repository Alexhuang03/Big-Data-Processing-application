# =============================================================================
# STRUCTURED STREAMING LAB - admin.py
# =============================================================================
# Crée le topic Kafka 'wikistreams' qui recevra les événements Wikimedia
# envoyés par wikistream_producer.py et lus par le notebook PySpark.
#
# Repris du lab Kafka : seul le nom du topic change.
#
# PRÉREQUIS :
#   - docker compose up -d   (le broker Kafka écoute sur localhost:9092)
#
# UTILISATION :
#   python admin.py            -> crée le topic et liste les topics
#   python admin.py --delete   -> supprime le topic (pour repartir de zéro)
# =============================================================================

import sys
from confluent_kafka.admin import AdminClient, NewTopic

# -----------------------------------------------------------------------------
# CONNEXION AU BROKER
# -----------------------------------------------------------------------------
# Depuis la machine hôte on passe par le listener EXTERNAL (localhost:9092).
# Le conteneur Spark, lui, utilise le listener INTERNAL (kafka:19092).
# -----------------------------------------------------------------------------
config = {
    'bootstrap.servers': 'localhost:9092',
}
admin_client = AdminClient(config)

topic = 'wikistreams'

# -----------------------------------------------------------------------------
# SUPPRESSION (optionnelle)
# -----------------------------------------------------------------------------
if '--delete' in sys.argv:
    for t, future in admin_client.delete_topics([topic]).items():
        try:
            future.result()
            print(f"Topic '{t}' supprimé.")
        except Exception as e:
            print(f"Impossible de supprimer '{t}' : {e}")
    sys.exit(0)

# -----------------------------------------------------------------------------
# CRÉATION DU TOPIC
# -----------------------------------------------------------------------------
# create_topics() est asynchrone : il renvoie un dict {topic: future}.
# On attend chaque future pour savoir si la création a réussi.
# Une seule partition suffit pour ce lab (un seul producteur, un seul broker).
# -----------------------------------------------------------------------------
futures = admin_client.create_topics(
    [NewTopic(topic, num_partitions=1, replication_factor=1)]
)
for t, future in futures.items():
    try:
        future.result()
        print(f"Topic '{t}' créé.")
    except Exception as e:
        # Cas typique : TOPIC_ALREADY_EXISTS si on relance le script
        print(f"Topic '{t}' non créé : {e}")

# -----------------------------------------------------------------------------
# LISTE DES TOPICS
# -----------------------------------------------------------------------------
print('\nTopics présents sur le broker :')
for t in admin_client.list_topics(timeout=10).topics.keys():
    print(' -', t)

# =============================================================================
# KAFKA LAB - consumer.py
# =============================================================================
# Ce fichier consomme les messages du topic Kafka 'book_lines' (les lignes du
# livre envoyées par producer.py), applique un nettoyage de texte inspiré
# du lab word_count (PySpark), et sauvegarde le résultat dans un fichier.
#
# Concept Kafka - Consommateur :
#   Le consommateur LIT des messages depuis un topic Kafka.
#   Il appartient à un "consumer group" (groupe de consommateurs) qui permet
#   à plusieurs instances de se partager les partitions pour paralléliser.
#   L'offset indique la position du dernier message lu dans chaque partition.
#
# Pipeline de traitement dans ce fichier :
#   Message Kafka (ligne brute)
#     -> clean_text()       : nettoyage NLP (minuscules, ponctuation, stop words)
#     -> Ecriture dans un fichier de sortie
# =============================================================================

import re
from confluent_kafka import Consumer

# -----------------------------------------------------------------------------
# CONFIGURATION DU CONSOMMATEUR
# -----------------------------------------------------------------------------
# 'bootstrap.servers'  : adresse du broker Kafka
# 'group.id'           : identifiant du groupe de consommateurs.
#                        Tous les consumers du même groupe se partagent les
#                        partitions du topic (load balancing automatique).
# 'auto.offset.reset'  : comportement quand il n'y a pas d'offset enregistré.
#                        'earliest' (= 'smallest') : repart depuis le début.
#                        'latest'                  : ne lit que les nouveaux msgs.
# -----------------------------------------------------------------------------
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'book_processor_group',
    'auto.offset.reset': 'earliest'
}

# Création de l'instance Consumer avec la configuration
consumer = Consumer(conf)

# -----------------------------------------------------------------------------
# ABONNEMENT AU TOPIC
# -----------------------------------------------------------------------------
# subscribe() prend une liste de topics (on peut s'abonner à plusieurs à la fois).
# Le topic doit avoir été créé au préalable par admin.py.
# -----------------------------------------------------------------------------
TOPIC_NAME = 'book_lines'
consumer.subscribe([TOPIC_NAME])

# Fichier dans lequel on va sauvegarder les lignes nettoyées
OUTPUT_FILE = 'cleaned_book_output.txt'

# -----------------------------------------------------------------------------
# STOP WORDS (mots vides)
# -----------------------------------------------------------------------------
# Les stop words sont des mots très fréquents qui n'apportent pas de valeur
# sémantique (articles, prépositions, conjonctions...).
# On les retire pour ne garder que les mots "significatifs".
# Inspiré de l'étape de nettoyage NLP du lab word_count (PySpark).
# -----------------------------------------------------------------------------
STOP_WORDS = {
    'a', 'an', 'the', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
    'of', 'with', 'by', 'from', 'up', 'about', 'into', 'through', 'is',
    'was', 'are', 'were', 'be', 'been', 'being', 'have', 'has', 'had',
    'do', 'does', 'did', 'will', 'would', 'could', 'should', 'may', 'might',
    'shall', 'can', 'not', 'no', 'nor', 'so', 'yet', 'both', 'either',
    'it', 'its', 'he', 'she', 'they', 'we', 'you', 'i', 'me', 'him',
    'her', 'them', 'us', 'my', 'your', 'his', 'their', 'our', 'that',
    'this', 'these', 'those', 'what', 'which', 'who', 'whom', 'when',
    'where', 'why', 'how', 'all', 'each', 'every', 'more', 'most',
    'other', 'than', 'then', 'as', 'if', 'only', 'also', 'just', 'now'
}


# -----------------------------------------------------------------------------
# FONCTION : clean_text(line)
# -----------------------------------------------------------------------------
# Applique une série de transformations NLP sur une ligne de texte brute.
# Inspiré des étapes de nettoyage du lab word_count avec PySpark/RDD :
#   rdd.flatMap -> split, lower, strip ponctuation, filter stop words
#
# Paramètre :
#   line (str) : une ligne de texte brute reçue depuis Kafka
#
# Retourne :
#   str : la ligne nettoyée, ou une chaîne vide si elle n'apporte rien
# -----------------------------------------------------------------------------
def clean_text(line):
    # Étape 1 : Conversion en minuscules
    # Permet de traiter "The" et "the" comme le même mot
    line = line.lower()

    # Étape 2 : Suppression de la ponctuation
    # On remplace tout caractère qui n'est pas une lettre ou un espace
    # par un espace. Le regex [^a-z\s] signifie "tout sauf a-z et espaces".
    line = re.sub(r'[^a-z\s]', ' ', line)

    # Étape 3 : Suppression des espaces multiples
    # On remplace les séquences d'espaces par un seul espace
    line = re.sub(r'\s+', ' ', line).strip()

    # Étape 4 : Tokenisation (découpage en mots)
    # On sépare la ligne en une liste de mots individuels
    words = line.split()

    # Étape 5 : Filtrage des stop words et des mots trop courts
    # On ne garde que les mots qui ne sont pas des stop words
    # et qui ont au moins 2 caractères (pour éviter les lettres isolées)
    meaningful_words = [
        word for word in words
        if word not in STOP_WORDS and len(word) >= 2
    ]

    # Étape 6 : Reconstruction de la ligne nettoyée
    # On rejoint les mots filtrés en une seule chaîne
    cleaned = ' '.join(meaningful_words)

    return cleaned


# -----------------------------------------------------------------------------
# BOUCLE DE CONSOMMATION PRINCIPALE
# -----------------------------------------------------------------------------
# On lit les messages en continu depuis Kafka avec une stratégie de sortie :
#   - MAX_EMPTY_POLLS : si on reçoit N polls vides consécutifs -> arrêt
#   - MAX_ERRORS      : si on accumule N erreurs consécutives  -> arrêt
# Cette stratégie évite une boucle infinie si le producer a terminé.
# -----------------------------------------------------------------------------
MAX_EMPTY_POLLS = 20   # Nombre de polls sans message avant arrêt (~20 secondes)
MAX_ERRORS = 5         # Nombre d'erreurs consécutives avant arrêt

empty_polls = 0   # Compteur de polls vides consécutifs
error_count = 0   # Compteur d'erreurs consécutives
messages_processed = 0  # Nombre total de messages traités avec succès
lines_written = 0       # Nombre de lignes écrites dans le fichier de sortie

print(f"Démarrage du consommateur sur le topic '{TOPIC_NAME}'...")
print(f"Ecriture du résultat dans '{OUTPUT_FILE}'...\n")

# Ouverture du fichier de sortie en mode écriture (écrase si existant)
with open(OUTPUT_FILE, 'w', encoding='utf-8') as output_file:

    while True:
        # -----------------------------------------------------------------
        # consumer.poll(timeout) : attente d'un message
        # -----------------------------------------------------------------
        # Attend jusqu'à 'timeout' secondes la réception d'un message.
        # Retourne :
        #   - un objet Message si un message est arrivé
        #   - None si le timeout est atteint sans message
        # Un timeout de 1.0 seconde est un bon compromis entre réactivité
        # et charge CPU.
        # -----------------------------------------------------------------
        msg = consumer.poll(1.0)

        # --- Cas 1 : Aucun message reçu (timeout) ---
        if msg is None:
            empty_polls += 1
            print(f"  [ATTENTE] Aucun message... ({empty_polls}/{MAX_EMPTY_POLLS})")
            if empty_polls >= MAX_EMPTY_POLLS:
                print("\n[FIN] Arrêt : plus de nouveaux messages.")
                break
            continue  # On retourne au début de la boucle

        # --- Cas 2 : Un message avec erreur ---
        if msg.error():
            error_count += 1
            print(f"[ERREUR] Erreur consommateur: {msg.error()}")
            if error_count >= MAX_ERRORS:
                print("[FIN] Arrêt : trop d'erreurs consécutives.")
                break
            continue

        # --- Cas 3 : Message reçu avec succès ---
        # On remet les compteurs à zéro car on reçoit bien des données
        empty_polls = 0
        error_count = 0
        messages_processed += 1

        # Décodage du message : les valeurs Kafka sont des bytes, on les
        # décode en str UTF-8 pour pouvoir les manipuler comme du texte
        raw_line = msg.value().decode('utf-8')

        # -----------------------------------------------------------------
        # NETTOYAGE DU TEXTE
        # -----------------------------------------------------------------
        # On applique notre pipeline de nettoyage NLP sur la ligne reçue.
        # Si le résultat est vide (ligne sans mots utiles), on la ignore.
        # -----------------------------------------------------------------
        cleaned_line = clean_text(raw_line)

        if cleaned_line:
            # On écrit la ligne nettoyée dans le fichier de sortie
            output_file.write(cleaned_line + '\n')
            lines_written += 1

            # Affichage de progression toutes les 500 lignes écrites
            if lines_written % 500 == 0:
                print(f"  [PROGRESS] {lines_written} lignes écrites...")

# -----------------------------------------------------------------------------
# NETTOYAGE ET RÉSUMÉ FINAL
# -----------------------------------------------------------------------------
# consumer.close() est important : il notifie le broker Kafka que ce consumer
# quitte son groupe, ce qui permet la re-assignation des partitions aux
# autres consumers du groupe (s'il y en a).
# -----------------------------------------------------------------------------
consumer.close()

print(f"\n{'='*50}")
print(f"RESUME DE TRAITEMENT")
print(f"{'='*50}")
print(f"  Messages Kafka recus          : {messages_processed}")
print(f"  Lignes ecrites (nettoyees)    : {lines_written}")
print(f"  Lignes ignorees (vides)       : {messages_processed - lines_written}")
print(f"  Fichier de sortie             : {OUTPUT_FILE}")
print(f"{'='*50}")

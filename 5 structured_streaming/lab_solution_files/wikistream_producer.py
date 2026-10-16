# =============================================================================
# STRUCTURED STREAMING LAB - wikistream_producer.py
# =============================================================================
# Lit le flux temps réel "recentchange" de Wikimedia (EventStreams, SSE) et
# envoie chaque événement, en JSON, dans le topic Kafka 'wikistreams'.
#
# Différence avec le producteur de la démo (TO DO 11) :
#   Les filtres ne sont plus codés en dur. On choisit en ligne de commande
#   quel(s) wiki(s), quel type d'événement, quelles pages, quels
#   utilisateurs... on veut suivre. Les filtres sont appliqués CÔTÉ
#   PRODUCTEUR : seuls les événements retenus arrivent dans Kafka.
#
# PRÉREQUIS :
#   pip install confluent-kafka pywikibot "requests-sse>=0.5.0"
#   python admin.py   (topic 'wikistreams' créé)
#
# EXEMPLES :
#   # Démo d'origine : éditions sur fr.wikipedia pendant 10 minutes
#   python wikistream_producer.py
#
#   # Wikipédia anglaise + allemande, éditions ET créations de pages
#   python wikistream_producer.py --wiki en.wikipedia.org de.wikipedia.org --type edit new
#
#   # Uniquement les humains, articles seulement (namespace 0), 30 minutes
#   python wikistream_producer.py --no-bot --namespace 0 --duration 30
#
#   # Suivre des pages précises
#   python wikistream_producer.py --title "Paris" "Emmanuel Macron"
#
#   # Seulement les "grosses" modifications (|delta| >= 500 octets)
#   python wikistream_producer.py --min-delta 500
#
#   # Rejouer l'historique depuis une date (EventStreams garde ~7 jours)
#   python wikistream_producer.py --since 2026-10-16T08:00:00Z
# =============================================================================

import argparse
import json
import os
import socket
import sys
from datetime import datetime, timedelta

# Les titres/commentaires contiennent des caractères de toutes les langues :
# sous Windows la console (cp1252, gbk...) planterait sur un print().
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# pywikibot cherche un fichier user-config.py au chargement ; il n'est pas
# nécessaire pour lire EventStreams, on désactive donc cette vérification.
os.environ.setdefault('PYWIKIBOT_NO_USER_CONFIG', '1')

from confluent_kafka import Producer
from pywikibot.comms.eventstreams import EventStreams

# -----------------------------------------------------------------------------
# ARGUMENTS EN LIGNE DE COMMANDE = FILTRES
# -----------------------------------------------------------------------------
# nargs='+' : on peut passer plusieurs valeurs, elles sont combinées en OU
# (ex : --wiki fr.wikipedia.org en.wikipedia.org). Les différents filtres
# entre eux sont combinés en ET.
# -----------------------------------------------------------------------------
parser = argparse.ArgumentParser(description='Wikimedia EventStreams -> Kafka')
parser.add_argument('--wiki', nargs='+', default=['fr.wikipedia.org'],
                    help="server_name à suivre (défaut : fr.wikipedia.org). "
                         "'all' = tous les wikis")
parser.add_argument('--type', nargs='+', default=['edit'],
                    help="type d'événement : edit, new, log, categorize "
                         "(défaut : edit). 'all' = tous les types")
parser.add_argument('--title', nargs='+',
                    help='ne suivre que ces pages (titre exact)')
parser.add_argument('--user', nargs='+',
                    help='ne suivre que ces utilisateurs')
parser.add_argument('--namespace', nargs='+', type=int,
                    help='namespaces à garder (0 = articles, 1 = discussions...)')
bot_group = parser.add_mutually_exclusive_group()
bot_group.add_argument('--bot', dest='bot', action='store_true', default=None,
                       help='uniquement les robots')
bot_group.add_argument('--no-bot', dest='bot', action='store_false',
                       help='uniquement les humains')
parser.add_argument('--min-delta', type=int, default=0,
                    help='taille minimale |new - old| en octets (défaut : 0)')
parser.add_argument('--since',
                    help="rejouer depuis cette date (ex 2026-10-16T08:00:00Z). "
                         "Par défaut : temps réel")
parser.add_argument('--duration', type=float, default=10,
                    help='durée du streaming en minutes (défaut : 10)')
parser.add_argument('--topic', default='wikistreams',
                    help='topic Kafka cible (défaut : wikistreams)')
parser.add_argument('--quiet', action='store_true',
                    help="n'affiche qu'un résumé par événement")
args = parser.parse_args()

# -----------------------------------------------------------------------------
# PRODUCTEUR KAFKA
# -----------------------------------------------------------------------------
# Sérialiseur : dict Python -> JSON -> bytes
value_serializer = lambda val: json.dumps(val).encode('utf-8')

conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': socket.gethostname(),
    'compression.type': 'lz4',
}
producer = Producer(conf)

# -----------------------------------------------------------------------------
# FLUX WIKIMEDIA + FILTRES
# -----------------------------------------------------------------------------
# On ne lit que 'recentchange'. Le flux 'revision-create' de la démo n'a pas
# le même schéma (pas de champ 'type', 'length', 'bot'...) : avec le filtre
# type='edit' ses événements étaient de toute façon tous rejetés.
#
# register_filter(champ=valeur) : un événement est gardé si le champ vaut
# la valeur ; une liste de valeurs signifie "l'une de ces valeurs".
# register_filter(fonction) : filtre personnalisé (ici la taille du delta).
# -----------------------------------------------------------------------------
stream_kwargs = {'streams': ['recentchange']}
if args.since:
    stream_kwargs['since'] = args.since
stream = EventStreams(**stream_kwargs)

if 'all' not in args.wiki:
    stream.register_filter(server_name=args.wiki)
if 'all' not in args.type:
    stream.register_filter(type=args.type)
if args.title:
    stream.register_filter(title=args.title)
if args.user:
    stream.register_filter(user=args.user)
if args.namespace:
    stream.register_filter(namespace=args.namespace)
if args.bot is not None:
    stream.register_filter(bot=args.bot)
if args.min_delta > 0:
    def big_enough(event):
        length = event.get('length') or {}
        delta = (length.get('new') or 0) - (length.get('old') or 0)
        return abs(delta) >= args.min_delta
    stream.register_filter(big_enough, ftype='all')

print(f'Filtres actifs : {vars(args)}')

# -----------------------------------------------------------------------------
# CALLBACK DE LIVRAISON
# -----------------------------------------------------------------------------
# Appelé par poll()/flush() pour chaque message : permet de détecter les
# erreurs (broker injoignable, topic inexistant...).
# -----------------------------------------------------------------------------
stats = {'sent': 0, 'errors': 0}

def delivery_report(err, msg):
    if err is not None:
        stats['errors'] += 1
        print(f'Erreur de livraison : {err}')
    else:
        stats['sent'] += 1

# -----------------------------------------------------------------------------
# BOUCLE DE STREAMING
# -----------------------------------------------------------------------------
stop_time = datetime.now() + timedelta(minutes=args.duration)
print(f"Streaming vers '{args.topic}' jusqu'à {stop_time:%H:%M:%S} "
      f'(Ctrl+C pour arrêter avant)\n')

try:
    while datetime.now() < stop_time:
        change = next(stream)
        producer.produce(
            topic=args.topic,
            value=value_serializer(change),
            callback=delivery_report,
        )
        producer.poll(0)  # déclenche les callbacks sans bloquer

        if args.quiet:
            length = change.get('length') or {}
            delta = (length.get('new') or 0) - (length.get('old') or 0)
            print('[{server_name}] {type:<10} {title!r} par {user} '
                  '(bot={bot}, delta={delta:+d})'
                  .format(delta=delta, **{k: change.get(k) for k in
                          ('server_name', 'type', 'title', 'user', 'bot')}))
        else:
            print(change, '\n')
except KeyboardInterrupt:
    print('\nArrêt demandé par l\'utilisateur.')
finally:
    # flush() attend que tous les messages en attente soient envoyés
    print('\nEnvoi des derniers messages...')
    producer.flush(60)
    print(f"Terminé : {stats['sent']} messages envoyés, "
          f"{stats['errors']} erreurs.")

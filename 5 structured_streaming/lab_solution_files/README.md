# Lab 5 – Structured Streaming : Wikimedia → Kafka → Spark

## Contenu du dossier

```
lab_solution_files/
├── compose.yaml                         # Kafka (KRaft) + Jupyter PySpark (inchangé)
├── admin.py                             # crée / supprime le topic 'wikistreams'
├── wikistream_producer.py               # EventStreams -> Kafka, filtres en ligne de commande
├── data/  jobs/                         # volumes montés dans le conteneur
└── notebooks/wikistream_pyspark.ipynb   # démo + 4 explorations (tumbling et glissantes)
```

## Lancement (TO DO 1 → 9)

```bash
cd "5 structured_streaming/lab_solution_files"
pip install confluent-kafka pywikibot "requests-sse>=0.5.0"

docker compose up -d
docker logs pyspark_notebook          # récupérer le lien http://127.0.0.1:8888/lab?token=...

python admin.py                       # crée le topic wikistreams
python wikistream_producer.py --quiet # 10 min sur fr.wikipedia (démo)
```

Dans Jupyter : ouvrir `work/wikistream_pyspark.ipynb` → *Run All*.
Sur l'hôte : `docker logs -f pyspark_notebook` pour voir les sinks `console`.
Pour finir : exécuter la dernière cellule (arrêt des requêtes) puis `docker compose down`.

## TO DO 10 – Agrégations ajoutées

Colonnes dérivées : `event_time` (cast du timestamp epoch), `size_delta = length.new - length.old`, `abs_delta`.

| Requête | Fenêtre | Type | Sortie | Ce qu'on mesure |
|---|---|---|---|---|
| `demo_bot_counts` | 1 h | tumbling | console / complete | éditions robot vs humain (démo d'origine) |
| `tumbling_1min_size` | 1 min | **tumbling** | console / update, watermark 2 min | nb d'éditions, éditeurs distincts, octets ajoutés/retirés, delta moyen/max, éditions mineures |
| `sliding_5min_top_users` | 5 min, pas 1 min | **glissante** | console / complete, trié | utilisateurs les plus actifs |
| `hot_pages` | 10 min, pas 2 min | **glissante** | memory (table SQL) | pages les plus modifiées, nb d'éditeurs, churn |
| `tumbling_2min_big_edits` | 2 min | **tumbling** + filtre | console / append, watermark 1 min | grosses modifs humaines (\|delta\| ≥ 1000 o) par wiki |

## TO DO 11 – Filtres côté producteur

Les filtres de `pywikibot.EventStreams.register_filter` sont exposés en arguments ; plusieurs valeurs = OU, plusieurs options = ET.

```bash
python wikistream_producer.py --wiki en.wikipedia.org de.wikipedia.org   # autres langues
python wikistream_producer.py --wiki all --type new                      # créations de pages, tous wikis
python wikistream_producer.py --title "Paris" "Emmanuel Macron"          # pages précises
python wikistream_producer.py --user "EmausBot"                          # un contributeur
python wikistream_producer.py --no-bot --namespace 0                     # humains, articles seulement
python wikistream_producer.py --min-delta 500                            # filtre personnalisé (fonction)
python wikistream_producer.py --since 2026-10-16T08:00:00Z --duration 5  # rejouer l'historique
```

Filtrer au niveau du producteur réduit le volume envoyé dans Kafka et traité par Spark ; filtrer dans Spark (`.filter(...)`, exploration D) garde toutes les données dans le topic et permet plusieurs consommateurs avec des vues différentes.

## TO DO 12 – Tumbling vs glissantes (overlapping)

- **Tumbling** – `window(event_time, "1 minute")` : fenêtres contiguës sans chevauchement, chaque événement appartient à **une seule** fenêtre. La somme des comptes = nombre réel d'événements. Adapté aux bilans par période (métriques par minute, rapports).
- **Glissante** – `window(event_time, "10 minutes", "2 minutes")` : une nouvelle fenêtre démarre tous les 2 min, chaque événement tombe dans **10/2 = 5 fenêtres**. Donne une moyenne mobile lissée, adaptée à la détection de tendances (« pages chaudes sur les 10 dernières minutes, mises à jour toutes les 2 min »). Vérifié dans le notebook : la somme des `edits` sur toutes les fenêtres de `hot_pages` ≈ 5 × le nombre d'éditions.

**Watermark et modes de sortie** :
- `complete` réécrit toute la table (seul mode qui autorise `orderBy`) ; l'état n'est jamais purgé.
- `update` n'affiche que les lignes modifiées ; avec un watermark, l'état des vieilles fenêtres est purgé.
- `append` n'émet une fenêtre qu'une fois, quand le watermark dépasse sa fin : résultat définitif, mais avec un délai (fenêtre + watermark).

## Problèmes rencontrés et corrections

1. **Version du connecteur Kafka** : la démo charge `spark-sql-kafka-0-10_2.13:4.1.0` alors que l'image `quay.io/jupyter/pyspark-notebook` (latest) embarque **Spark 4.2.0**. Le notebook construit maintenant le package à partir de `pyspark.__version__`.
2. **`from_unixtime`** renvoie une chaîne ; on caste directement `timestamp` (epoch en secondes) en `TimestampType`.
3. **`since='20260209'`** codé en dur dans le producteur de la démo : EventStreams ne conserve qu'environ 7 jours d'historique. Par défaut on lit maintenant le temps réel, et `--since` permet de rejouer.
4. Le flux **`revision-create`** de la démo n'a pas de champ `type` : avec le filtre `type='edit'` tous ses événements étaient rejetés. On ne lit que `recentchange`.
5. **Windows** : `print()` plantait (`UnicodeEncodeError: 'gbk'`) sur les titres accentués → `sys.stdout.reconfigure(encoding='utf-8')`.
6. pywikibot demande `user-config.py` → `PYWIKIBOT_NO_USER_CONFIG=1` ; il faut aussi installer `requests-sse`.
7. Les envois sont confirmés par un callback de livraison et `producer.flush()` (au lieu d'un `sleep(60)` puis `close()`).

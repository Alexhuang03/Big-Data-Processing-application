"""Polite clients for Wikidata and Wikipedia (data gathering only).

Gathering is the only step done outside Spark: this module downloads raw
records and appends them to JSON Lines files. Records already present in a
file are never requested again, so an interrupted run can simply be restarted.

Sources (human-authored, open licence):
    - Wikidata SPARQL endpoint: maps IMDb ids (property P345) to the exact
      English Wikipedia article and gives birth/death facts for people.
    - English Wikipedia API: article wikitext (films) and lead extract (people).

Etiquette (https://meta.wikimedia.org/wiki/User-Agent_policy, API:Etiquette):
    - descriptive User-Agent with a contact URL
    - one request at a time, at least MIN_INTERVAL seconds apart
    - maxlag=5 on Wikipedia, Retry-After honoured on 429 / 5xx / maxlag
"""
import json
import logging
import time
from datetime import datetime, timezone
from urllib.parse import quote, unquote

import requests

log = logging.getLogger(__name__)

USER_AGENT = ("ece-imdb-enrichment/1.0 "
              "(https://github.com/Alexhuang03/Big-Data-Processing-application; "
              "student project)")
SPARQL_URL = "https://query.wikidata.org/sparql"
WIKIPEDIA_API = "https://en.wikipedia.org/w/api.php"
WIKIPEDIA_ARTICLE_PREFIX = "https://en.wikipedia.org/wiki/"

MIN_INTERVAL = 1.0          # seconds between two requests
MAX_RETRIES = 6
SPARQL_BATCH = {"films": 200, "people": 100}  # ids per SPARQL query
WIKITEXT_BATCH = 50         # API limit for revisions with several titles
EXTRACT_BATCH = 20          # API limit for intro extracts


def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def chunks(items, size):
    for start in range(0, len(items), size):
        yield items[start:start + size]


class PoliteClient:
    """Sequential HTTP client with rate limiting and retries."""

    def __init__(self, min_interval=MIN_INTERVAL):
        self.session = requests.Session()
        self.session.headers["User-Agent"] = USER_AGENT
        self.min_interval = min_interval
        self._last = 0.0
        self.requests_sent = 0

    def _wait_turn(self):
        delay = self.min_interval - (time.monotonic() - self._last)
        if delay > 0:
            time.sleep(delay)
        self._last = time.monotonic()

    def request(self, method, url, **kwargs):
        for attempt in range(MAX_RETRIES):
            self._wait_turn()
            self.requests_sent += 1
            try:
                response = self.session.request(method, url, timeout=90, **kwargs)
            except requests.RequestException as exc:
                wait = 5 * 2 ** attempt
                log.warning("Request error (%s), retry in %ss", exc, wait)
                time.sleep(wait)
                continue

            payload = None
            if response.status_code == 200:
                try:
                    payload = response.json()
                except ValueError:
                    # The SPARQL service sometimes cuts a slow response midway.
                    wait = 5 * 2 ** attempt
                    log.warning("Truncated response, retry in %ss", wait)
                    time.sleep(wait)
                    continue
                lagging = payload.get("error", {}).get("code") == "maxlag"
                if not lagging:
                    return payload
            if response.status_code in (200, 429, 500, 502, 503, 504):
                wait = int(response.headers.get("Retry-After", 5 * 2 ** attempt))
                log.warning("HTTP %s%s, retry in %ss", response.status_code,
                            " (maxlag)" if payload else "", wait)
                time.sleep(wait)
                continue
            response.raise_for_status()
        raise RuntimeError(f"Giving up on {url} after {MAX_RETRIES} attempts")

    def wikipedia_query(self, params):
        """Run an action=query request and merge all continuation pages."""
        base = {"action": "query", "format": "json", "formatversion": 2,
                "redirects": 1, "maxlag": 5, **params}
        merged = {"pages": {}, "normalized": [], "redirects": []}
        cont = {}
        while True:
            data = self.request("GET", WIKIPEDIA_API, params={**base, **cont})
            query = data.get("query", {})
            merged["normalized"] += query.get("normalized", [])
            merged["redirects"] += query.get("redirects", [])
            for page in query.get("pages", []):
                # Continuations return the same page with more properties.
                merged["pages"].setdefault(page["title"], {}).update(page)
            if "continue" not in data:
                return merged
            cont = data["continue"]


# ---------------------------------------------------------------------------
# JSON Lines cache
# ---------------------------------------------------------------------------

def load_done_keys(path, key):
    if not path.exists():
        return set()
    with path.open(encoding="utf-8") as handle:
        return {json.loads(line)[key] for line in handle if line.strip()}


def append_records(path, records):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def article_title_from_url(url):
    return unquote(url[len(WIKIPEDIA_ARTICLE_PREFIX):]).replace("_", " ")


def revision_url(title, revid):
    """Permanent link to the exact revision that was read (lineage)."""
    return (f"https://en.wikipedia.org/w/index.php?title="
            f"{quote(title.replace(' ', '_'))}&oldid={revid}")


def resolve_titles(result, titles):
    """Map each requested title to the final page title (after redirects)."""
    normalized = {n["from"]: n["to"] for n in result["normalized"]}
    redirects = {r["from"]: r["to"] for r in result["redirects"]}
    resolved = {}
    for title in titles:
        final = normalized.get(title, title)
        final = redirects.get(final, final)
        resolved[title] = final
    return resolved


# ---------------------------------------------------------------------------
# Wikidata: IMDb id -> Wikipedia article (+ facts about people)
# ---------------------------------------------------------------------------

FILM_QUERY = """
SELECT ?imdb ?item ?article WHERE {
  VALUES ?imdb { %s }
  ?item wdt:P345 ?imdb .
  OPTIONAL { ?article schema:about ?item ;
                      schema:isPartOf <https://en.wikipedia.org/> . }
}"""

PERSON_QUERY = """
SELECT ?imdb ?item ?article ?birth ?death ?birthPlace WHERE {
  VALUES ?imdb { %s }
  ?item wdt:P345 ?imdb .
  OPTIONAL { ?article schema:about ?item ;
                      schema:isPartOf <https://en.wikipedia.org/> . }
  OPTIONAL { ?item wdt:P569 ?birth . }
  OPTIONAL { ?item wdt:P570 ?death . }
  OPTIONAL { ?item wdt:P19 ?place .
             ?place rdfs:label ?birthPlace . FILTER(LANG(?birthPlace) = "en") }
}"""


def fetch_wikidata(client, ids, kind, path):
    """Append one record per IMDb id to path (status matched / no_wikidata_item)."""
    todo = sorted(set(ids) - load_done_keys(path, "imdb_id"))
    log.info("Wikidata %s: %d ids to fetch (%d cached)", kind, len(todo),
             len(set(ids)) - len(todo))
    template = PERSON_QUERY if kind == "people" else FILM_QUERY
    for batch in chunks(todo, SPARQL_BATCH[kind]):
        values = " ".join(f'"{imdb_id}"' for imdb_id in batch)
        data = client.request(
            "POST", SPARQL_URL,
            data={"query": template % values, "format": "json"},
            headers={"Accept": "application/sparql-results+json"})
        found = {}
        for row in data["results"]["bindings"]:
            imdb_id = row["imdb"]["value"]
            # Several rows per id are possible (many birth places...): keep first.
            found.setdefault(imdb_id, {k: v["value"] for k, v in row.items()})
        retrieved = now_iso()
        records = []
        for imdb_id in batch:
            row = found.get(imdb_id)
            records.append({
                "imdb_id": imdb_id,
                "status": "matched" if row else "no_wikidata_item",
                "wikidata_uri": row.get("item") if row else None,
                "wikipedia_url": row.get("article") if row else None,
                "birth_date": row.get("birth") if row else None,
                "death_date": row.get("death") if row else None,
                "birth_place": row.get("birthPlace") if row else None,
                "retrieved_at": retrieved,
            })
        append_records(path, records)


# ---------------------------------------------------------------------------
# Wikipedia: film wikitext and person lead extracts
# ---------------------------------------------------------------------------

def _wikipedia_targets(wikidata_path, done):
    """(imdb_id, article title) pairs that have an English article to fetch."""
    targets = []
    with wikidata_path.open(encoding="utf-8") as handle:
        for line in handle:
            record = json.loads(line)
            if record["wikipedia_url"] and record["imdb_id"] not in done:
                targets.append((record["imdb_id"],
                                article_title_from_url(record["wikipedia_url"])))
    return targets


def fetch_film_wikitext(client, wikidata_path, path):
    """Store the full wikitext of each film article (cleaned later in Spark)."""
    targets = _wikipedia_targets(wikidata_path, load_done_keys(path, "imdb_id"))
    log.info("Wikipedia films: %d articles to fetch", len(targets))
    for batch in chunks(targets, WIKITEXT_BATCH):
        titles = [title for _, title in batch]
        result = client.wikipedia_query({
            "prop": "revisions", "rvprop": "ids|content", "rvslots": "main",
            "titles": "|".join(titles)})
        resolved = resolve_titles(result, titles)
        retrieved = now_iso()
        records = []
        for imdb_id, title in batch:
            page = result["pages"].get(resolved[title], {})
            revision = (page.get("revisions") or [{}])[0]
            content = revision.get("slots", {}).get("main", {}).get("content")
            records.append({
                "imdb_id": imdb_id,
                "page_title": page.get("title", title),
                "revision_id": revision.get("revid"),
                "source_url": revision_url(page["title"], revision["revid"])
                if content else None,
                "wikitext": content,
                "status": "ok" if content else "page_missing",
                "retrieved_at": retrieved,
            })
        append_records(path, records)


def fetch_person_extracts(client, wikidata_path, path):
    """Store the plain-text lead section of each person article."""
    targets = _wikipedia_targets(wikidata_path, load_done_keys(path, "imdb_id"))
    log.info("Wikipedia people: %d articles to fetch", len(targets))
    for batch in chunks(targets, EXTRACT_BATCH):
        titles = [title for _, title in batch]
        result = client.wikipedia_query({
            "prop": "extracts|info", "exintro": 1, "explaintext": 1,
            "exlimit": EXTRACT_BATCH, "titles": "|".join(titles)})
        resolved = resolve_titles(result, titles)
        retrieved = now_iso()
        records = []
        for imdb_id, title in batch:
            page = result["pages"].get(resolved[title], {})
            extract = page.get("extract")
            revid = page.get("lastrevid")
            records.append({
                "imdb_id": imdb_id,
                "page_title": page.get("title", title),
                "revision_id": revid,
                "source_url": revision_url(page["title"], revid) if extract else None,
                "extract": extract,
                "status": "ok" if extract else "page_missing",
                "retrieved_at": retrieved,
            })
        append_records(path, records)

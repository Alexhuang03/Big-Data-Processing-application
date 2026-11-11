"""Description of the seven IMDb non-commercial tables.

Source: https://developer.imdb.com/non-commercial-datasets/
Each column is declared with its target Spark type. "array" columns are
stored in the TSV files as comma-separated strings.
"""

IMDB_BASE_URL = "https://datasets.imdbws.com"

# IMDb writes missing values as the two characters backslash + N.
IMDB_NULL = "\\N"

TABLES = {
    "title_basics": {
        "file": "title.basics.tsv.gz",
        "columns": [
            ("tconst", "string"),
            ("titleType", "string"),
            ("primaryTitle", "string"),
            ("originalTitle", "string"),
            ("isAdult", "int"),
            ("startYear", "int"),
            ("endYear", "int"),
            ("runtimeMinutes", "int"),
            ("genres", "array"),
        ],
        "primary_key": ["tconst"],
    },
    "title_akas": {
        "file": "title.akas.tsv.gz",
        "columns": [
            ("titleId", "string"),
            ("ordering", "int"),
            ("title", "string"),
            ("region", "string"),
            ("language", "string"),
            ("types", "array"),
            ("attributes", "array"),
            ("isOriginalTitle", "int"),
        ],
        "primary_key": ["titleId", "ordering"],
    },
    "title_crew": {
        "file": "title.crew.tsv.gz",
        "columns": [
            ("tconst", "string"),
            ("directors", "array"),
            ("writers", "array"),
        ],
        "primary_key": ["tconst"],
    },
    "title_episode": {
        "file": "title.episode.tsv.gz",
        "columns": [
            ("tconst", "string"),
            ("parentTconst", "string"),
            ("seasonNumber", "int"),
            ("episodeNumber", "int"),
        ],
        "primary_key": ["tconst"],
    },
    "title_principals": {
        "file": "title.principals.tsv.gz",
        "columns": [
            ("tconst", "string"),
            ("ordering", "int"),
            ("nconst", "string"),
            ("category", "string"),
            ("job", "string"),
            ("characters", "json_array"),
        ],
        "primary_key": ["tconst", "ordering"],
    },
    "title_ratings": {
        "file": "title.ratings.tsv.gz",
        "columns": [
            ("tconst", "string"),
            ("averageRating", "double"),
            ("numVotes", "int"),
        ],
        "primary_key": ["tconst"],
    },
    "name_basics": {
        "file": "name.basics.tsv.gz",
        "columns": [
            ("nconst", "string"),
            ("primaryName", "string"),
            ("birthYear", "int"),
            ("deathYear", "int"),
            ("primaryProfession", "array"),
            ("knownForTitles", "array"),
        ],
        "primary_key": ["nconst"],
    },
}

# Columns whose values in the file are not plain strings and need a cast.
CAST_TYPES = {"int", "double"}


def column_names(table):
    return [name for name, _ in TABLES[table]["columns"]]


def column_types(table):
    return dict(TABLES[table]["columns"])

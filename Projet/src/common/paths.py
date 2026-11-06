"""Storage locations shared by every pipeline step.

Layers:
    raw     original .tsv.gz files downloaded from IMDb
    bronze  same content as raw, all columns kept as strings, stored as Parquet
    silver  typed, cleaned and validated tables used by the analyses
"""
import os
from pathlib import Path

DATA_DIR = Path(os.environ.get("IMDB_DATA_DIR", "/home/jovyan/data"))
OUTPUT_DIR = Path(os.environ.get("IMDB_OUTPUT_DIR", "/home/jovyan/output"))

RAW_DIR = DATA_DIR / "raw"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"


def bronze_path(table):
    return str(BRONZE_DIR / table)


def silver_path(table):
    return str(SILVER_DIR / table)


def output_dir(section):
    path = OUTPUT_DIR / section
    path.mkdir(parents=True, exist_ok=True)
    return path

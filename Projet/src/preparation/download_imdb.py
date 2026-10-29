"""Download the seven IMDb non-commercial dataset files into the raw layer.

Usage:
    python -m preparation.download_imdb [--force]

Files already present are skipped unless --force is given.
"""
import argparse
import shutil
import time
import urllib.request

from common.paths import RAW_DIR
from common.schemas import IMDB_BASE_URL, TABLES

USER_AGENT = "ece-big-data-project/1.0 (student project)"


def download(url, target):
    tmp = target.with_suffix(target.suffix + ".part")
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    start = time.time()
    with urllib.request.urlopen(request, timeout=60) as response, open(tmp, "wb") as out:
        shutil.copyfileobj(response, out, length=1024 * 1024)
    tmp.rename(target)
    size_mb = target.stat().st_size / 1024 / 1024
    print(f"  {target.name}: {size_mb:.0f} MB in {time.time() - start:.0f} s")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--force", action="store_true", help="re-download existing files")
    args = parser.parse_args()

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    for table in TABLES.values():
        target = RAW_DIR / table["file"]
        if target.exists() and not args.force:
            print(f"  {target.name}: already downloaded, skipped")
            continue
        download(f"{IMDB_BASE_URL}/{table['file']}", target)
    print(f"Raw files are in {RAW_DIR}")


if __name__ == "__main__":
    main()

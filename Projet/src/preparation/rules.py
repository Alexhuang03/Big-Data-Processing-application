"""Thresholds shared by the quality checks and the cleaning job."""
import datetime

CURRENT_YEAR = datetime.date.today().year
# First known motion pictures date from the late 1870s / 1880s.
FIRST_FILM_YEAR = 1874
MAX_LIFESPAN = 120
# 24 hours: longer runtimes exist (experimental films) but are outliers.
MAX_RUNTIME_MINUTES = 24 * 60
SERIES_TYPES = ["tvSeries", "tvMiniSeries"]
ARCHIVE_CATEGORIES = ["archive_footage", "archive_sound"]
POSTHUMOUS_MARGIN_YEARS = 10

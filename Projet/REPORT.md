# ECE Big Data Processing Project: IMDb Report

All results below come from the full IMDb non-commercial dataset and were
produced by the code in `src/`. Every table is also stored as CSV in `output/`.
See `PROJECT.md` to run the code again.

## Table of contents

1. [Data preparation](#1-data-preparation)
2. [Data exploration](#2-data-exploration)
3. [Data enrichment](#3-data-enrichment)
4. [Streaming data processing](#4-streaming-data-processing)
5. [Open question](#5-open-question)

## 1. Data preparation

### 1.1 Missing values

Five columns with the highest share of null, missing or empty values (`output/preparation/top5_null_columns.md`):

| table | column | rows | missing_count | missing_pct |
| --- | --- | --- | --- | --- |
| title_akas | attributes | 60003988 | 59687497 | 99.47 |
| title_basics | endYear | 12907853 | 12744147 | 98.73 |
| name_basics | deathYear | 15774367 | 15509398 | 98.32 |
| name_basics | birthYear | 15774367 | 15089556 | 95.66 |
| title_principals | job | 102574272 | 82768514 | 80.69 |

Missing values for every column of every table (`output/preparation/null_report.md`):

| table | column | type | rows | null_count | marker_count | empty_count | missing_count | missing_pct | invalid_count | invalid_pct |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| title_basics | tconst | string | 12907853 | 0 | 0 | 0 | 0 | 0.00 | 0 | 0.00 |
| title_basics | titleType | string | 12907853 | 0 | 0 | 0 | 0 | 0.00 | 0 | 0.00 |
| title_basics | primaryTitle | string | 12907853 | 0 | 0 | 0 | 0 | 0.00 | 0 | 0.00 |
| title_basics | originalTitle | string | 12907853 | 0 | 0 | 0 | 0 | 0.00 | 0 | 0.00 |
| title_basics | isAdult | int | 12907853 | 0 | 0 | 0 | 0 | 0.00 | 0 | 0.00 |
| title_basics | startYear | int | 12907853 | 0 | 1487544 | 0 | 1487544 | 11.52 | 0 | 0.00 |
| title_basics | endYear | int | 12907853 | 0 | 12744147 | 0 | 12744147 | 98.73 | 0 | 0.00 |
| title_basics | runtimeMinutes | int | 12907853 | 0 | 8038970 | 0 | 8038970 | 62.28 | 0 | 0.00 |
| title_basics | genres | array | 12907853 | 0 | 546853 | 0 | 546853 | 4.24 | 0 | 0.00 |
| title_akas | titleId | string | 60003988 | 0 | 0 | 0 | 0 | 0.00 | 0 | 0.00 |
| title_akas | ordering | int | 60003988 | 0 | 0 | 0 | 0 | 0.00 | 0 | 0.00 |
| title_akas | title | string | 60003988 | 0 | 0 | 0 | 0 | 0.00 | 0 | 0.00 |
| title_akas | region | string | 60003988 | 0 | 12948283 | 0 | 12948283 | 21.58 | 0 | 0.00 |
| title_akas | language | string | 60003988 | 0 | 19563407 | 0 | 19563407 | 32.60 | 0 | 0.00 |
| title_akas | types | array | 60003988 | 0 | 40153455 | 0 | 40153455 | 66.92 | 0 | 0.00 |
| title_akas | attributes | array | 60003988 | 0 | 59687497 | 0 | 59687497 | 99.47 | 0 | 0.00 |
| title_akas | isOriginalTitle | int | 60003988 | 0 | 0 | 0 | 0 | 0.00 | 0 | 0.00 |
| title_crew | tconst | string | 12910620 | 0 | 0 | 0 | 0 | 0.00 | 0 | 0.00 |
| title_crew | directors | array | 12910620 | 0 | 5755093 | 0 | 5755093 | 44.58 | 0 | 0.00 |
| title_crew | writers | array | 12910620 | 0 | 6395627 | 0 | 6395627 | 49.54 | 0 | 0.00 |
| title_episode | tconst | string | 9985716 | 0 | 0 | 0 | 0 | 0.00 | 0 | 0.00 |
| title_episode | parentTconst | string | 9985716 | 0 | 0 | 0 | 0 | 0.00 | 0 | 0.00 |
| title_episode | seasonNumber | int | 9985716 | 0 | 2098978 | 0 | 2098978 | 21.02 | 0 | 0.00 |
| title_episode | episodeNumber | int | 9985716 | 0 | 2098978 | 0 | 2098978 | 21.02 | 0 | 0.00 |
| title_principals | tconst | string | 102574272 | 0 | 0 | 0 | 0 | 0.00 | 0 | 0.00 |
| title_principals | ordering | int | 102574272 | 0 | 0 | 0 | 0 | 0.00 | 0 | 0.00 |
| title_principals | nconst | string | 102574272 | 0 | 0 | 0 | 0 | 0.00 | 0 | 0.00 |
| title_principals | category | string | 102574272 | 0 | 0 | 0 | 0 | 0.00 | 0 | 0.00 |
| title_principals | job | string | 102574272 | 0 | 82768514 | 0 | 82768514 | 80.69 | 0 | 0.00 |
| title_principals | characters | json_array | 102574272 | 0 | 52532803 | 0 | 52532803 | 51.21 | 0 | 0.00 |
| title_ratings | tconst | string | 1726786 | 0 | 0 | 0 | 0 | 0.00 | 0 | 0.00 |
| title_ratings | averageRating | double | 1726786 | 0 | 0 | 0 | 0 | 0.00 | 0 | 0.00 |
| title_ratings | numVotes | int | 1726786 | 0 | 0 | 0 | 0 | 0.00 | 0 | 0.00 |
| name_basics | nconst | string | 15774367 | 0 | 0 | 0 | 0 | 0.00 | 0 | 0.00 |
| name_basics | primaryName | string | 15774367 | 0 | 90 | 0 | 90 | 0.00 | 0 | 0.00 |
| name_basics | birthYear | int | 15774367 | 0 | 15089556 | 0 | 15089556 | 95.66 | 0 | 0.00 |
| name_basics | deathYear | int | 15774367 | 0 | 15509398 | 0 | 15509398 | 98.32 | 0 | 0.00 |
| name_basics | primaryProfession | array | 15774367 | 0 | 3206143 | 0 | 3206143 | 20.33 | 0 | 0.00 |
| name_basics | knownForTitles | array | 15774367 | 0 | 1910855 | 0 | 1910855 | 12.11 | 0 | 0.00 |

### 1.2 Other data quality issues

Quality checks on the typed data, before cleaning (`output/preparation/quality_issues_typed.md`):

| check | table | rule | issue_count | table_rows | issue_pct | handling |
| --- | --- | --- | --- | --- | --- | --- |
| malformed_rows_title_basics | title_basics | rows without the expected number of tab-separated fields | 0 | 12907853 | 0.0000 | row dropped |
| malformed_rows_title_akas | title_akas | rows without the expected number of tab-separated fields | 0 | 60003988 | 0.0000 | row dropped |
| malformed_rows_title_crew | title_crew | rows without the expected number of tab-separated fields | 0 | 12910620 | 0.0000 | row dropped |
| malformed_rows_title_episode | title_episode | rows without the expected number of tab-separated fields | 0 | 9985716 | 0.0000 | row dropped |
| malformed_rows_title_principals | title_principals | rows without the expected number of tab-separated fields | 0 | 102574272 | 0.0000 | row dropped |
| malformed_rows_title_ratings | title_ratings | rows without the expected number of tab-separated fields | 0 | 1726786 | 0.0000 | row dropped |
| malformed_rows_name_basics | name_basics | rows without the expected number of tab-separated fields | 0 | 15774367 | 0.0000 | row dropped |
| untrimmed_primary_title | title_basics | primaryTitle with leading/trailing spaces | 0 | 12907853 | 0.0000 | value trimmed |
| untrimmed_primary_name | name_basics | primaryName with leading/trailing spaces | 0 | 15774367 | 0.0000 | value trimmed |
| duplicate_key_title_basics | title_basics | duplicate primary key (tconst) | 0 | 12907853 | 0.0000 | first row kept |
| duplicate_key_title_akas | title_akas | duplicate primary key (titleId, ordering) | 0 | 60003988 | 0.0000 | first row kept |
| duplicate_key_title_crew | title_crew | duplicate primary key (tconst) | 0 | 12910620 | 0.0000 | first row kept |
| duplicate_key_title_episode | title_episode | duplicate primary key (tconst) | 0 | 9985716 | 0.0000 | first row kept |
| duplicate_key_title_principals | title_principals | duplicate primary key (tconst, ordering) | 0 | 102574272 | 0.0000 | first row kept |
| duplicate_key_title_ratings | title_ratings | duplicate primary key (tconst) | 0 | 1726786 | 0.0000 | first row kept |
| duplicate_key_name_basics | name_basics | duplicate primary key (nconst) | 0 | 15774367 | 0.0000 | first row kept |
| death_before_birth | name_basics | deathYear < birthYear | 27 | 15774367 | 0.0002 | birthYear and deathYear set to null, row flagged |
| implausible_lifespan | name_basics | deathYear - birthYear > 120 | 2 | 15774367 | 0.0000 | kept, row flagged |
| alive_older_than_max_lifespan | name_basics | no deathYear but born before 1906 | 2793 | 15774367 | 0.0177 | kept, row flagged (death year probably missing) |
| future_birth_or_death | name_basics | birthYear or deathYear after 2026 | 0 | 15774367 | 0.0000 | value set to null, row flagged |
| end_before_start | title_basics | endYear < startYear | 0 | 12907853 | 0.0000 | endYear set to null, row flagged |
| start_year_too_early | title_basics | startYear < 1874 | 0 | 12907853 | 0.0000 | startYear set to null, row flagged |
| runtime_not_positive | title_basics | runtimeMinutes <= 0 | 4 | 12907853 | 0.0000 | runtimeMinutes set to null, row flagged |
| runtime_outlier | title_basics | runtimeMinutes > 1440 | 210 | 12907853 | 0.0016 | kept, row flagged |
| is_adult_invalid | title_basics | isAdult not in (0, 1) | 0 | 12907853 | 0.0000 | isAdult set to null, row flagged |
| rating_out_of_range | title_ratings | averageRating outside [1, 10] or numVotes <= 0 | 0 | 1726786 | 0.0000 | row dropped |
| episode_is_own_parent | title_episode | tconst = parentTconst | 0 | 9985716 | 0.0000 | row dropped |
| season_or_episode_not_positive | title_episode | seasonNumber <= 0 or episodeNumber <= 0 | 7113 | 9985716 | 0.0712 | value set to null, row flagged |
| episode_parent_not_series | title_episode | parent title is not a tvSeries / tvMiniSeries | 0 | 9985716 | 0.0000 | kept, row flagged |
| episode_before_series_start | title_episode | episode startYear < series startYear | 3507 | 9985716 | 0.0351 | kept, row flagged |
| principal_born_after_release | title_principals | title startYear < person birthYear | 3104 | 102574272 | 0.0030 | kept, row flagged |
| principal_long_after_death | title_principals | title released > 10 years after death (not archive footage/sound) | 165250 | 102574272 | 0.1611 | kept, row flagged |
| orphan_akas_title | title_akas | titleId not in title_basics | 0 | 60003988 | 0.0000 | row dropped |
| orphan_crew_title | title_crew | tconst not in title_basics | 2774 | 12910620 | 0.0215 | row dropped |
| orphan_crew_director | title_crew | director nconst not in name_basics | 420 | 12910620 | 0.0033 | id removed from the array |
| orphan_crew_writer | title_crew | writer nconst not in name_basics | 1147 | 12910620 | 0.0089 | id removed from the array |
| orphan_episode_title | title_episode | tconst not in title_basics | 0 | 9985716 | 0.0000 | row dropped |
| orphan_episode_parent | title_episode | parentTconst not in title_basics | 5 | 9985716 | 0.0001 | row dropped |
| orphan_principal_title | title_principals | tconst not in title_basics | 0 | 102574272 | 0.0000 | row dropped |
| orphan_principal_name | title_principals | nconst not in name_basics | 7192 | 102574272 | 0.0070 | row dropped |
| orphan_rating_title | title_ratings | tconst not in title_basics | 0 | 1726786 | 0.0000 | row dropped |
| orphan_known_for_title | name_basics | knownForTitles id not in title_basics | 725 | 15774367 | 0.0046 | id removed from the array |

The same checks on the silver layer, after cleaning (`output/preparation/quality_issues_silver.md`):

| check | table | rule | issue_count | table_rows | issue_pct | handling |
| --- | --- | --- | --- | --- | --- | --- |
| duplicate_key_title_basics | title_basics | duplicate primary key (tconst) | 0 | 12907853 | 0.0000 | first row kept |
| duplicate_key_title_akas | title_akas | duplicate primary key (titleId, ordering) | 0 | 60003988 | 0.0000 | first row kept |
| duplicate_key_title_crew | title_crew | duplicate primary key (tconst) | 0 | 12907846 | 0.0000 | first row kept |
| duplicate_key_title_episode | title_episode | duplicate primary key (tconst) | 0 | 9985711 | 0.0000 | first row kept |
| duplicate_key_title_principals | title_principals | duplicate primary key (tconst, ordering) | 0 | 102567080 | 0.0000 | first row kept |
| duplicate_key_title_ratings | title_ratings | duplicate primary key (tconst) | 0 | 1726786 | 0.0000 | first row kept |
| duplicate_key_name_basics | name_basics | duplicate primary key (nconst) | 0 | 15774367 | 0.0000 | first row kept |
| death_before_birth | name_basics | deathYear < birthYear | 0 | 15774367 | 0.0000 | birthYear and deathYear set to null, row flagged |
| implausible_lifespan | name_basics | deathYear - birthYear > 120 | 2 | 15774367 | 0.0000 | kept, row flagged |
| alive_older_than_max_lifespan | name_basics | no deathYear but born before 1906 | 2793 | 15774367 | 0.0177 | kept, row flagged (death year probably missing) |
| future_birth_or_death | name_basics | birthYear or deathYear after 2026 | 0 | 15774367 | 0.0000 | value set to null, row flagged |
| end_before_start | title_basics | endYear < startYear | 0 | 12907853 | 0.0000 | endYear set to null, row flagged |
| start_year_too_early | title_basics | startYear < 1874 | 0 | 12907853 | 0.0000 | startYear set to null, row flagged |
| runtime_not_positive | title_basics | runtimeMinutes <= 0 | 0 | 12907853 | 0.0000 | runtimeMinutes set to null, row flagged |
| runtime_outlier | title_basics | runtimeMinutes > 1440 | 210 | 12907853 | 0.0016 | kept, row flagged |
| is_adult_invalid | title_basics | isAdult not in (0, 1) | 0 | 12907853 | 0.0000 | isAdult set to null, row flagged |
| rating_out_of_range | title_ratings | averageRating outside [1, 10] or numVotes <= 0 | 0 | 1726786 | 0.0000 | row dropped |
| episode_is_own_parent | title_episode | tconst = parentTconst | 0 | 9985711 | 0.0000 | row dropped |
| season_or_episode_not_positive | title_episode | seasonNumber <= 0 or episodeNumber <= 0 | 0 | 9985711 | 0.0000 | value set to null, row flagged |
| episode_parent_not_series | title_episode | parent title is not a tvSeries / tvMiniSeries | 0 | 9985711 | 0.0000 | kept, row flagged |
| episode_before_series_start | title_episode | episode startYear < series startYear | 3507 | 9985711 | 0.0351 | kept, row flagged |
| principal_born_after_release | title_principals | title startYear < person birthYear | 3102 | 102567080 | 0.0030 | kept, row flagged |
| principal_long_after_death | title_principals | title released > 10 years after death (not archive footage/sound) | 165250 | 102567080 | 0.1611 | kept, row flagged |
| orphan_akas_title | title_akas | titleId not in title_basics | 0 | 60003988 | 0.0000 | row dropped |
| orphan_crew_title | title_crew | tconst not in title_basics | 0 | 12907846 | 0.0000 | row dropped |
| orphan_crew_director | title_crew | director nconst not in name_basics | 0 | 12907846 | 0.0000 | id removed from the array |
| orphan_crew_writer | title_crew | writer nconst not in name_basics | 0 | 12907846 | 0.0000 | id removed from the array |
| orphan_episode_title | title_episode | tconst not in title_basics | 0 | 9985711 | 0.0000 | row dropped |
| orphan_episode_parent | title_episode | parentTconst not in title_basics | 0 | 9985711 | 0.0000 | row dropped |
| orphan_principal_title | title_principals | tconst not in title_basics | 0 | 102567080 | 0.0000 | row dropped |
| orphan_principal_name | title_principals | nconst not in name_basics | 0 | 102567080 | 0.0000 | row dropped |
| orphan_rating_title | title_ratings | tconst not in title_basics | 0 | 1726786 | 0.0000 | row dropped |
| orphan_known_for_title | name_basics | knownForTitles id not in title_basics | 0 | 15774367 | 0.0000 | id removed from the array |

Example rows for each issue:

- Before cleaning: [`output/preparation/quality_samples_typed.md`](output/preparation/quality_samples_typed.md)
- After cleaning: [`output/preparation/quality_samples_silver.md`](output/preparation/quality_samples_silver.md)

### 1.3 Handling of the issues

> *To be written by the team (no AI tools allowed in this part).*

### 1.4 Methodological explanation

> *To be written by the team (no AI tools allowed in this part).*

Points to cover:

- Method used and why it was chosen
- Alternatives considered and trade-offs made
- How missing values were measured (`\N`, empty strings, empty arrays)
- Why the raw / bronze / silver layers
- Why some bad rows were fixed, flagged (`quality_flags`) or dropped

## 2. Data exploration

All questions use feature films: `titleType = movie` and runtime over 40 minutes.

### 2.1 Q1: Movies whose runtime is far from their genre average

Outliers per genre (`output/exploration/q1_genre_summary.md`):

| genre | movies | mean_runtime | std_runtime | q1_runtime | median_runtime | q3_runtime | outliers | longer | shorter | outlier_pct |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Drama | 196423 | 96.06 | 37.07 | 81 | 93 | 107 | 1966 | 1966 | 0 | 1.00 |
| Documentary | 115860 | 78.05 | 223.74 | 57 | 74 | 90 | 58 | 58 | 0 | 0.05 |
| Comedy | 96484 | 93.14 | 28.70 | 82 | 91 | 102 | 1615 | 1615 | 0 | 1.67 |
| Romance | 42414 | 99.07 | 25.08 | 85 | 95 | 110 | 2042 | 1813 | 229 | 4.81 |
| Action | 40302 | 101.21 | 33.75 | 85 | 94 | 113 | 838 | 838 | 0 | 2.08 |
| Crime | 34565 | 95.52 | 25.57 | 83 | 92 | 105 | 1174 | 1165 | 9 | 3.40 |
| Thriller | 34013 | 96.11 | 21.74 | 85 | 93 | 105 | 2084 | 1386 | 698 | 6.13 |
| Horror | 30667 | 88.60 | 22.29 | 79 | 88 | 96 | 702 | 696 | 6 | 2.29 |
| Adventure | 24031 | 93.01 | 30.57 | 79 | 90 | 101 | 633 | 633 | 0 | 2.63 |
| Mystery | 16459 | 94.35 | 50.40 | 81 | 92 | 105 | 52 | 52 | 0 | 0.32 |
| Biography | 16088 | 89.22 | 29.40 | 69 | 88 | 104 | 455 | 455 | 0 | 2.83 |
| Family | 15926 | 92.33 | 32.69 | 77 | 90 | 102 | 281 | 281 | 0 | 1.76 |
| Music | 13937 | 88.44 | 57.74 | 68 | 87 | 100 | 97 | 97 | 0 | 0.70 |
| History | 13611 | 94.31 | 52.89 | 72 | 90 | 108 | 143 | 143 | 0 | 1.05 |
| Fantasy | 12697 | 93.96 | 31.87 | 80 | 91 | 104 | 241 | 241 | 0 | 1.90 |
| Sci-Fi | 9806 | 91.21 | 30.45 | 79 | 90 | 100 | 147 | 147 | 0 | 1.50 |
| Musical | 8486 | 99.99 | 33.30 | 82 | 94 | 111 | 238 | 238 | 0 | 2.80 |
| Animation | 8317 | 82.65 | 26.79 | 68 | 81 | 93 | 175 | 175 | 0 | 2.10 |
| War | 8041 | 94.79 | 31.26 | 80 | 92 | 105 | 218 | 218 | 0 | 2.71 |
| Sport | 6801 | 88.95 | 37.40 | 71 | 90 | 100 | 99 | 99 | 0 | 1.46 |
| Western | 6374 | 76.54 | 28.71 | 57 | 70 | 90 | 151 | 151 | 0 | 2.37 |
| Adult | 6118 | 79.47 | 30.11 | 66 | 77 | 85 | 138 | 138 | 0 | 2.26 |
| News | 1409 | 77.60 | 32.17 | 60 | 75 | 88 | 27 | 27 | 0 | 1.92 |
| Film-Noir | 876 | 81.89 | 13.77 | 71 | 82 | 91 | 31 | 28 | 3 | 3.54 |
| Reality-TV | 299 | 91.78 | 84.83 | 64 | 90 | 96 | 2 | 2 | 0 | 0.67 |
| Talk-Show | 104 | 87.64 | 33.56 | 60 | 79 | 112 | 5 | 5 | 0 | 4.81 |
| Game-Show | 8 | 80.50 | 32.84 | 52 | 67 | 87 | 1 | 1 | 0 | 12.50 |

- Examples of outlier films: [`output/exploration/q1_outliers_examples.md`](output/exploration/q1_outliers_examples.md)
- Full list of outlier films: [`output/exploration/q1_outliers.csv`](output/exploration/q1_outliers.csv)

#### Key takeaways

> *To be written by the team (no AI tools allowed in this part).*

#### Methodological explanation

> *To be written by the team (no AI tools allowed in this part).*

Points to cover:

- Method used and why it was chosen
- Alternatives considered and trade-offs made
- Why z-score (and IQR as an option), which threshold

### 2.2 Q2: Top three genres per year (1980 to 2025)

How often each genre is in the yearly top three (`output/exploration/q2_top3_frequency.md`):

| genre | years_in_top3 | years_ranked_first | first_year | last_year |
| --- | --- | --- | --- | --- |
| Drama | 46 | 42 | 1980 | 2025 |
| Comedy | 34 | 2 | 1985 | 2025 |
| Action | 29 | 1 | 1982 | 2025 |
| Adventure | 16 | 1 | 1980 | 2021 |
| Crime | 9 | 0 | 1990 | 1999 |
| Horror | 2 | 0 | 1980 | 1981 |
| Thriller | 1 | 0 | 2006 | 2006 |
| Sci-Fi | 1 | 0 | 1982 | 1982 |

- Top three genres for each year: [`output/exploration/q2_top3_genres_by_year.md`](output/exploration/q2_top3_genres_by_year.md)

#### Key takeaways

> *To be written by the team (no AI tools allowed in this part).*

#### Methodological explanation

> *To be written by the team (no AI tools allowed in this part).*

Points to cover:

- Method used and why it was chosen
- Alternatives considered and trade-offs made
- How popularity was defined (number of votes, rating, number of films...)

### 2.3 Q3: Top three genres compared to the other genres

Summary over 1980 to 2025 (`output/exploration/q3_top3_vs_rest_summary.md`):

| metric | average_1980_2025 |
| --- | --- |
| top3_genres | 3.00 |
| top3_movies | 4676.98 |
| top3_votes | 29517868.13 |
| top3_share_pct | 44.75 |
| top3_votes_per_movie | 8173.79 |
| top3_avg_rating | 5.91 |
| rest_genres | 20.76 |
| rest_movies | 7571.22 |
| rest_votes | 37463184.91 |
| rest_share_pct | 55.25 |
| rest_votes_per_movie | 8148.06 |
| rest_avg_rating | 6.15 |
| votes_per_movie_ratio | 1.09 |
| rating_gap | -0.24 |

- Comparison for each year: [`output/exploration/q3_top3_vs_rest_by_year.md`](output/exploration/q3_top3_vs_rest_by_year.md)

#### Key takeaways

> *To be written by the team (no AI tools allowed in this part).*

#### Methodological explanation

> *To be written by the team (no AI tools allowed in this part).*

Points to cover:

- Method used and why it was chosen
- Alternatives considered and trade-offs made

### 2.4 Q4: One-hit wonder directors

Top directors (`output/exploration/q4_one_hit_directors_top.md`):

| nconst | director_name | hit_title | hit_year | hit_rating | other_movies_count | other_avg_rating | rating_gap | total_movies | hit_tconst |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| nm2840629 | Steve Rahaman | To Be King | 2011 | 8.60 | 2 | 3.00 | 5.60 | 3 | tt1813719 |
| nm3136866 | Kevin Van Stevenson | Short Fuse | 2026 | 8.80 | 5 | 3.28 | 5.52 | 6 | tt33303912 |
| nm7263882 | Swaylee Loughnane | Mother's Child | 2020 | 8.20 | 4 | 2.70 | 5.50 | 5 | tt10123970 |
| nm4276395 | John Migliore | The Friday Night Death Slot | 2015 | 8.00 | 7 | 2.66 | 5.34 | 8 | tt4696666 |
| nm3655122 | Jastis Arimba | Jomblo Fi Sabilillah | 2023 | 9.20 | 3 | 3.90 | 5.30 | 4 | tt27648500 |
| nm0464424 | Gábor Koltay | István, a király | 1984 | 8.30 | 2 | 3.00 | 5.30 | 3 | tt0087488 |
| nm0843785 | William Szarka | Muttnik | 2009 | 9.10 | 2 | 3.85 | 5.25 | 3 | tt1553086 |
| nm10898010 | Robbie Lopez | River Beauty | 2023 | 8.30 | 3 | 3.07 | 5.23 | 4 | tt20764956 |
| nm1069500 | Sam Logan Khaleghi | Love Is a Thieves' Game | 2011 | 8.60 | 2 | 3.40 | 5.20 | 3 | tt1380912 |
| nm0728375 | Michael Rissi | Annabel Lee | 2009 | 8.80 | 3 | 3.83 | 4.97 | 4 | tt1285211 |
| nm18399764 | Kodukula Srinivasa Rao | Kadapa Redamma | 1990 | 9.30 | 2 | 4.35 | 4.95 | 3 | tt0361788 |
| nm0150503 | Georges Chamchoum | Journey into my Faith | 2012 | 9.10 | 2 | 4.20 | 4.90 | 3 | tt2215187 |
| nm5670681 | Vincent Zhou | The Truth | 2022 | 8.10 | 2 | 3.30 | 4.80 | 3 | tt17373462 |
| nm0448831 | Randy Kent | Road King | 2023 | 9.20 | 5 | 4.48 | 4.72 | 6 | tt4971262 |
| nm14464956 | Kinnedy 'Scribecash' Storay | Lord Have Mercy II Director's Cut | 2024 | 8.70 | 2 | 4.00 | 4.70 | 3 | tt32431536 |
| nm9619857 | Andrey Nikiforov | Skazka o poteryannom vremeni | 2026 | 8.90 | 4 | 4.23 | 4.67 | 5 | tt34830388 |
| nm7799059 | Jaimin Bal | Downup the Exit 796 | 2018 | 9.30 | 2 | 4.65 | 4.65 | 3 | tt5280308 |
| nm2594614 | Danny LeGare | Not My Dog | 2026 | 8.10 | 5 | 3.46 | 4.64 | 6 | tt34769247 |
| nm3583561 | Larry Rosen | Starting Over | 2013 | 8.70 | 11 | 4.10 | 4.60 | 12 | tt1621966 |
| nm3078850 | Edward Payson | Our Friend Jon | 2020 | 9.30 | 7 | 4.71 | 4.59 | 8 | tt4023464 |

- Full list: [`output/exploration/q4_one_hit_directors.csv`](output/exploration/q4_one_hit_directors.csv)

#### Key takeaways

> *To be written by the team (no AI tools allowed in this part).*

#### Methodological explanation

> *To be written by the team (no AI tools allowed in this part).*

Points to cover:

- Method used and why it was chosen
- Alternatives considered and trade-offs made
- Minimum number of votes used, if any

### 2.5 Q5: TV series that decline in their last season

Top series (`output/exploration/q5_declining_tv_series_top.md`):

| series_id | series_title | startYear | endYear | total_seasons | qualifying_seasons_count | total_episodes | best_season | best_ep_num | best_ep_title | best_rating | worst_season | worst_ep_num | worst_ep_title | worst_rating | rating_drop |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| tt4508902 | One Punch Man | 2015 |  | 3 | 3 | 36 | 1 | 12 | The Strongest Hero | 9.50 | 3 | 6 | Motley Heroes | 1.60 | 7.90 |
| tt8560918 | The Queen of Flow | 2018 | 2026 | 3 | 3 | 237 | 1 | 82 | Episode #1.82 | 9.60 | 3 | 65 | Episode #3.65 | 1.90 | 7.70 |
| tt34174211 | Celebrity Substitute | 2024 |  | 3 | 3 | 36 | 1 | 2 | LISA forms a New Girl Group | 9.60 | 3 | 5 | Tom Holland Films an Action Movie with Kids | 2.30 | 7.30 |
| tt1856010 | House of Cards | 2013 | 2018 | 6 | 5 | 73 | 2 | 13 | Chapter 26 | 9.50 | 6 | 8 | Chapter 73 | 2.50 | 7.00 |
| tt0050022 | The Gumby Show | 1956 | 1969 | 3 | 3 | 110 | 1 | 10 | The Eggs and Trixie | 8.10 | 3 | 70 | Point of Honor | 1.40 | 6.70 |
| tt0200337 | Family Law | 1999 | 2002 | 3 | 3 | 68 | 2 | 6 | Telling Lies, Conclusion | 9.40 | 3 | 12 | Blood and Water | 2.70 | 6.70 |
| tt0118252 | All Dogs Go to Heaven: The Series | 1996 | 1999 | 3 | 3 | 40 | 2 | 10 | Sidekicked | 9.20 | 3 | 12 | The Wrong Stuff | 2.60 | 6.60 |
| tt33145195 | Margarita | 2024 | 2026 | 3 | 3 | 80 | 1 | 26 | Enamorhada | 9.40 | 3 | 20 | Que tu cuento valga la pena | 2.90 | 6.50 |
| tt19638434 | FRO Thizzle Reviews | 2022 |  | 5 | 5 | 395 | 1 | 82 | Shaun of the Dead | 10.00 | 5 | 25 | Obsession | 3.60 | 6.40 |
| tt5189554 | Soy Luna | 2016 | 2026 | 4 | 4 | 232 | 2 | 80 | La final de la Rodafest en México, sobre ruedas (Parte 2) | 9.10 | 4 | 12 | Alas en los pies y un corazón expectante | 2.80 | 6.30 |
| tt0108804 | High Tide | 1994 | 1997 | 3 | 3 | 72 | 1 | 17 | Dead in the Water | 8.40 | 3 | 15 | Little Black Book | 2.20 | 6.20 |
| tt0856757 | Leaf Cast | 2006 | 2010 | 5 | 5 | 174 | 1 | 38 | Bölüm 38 | 8.30 | 5 | 3 | Bölüm 162 | 2.40 | 5.90 |
| tt0122813 | Bob and Margaret | 1993 | 2001 | 4 | 4 | 53 | 1 | 2 | A Night In | 8.60 | 4 | 1 | New Lease on Life | 2.70 | 5.90 |
| tt3107288 | The Flash | 2014 | 2023 | 9 | 9 | 184 | 1 | 15 | Out of Time | 9.60 | 9 | 6 | The Good, the Bad and the Lucky | 3.80 | 5.80 |
| tt23735438 | Love Is Blind: Sweden | 2024 |  | 3 | 3 | 35 | 1 | 10 | The Reunion | 7.70 | 3 | 9 | Reactions: Living Together | 1.90 | 5.80 |
| tt22199742 | Rykter | 2022 | 2025 | 4 | 4 | 60 | 2 | 15 | Alt eller ingenting | 9.20 | 4 | 5 | Løgnaktige bitch | 3.60 | 5.60 |
| tt3520702 | Bloodline | 2015 | 2017 | 3 | 3 | 33 | 1 | 12 | Part 12 | 9.10 | 3 | 9 | Part 32 | 3.60 | 5.50 |
| tt0154063 | Chiquititas | 1995 | 2001 | 6 | 6 | 1150 | 1 | 1 | Episode #1.1 | 9.20 | 6 | 1 | Episode #6.1 | 3.80 | 5.40 |
| tt2039304 | Your Favorite Martian | 2010 | 2024 | 3 | 3 | 59 | 1 | 7 | Orphan Tears | 9.20 | 3 | 13 | Telescope | 4.00 | 5.20 |
| tt0126179 | Women: Stories of Passion | 1996 | 1999 | 3 | 3 | 39 | 2 | 4 | The Bitter and the Sweet | 7.70 | 3 | 12 | Angel from the Sky | 2.60 | 5.10 |

- Full list: [`output/exploration/q5_declining_tv_series.csv`](output/exploration/q5_declining_tv_series.csv)

#### Key takeaways

> *To be written by the team (no AI tools allowed in this part).*

#### Methodological explanation

> *To be written by the team (no AI tools allowed in this part).*

Points to cover:

- Method used and why it was chosen
- Alternatives considered and trade-offs made
- Handling of ties for best and worst episode

### 2.6 Q6: Actors successful in several languages

Number of actors per number of languages (`output/exploration/q6_multilingual_summary.md`):

| num_languages | actor_count |
| --- | --- |
| 2 | 2303 |
| 3 | 190 |
| 4 | 19 |
| 5 | 1 |

Top actors, 2 languages or more (`output/exploration/q6_multilingual_actors_2plus_top.md`):

| nconst | actor_name | num_languages | languages_str | successful_films_count | avg_rating | total_votes |
| --- | --- | --- | --- | --- | --- | --- |
| nm0462407 | Sebastian Koch | 5 | de, en, fr, ja, yue | 6 | 7.57 | 585088 |
| nm0080238 | Tanikella Bharani | 4 | en, fr, hi, qbn | 47 | 7.64 | 243793 |
| nm0000706 | Michelle Yeoh | 4 | cmn, en, fr, yue | 11 | 7.26 | 2613577 |
| nm0004486 | Bruno Ganz | 4 | de, en, fr, sv | 9 | 7.44 | 504972 |
| nm2573928 | Kana Hanazawa | 4 | cmn, en, fr, ja | 7 | 7.41 | 87919 |
| nm0000052 | Marcello Mastroianni | 4 | en, fr, ja, sr | 7 | 7.40 | 234055 |
| nm0784038 | Şener Şen | 4 | en, fr, ja, tr | 6 | 7.81 | 176536 |
| nm0319843 | Daniel Giménez Cacho | 4 | en, es, fr, sv | 6 | 7.38 | 240230 |
| nm0504897 | Tony Leung Chiu-wai | 4 | bg, en, fr, yue | 5 | 7.72 | 1019014 |
| nm0226699 | Bogdan Diklic | 4 | bs, en, hr, ja | 5 | 7.60 | 69537 |
| nm0814280 | Song Kang-ho | 4 | en, es, fr, tr | 5 | 7.54 | 839831 |
| nm1254805 | Manolo Solo | 4 | en, es, fr, hi | 5 | 7.42 | 871100 |
| nm2569233 | Yûki Kaji | 4 | en, fr, hi, ja | 5 | 7.42 | 95322 |
| nm1914298 | Pål Sverre Hagen | 4 | en, fr, ja, sv | 5 | 7.26 | 129162 |
| nm0159507 | Stephen Chow | 4 | en, es, fr, yue | 5 | 7.23 | 287179 |
| nm3426369 | Yui Ishikawa | 4 | en, fr, hi, ja | 4 | 8.10 | 182076 |
| nm0342458 | Nikolay Grinko | 4 | az, bg, en, uk | 4 | 7.95 | 274368 |
| nm0151654 | Chang Chen | 4 | bg, cmn, en, fr | 4 | 7.45 | 367858 |
| nm0458460 | Burghart Klaußner | 4 | de, en, fr, sv | 4 | 7.23 | 184784 |
| nm0785842 | Toni Servillo | 4 | ca, en, fr, it | 4 | 7.15 | 131910 |
| nm0000821 | Amitabh Bachchan | 3 | en, es, fr | 50 | 7.45 | 1572274 |
| nm2496992 | Ajay | 3 | en, hi, qbn | 30 | 7.47 | 260955 |
| nm0000026 | Cary Grant | 3 | en, fr, qbn | 30 | 7.41 | 1279508 |
| nm0000158 | Tom Hanks | 3 | en, fr, nl | 29 | 7.64 | 16641245 |
| nm0000020 | Henry Fonda | 3 | en, es, fr | 26 | 7.47 | 1548775 |

Top actors, 3 languages or more (`output/exploration/q6_multilingual_actors_3plus_top.md`):

| nconst | actor_name | num_languages | languages_str | successful_films_count | avg_rating | total_votes |
| --- | --- | --- | --- | --- | --- | --- |
| nm0462407 | Sebastian Koch | 5 | de, en, fr, ja, yue | 6 | 7.57 | 585088 |
| nm0080238 | Tanikella Bharani | 4 | en, fr, hi, qbn | 47 | 7.64 | 243793 |
| nm0000706 | Michelle Yeoh | 4 | cmn, en, fr, yue | 11 | 7.26 | 2613577 |
| nm0004486 | Bruno Ganz | 4 | de, en, fr, sv | 9 | 7.44 | 504972 |
| nm2573928 | Kana Hanazawa | 4 | cmn, en, fr, ja | 7 | 7.41 | 87919 |
| nm0000052 | Marcello Mastroianni | 4 | en, fr, ja, sr | 7 | 7.40 | 234055 |
| nm0784038 | Şener Şen | 4 | en, fr, ja, tr | 6 | 7.81 | 176536 |
| nm0319843 | Daniel Giménez Cacho | 4 | en, es, fr, sv | 6 | 7.38 | 240230 |
| nm0504897 | Tony Leung Chiu-wai | 4 | bg, en, fr, yue | 5 | 7.72 | 1019014 |
| nm0226699 | Bogdan Diklic | 4 | bs, en, hr, ja | 5 | 7.60 | 69537 |
| nm0814280 | Song Kang-ho | 4 | en, es, fr, tr | 5 | 7.54 | 839831 |
| nm1254805 | Manolo Solo | 4 | en, es, fr, hi | 5 | 7.42 | 871100 |
| nm2569233 | Yûki Kaji | 4 | en, fr, hi, ja | 5 | 7.42 | 95322 |
| nm1914298 | Pål Sverre Hagen | 4 | en, fr, ja, sv | 5 | 7.26 | 129162 |
| nm0159507 | Stephen Chow | 4 | en, es, fr, yue | 5 | 7.23 | 287179 |
| nm3426369 | Yui Ishikawa | 4 | en, fr, hi, ja | 4 | 8.10 | 182076 |
| nm0342458 | Nikolay Grinko | 4 | az, bg, en, uk | 4 | 7.95 | 274368 |
| nm0151654 | Chang Chen | 4 | bg, cmn, en, fr | 4 | 7.45 | 367858 |
| nm0458460 | Burghart Klaußner | 4 | de, en, fr, sv | 4 | 7.23 | 184784 |
| nm0785842 | Toni Servillo | 4 | ca, en, fr, it | 4 | 7.15 | 131910 |
| nm0000821 | Amitabh Bachchan | 3 | en, es, fr | 50 | 7.45 | 1572274 |
| nm2496992 | Ajay | 3 | en, hi, qbn | 30 | 7.47 | 260955 |
| nm0000026 | Cary Grant | 3 | en, fr, qbn | 30 | 7.41 | 1279508 |
| nm0000158 | Tom Hanks | 3 | en, fr, nl | 29 | 7.64 | 16641245 |
| nm0000020 | Henry Fonda | 3 | en, es, fr | 26 | 7.47 | 1548775 |

- Full list, 2 languages or more: [`output/exploration/q6_multilingual_actors_2plus.csv`](output/exploration/q6_multilingual_actors_2plus.csv)
- Full list, 3 languages or more: [`output/exploration/q6_multilingual_actors_3plus.csv`](output/exploration/q6_multilingual_actors_3plus.csv)

#### Key takeaways

> *To be written by the team (no AI tools allowed in this part).*

#### Methodological explanation

> *To be written by the team (no AI tools allowed in this part).*

Points to cover:

- Method used and why it was chosen
- Alternatives considered and trade-offs made
- How the original language of a film was found
- How a successful film was defined

### 2.7 Q7: Actors whose popularity peaked early

Top actors (`output/exploration/q7_early_peak_actors_top.md`):

| nconst | actor_name | career_start | career_end | career_span | total_films | early_avg_rating | late_avg_rating | early_peak_score |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| nm0025618 | Mahesh Anand | 1984 | 2019 | 35 | 44 | 5.43 | 1.90 | 3.53 |
| nm0562770 | Ken Maynard | 1924 | 1970 | 46 | 39 | 6.17 | 2.70 | 3.47 |
| nm0576455 | Mehmood Jr. | 1969 | 2025 | 56 | 19 | 6.50 | 3.10 | 3.40 |
| nm0930695 | Grant Williams | 1956 | 1976 | 20 | 15 | 6.29 | 2.90 | 3.38 |
| nm0437053 | Kanakalatha | 1986 | 2017 | 31 | 17 | 6.23 | 2.90 | 3.33 |
| nm0936295 | Norman Wisdom | 1953 | 2002 | 49 | 18 | 6.32 | 3.20 | 3.12 |
| nm0006742 | Helen Vita | 1956 | 1997 | 41 | 20 | 5.78 | 2.70 | 3.08 |
| nm0043728 | Oya Aydogan | 1976 | 2016 | 40 | 17 | 5.87 | 2.93 | 2.94 |
| nm0281486 | Charles Fleischer | 1984 | 2025 | 41 | 16 | 6.26 | 3.37 | 2.89 |
| nm0141330 | Sunset Carson | 1944 | 1985 | 41 | 19 | 6.99 | 4.20 | 2.79 |
| nm0613098 | Caroline Munro | 1972 | 2020 | 48 | 19 | 5.59 | 2.82 | 2.78 |
| nm0049395 | Nandamuri Balakrishna | 1974 | 2025 | 51 | 57 | 7.09 | 4.34 | 2.74 |
| nm1261215 | Fathali Oveisi | 1987 | 2015 | 28 | 15 | 6.41 | 3.68 | 2.73 |
| nm0518579 | Ulli Lommel | 1964 | 2020 | 56 | 29 | 5.64 | 2.91 | 2.73 |
| nm0479017 | Lash La Rue | 1945 | 1987 | 42 | 24 | 6.11 | 3.40 | 2.71 |
| nm0007147 | Dev Anand | 1949 | 2003 | 54 | 60 | 7.12 | 4.45 | 2.67 |
| nm0351352 | Coşkun Göğen | 1974 | 2022 | 48 | 29 | 5.33 | 2.70 | 2.63 |
| nm0434085 | Olga Kabo | 1986 | 2012 | 26 | 16 | 6.06 | 3.45 | 2.61 |
| nm9969854 | Rekha | 1986 | 2026 | 40 | 34 | 7.36 | 4.75 | 2.60 |
| nm1085951 | Lorelei Linklater | 2001 | 2026 | 25 | 17 | 7.60 | 5.03 | 2.57 |
| nm0767741 | Tom Savini | 1977 | 2015 | 38 | 22 | 6.13 | 3.57 | 2.56 |
| nm1559072 | Kollam Ajith | 1986 | 2012 | 26 | 17 | 7.24 | 4.69 | 2.55 |
| nm0619870 | Reza Naji | 1997 | 2025 | 28 | 21 | 7.13 | 4.59 | 2.54 |
| nm0454448 | Andrea King | 1944 | 1994 | 50 | 24 | 6.49 | 3.95 | 2.54 |
| nm0469107 | Natalya Krachkovskaya | 1971 | 2003 | 32 | 17 | 7.37 | 4.84 | 2.53 |

- Full list: [`output/exploration/q7_early_peak_actors.csv`](output/exploration/q7_early_peak_actors.csv)

#### Key takeaways

> *To be written by the team (no AI tools allowed in this part).*

#### Methodological explanation

> *To be written by the team (no AI tools allowed in this part).*

Points to cover:

- Method used and why it was chosen
- Alternatives considered and trade-offs made
- How early and late career were split

### 2.8 Q8: Multi-talented film professionals

Top multi-talented professionals (`output/exploration/q8_multi_talented_top.md`):

| nconst | person_name | role_combination | total_films | multi_role_film_count | avg_rating | avg_votes |
| --- | --- | --- | --- | --- | --- | --- |
| nm0001238 | Jesús Franco | writer+director+actor | 180 | 149 | 4.74 | 699 |
| nm0939147 | Jing Wong | writer+director+actor | 197 | 111 | 5.69 | 935 |
| nm0945282 | Yōji Yamada | writer+director | 85 | 77 | 6.83 | 788 |
| nm0654839 | Mariano Ozores | writer+director | 83 | 67 | 4.89 | 149 |
| nm0197929 | Giannis Dalianidis | writer+director+actor | 78 | 64 | 5.94 | 408 |
| nm0061792 | Godfrey Ho | writer+director+actor | 110 | 63 | 4.57 | 219 |
| nm0000419 | Jean-Luc Godard | writer+director+actor | 75 | 61 | 6.37 | 6241 |
| nm0889402 | Carlo Vanzina | writer+director | 73 | 57 | 4.87 | 572 |
| nm0151653 | Cheh Chang | writer+director | 86 | 56 | 6.43 | 758 |
| nm0434318 | Kader Khan | writer+actor | 224 | 55 | 5.46 | 954 |
| nm0000095 | Woody Allen | writer+director+actor | 64 | 54 | 6.73 | 60842 |
| nm5954636 | Sergey A. | writer+director+actor | 55 | 53 | 6.75 | 428 |
| nm0781261 | Osman F. Seden | writer+director+actor | 111 | 52 | 5.94 | 757 |
| nm0826642 | Steno | writer+director | 102 | 52 | 6.04 | 577 |
| nm0245213 | Julien Duvivier | writer+director | 57 | 50 | 6.74 | 1174 |
| nm0765430 | Joseph W. Sarno | writer+director | 57 | 49 | 5.38 | 231 |
| nm0001031 | Claude Chabrol | writer+director+actor | 77 | 47 | 6.31 | 4881 |
| nm5472963 | Brett Bentman | writer+director+actor | 50 | 47 | 3.49 | 241 |
| nm0757157 | Alekos Sakellarios | writer+director+actor | 65 | 46 | 6.57 | 695 |
| nm0500988 | Claude Lelouch | writer+director+actor | 53 | 46 | 6.32 | 1433 |
| nm0598102 | Mario Monicelli | writer+director+actor | 89 | 45 | 6.57 | 1876 |
| nm0006620 | Jean-Pierre Mocky | writer+director+actor | 54 | 45 | 5.82 | 536 |
| nm0728271 | Dino Risi | writer+director | 61 | 44 | 6.37 | 7276 |
| nm0001348 | Werner Herzog | writer+director+actor | 55 | 44 | 6.96 | 20783 |
| nm0002194 | Pupi Avati | writer+director | 51 | 44 | 6.15 | 2126 |

Ratings when holding several roles on the same film (`output/exploration/q8_same_film_performance.md`):

| role_type | person_film_count | avg_rating | avg_numVotes |
| --- | --- | --- | --- |
| multi_role | 137907 | 5.89 | 5970 |
| single_role | 285287 | 5.88 | 15723 |

- All profiles: [`output/exploration/q8_multi_talented_profiles.csv`](output/exploration/q8_multi_talented_profiles.csv)

#### Key takeaways

> *To be written by the team (no AI tools allowed in this part).*

#### Methodological explanation

> *To be written by the team (no AI tools allowed in this part).*

Points to cover:

- Method used and why it was chosen
- Alternatives considered and trade-offs made

## 3. Data enrichment

Film synopsis coverage (`output/enrichment/film_coverage.md`):

| result | count | pct |
| --- | --- | --- |
| enriched | 5970 | 83.10 |
| no Wikidata item linked to this IMDb id | 499 | 6.95 |
| no plot section, lead section used | 394 | 5.48 |
| no English Wikipedia article | 320 | 4.45 |
| article has no usable text | 1 | 0.01 |

Film coverage by decade (`output/enrichment/film_coverage_by_decade.md`):

| decade | films | with_synopsis_pct | with_plot_pct |
| --- | --- | --- | --- |
| 1980 | 1505 | 87.77 | 80.80 |
| 1990 | 1530 | 89.08 | 83.07 |
| 2000 | 1504 | 91.69 | 86.90 |
| 2010 | 1663 | 87.79 | 82.86 |
| 2020 | 982 | 85.64 | 81.26 |

Principal biography coverage (`output/enrichment/principal_coverage.md`):

| result | count | pct |
| --- | --- | --- |
| enriched | 7783 | 71.25 |
| no Wikidata item linked to this IMDb id | 2009 | 18.39 |
| no English Wikipedia article (Wikidata facts only) | 1130 | 10.35 |
| Wikipedia page could not be read | 1 | 0.01 |

Samples of enriched records:

- Films: [`output/enrichment/film_sample.md`](output/enrichment/film_sample.md)
- Principals: [`output/enrichment/principal_sample.md`](output/enrichment/principal_sample.md)

#### Key takeaways

> *To be written by the team (no AI tools allowed in this part).*

#### Methodological explanation

> *To be written by the team (no AI tools allowed in this part).*

Points to cover:

- Method used and why it was chosen
- Alternatives considered and trade-offs made
- Choice of source (Wikipedia, Wikidata) and how films and people were matched
- Respect of rate limits and API policies, lineage field
- What runs outside Spark and what runs in Spark
- How the code scales to the full dataset

## 4. Streaming data processing

Metrics of each streaming query (20-minute test window) (`output/streaming/streaming_metrics.md`):

| query | micro_batches | non_empty_batches | input_rows | avg_input_rows_per_s | avg_processed_rows_per_s | avg_batch_ms | rows_dropped_by_watermark | last_watermark |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| matched_events | 41 | 40 | 1486 | 1.25 | 34.11 | 1110.65 | 0 | 2026-11-11T12:50:25.000Z |
| matched_windows | 41 | 40 | 1486 | 1.25 | 38.96 | 954.55 | 0 | 2026-11-11T12:50:25.000Z |
| all_windows | 41 | 40 | 1486 | 1.25 | 45.87 | 813.67 | 0 | 2026-11-11T12:50:25.000Z |

Matched events by match type (`output/streaming/matches_by_type.md`):

| match_type | entity_type | events | distinct_entities |
| --- | --- | --- | --- |
| exact_title | film | 26 | 11 |
| exact_name | person | 20 | 16 |
| film_title | film | 3 | 2 |
| film_title_year | film | 2 | 2 |
| person_disambiguated | person | 1 | 1 |

Most edited IMDb films and people (`output/streaming/top_matched_entities.md`):

| entity_type | imdb_id | imdb_name | edits | editors | wikipedia_title |
| --- | --- | --- | --- | --- | --- |
| film | tt0054892 | Francis of Assisi | 8 | 1 | Francis of Assisi |
| film | tt32321803 | The Get Out | 4 | 1 | The Get Out |
| film | tt0385842 | Wolf | 4 | 1 | Wolf |
| person | nm0476202 | Yayoi Kusama | 3 | 1 | Yayoi Kusama |
| film | tt13423468 | Attention Please | 2 | 1 | Attention Please (film) |
| person | nm0002546 | Mena Suvari | 2 | 2 | Mena Suvari |
| film | tt1206885 | Rambo: Last Blood | 2 | 1 | Rambo: Last Blood |
| person | nm10684771 | Samreen Kaur | 2 | 1 | Samreen Kaur |
| film | tt42897565 | Yen Ennai Edho Seidhai | 2 | 1 | Yen Ennai Edho Seidhai |
| film | tt1666185 | All Eyez on Me | 1 | 1 | All Eyez on Me |
| person | nm0197642 | Anders Dale | 1 | 1 | Anders Dale |
| person | nm3237449 | Brandon O'Neill | 1 | 1 | Brandon O'Neill |
| film | tt6272704 | Breath | 1 | 1 | Breath (2016 film) |
| person | nm11875179 | Cao Yang | 1 | 1 | Cao Yang |
| person | nm7736637 | Carmine Giordano | 1 | 1 | Carmine Giordano |
| person | nm2599617 | Emma Fryer | 1 | 1 | Emma Fryer |
| person | nm1406740 | Florian Habicht | 1 | 1 | Florian Habicht |
| person | nm0670688 | Harvey Pekar | 1 | 1 | Harvey Pekar |
| person | nm0728567 | Jack Ritchie | 1 | 1 | Jack Ritchie |
| person | nm2595047 | Junaid Khan | 1 | 1 | Junaid Khan (Indian actor) |

Delay between the Wikipedia edit and its processing (`output/streaming/matched_delay_summary.md`):

| matched_events | first_event | last_event | median_delay_s | p99_delay_s | max_delay_s |
| --- | --- | --- | --- | --- | --- |
| 52 | 2026-11-11 12:33:12 | 2026-11-11 12:52:34 | 5.22 | 15.89 | 17.61 |

Matched edits per time window (`output/streaming/matched_windows.md`):

| window_start | window_end | match_type | events | distinct_users | distinct_pages | avg_delay_seconds | max_delay_seconds |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 2026-11-11 12:30:00 | 2026-11-11 12:35:00 | exact_name | 1 | 1 | 1 | 4.58 | 4.58 |
| 2026-11-11 12:30:00 | 2026-11-11 12:35:00 | exact_title | 1 | 1 | 1 | 4.72 | 4.72 |
| 2026-11-11 12:35:00 | 2026-11-11 12:40:00 | exact_name | 3 | 3 | 2 | 5.19 | 6.40 |
| 2026-11-11 12:35:00 | 2026-11-11 12:40:00 | exact_title | 13 | 7 | 7 | 6.01 | 10.08 |
| 2026-11-11 12:35:00 | 2026-11-11 12:40:00 | film_title_year | 2 | 2 | 2 | 6.93 | 10.05 |
| 2026-11-11 12:35:00 | 2026-11-11 12:40:00 | person_disambiguated | 1 | 1 | 1 | 6.12 | 6.12 |
| 2026-11-11 12:40:00 | 2026-11-11 12:45:00 | exact_name | 6 | 4 | 5 | 8.17 | 14.23 |
| 2026-11-11 12:40:00 | 2026-11-11 12:45:00 | exact_title | 6 | 2 | 3 | 8.09 | 13.67 |
| 2026-11-11 12:45:00 | 2026-11-11 12:50:00 | exact_name | 9 | 8 | 8 | 6.20 | 13.01 |
| 2026-11-11 12:45:00 | 2026-11-11 12:50:00 | exact_title | 3 | 3 | 3 | 12.57 | 17.61 |
| 2026-11-11 12:45:00 | 2026-11-11 12:50:00 | film_title | 1 | 1 | 1 | 3.70 | 3.70 |

All edits per time window (`output/streaming/all_windows.md`):

| window_start | window_end | events | distinct_users | distinct_pages | avg_delay_seconds | max_delay_seconds |
| --- | --- | --- | --- | --- | --- | --- |
| 2026-11-11 12:30:00 | 2026-11-11 12:35:00 | 184 | 79 | 159 | 5.50 | 62.88 |
| 2026-11-11 12:35:00 | 2026-11-11 12:40:00 | 377 | 145 | 339 | 4.98 | 33.39 |
| 2026-11-11 12:40:00 | 2026-11-11 12:45:00 | 361 | 150 | 328 | 4.95 | 23.45 |
| 2026-11-11 12:45:00 | 2026-11-11 12:50:00 | 373 | 159 | 333 | 5.27 | 39.28 |

- Sample of matched events: [`output/streaming/matched_events_sample.md`](output/streaming/matched_events_sample.md)

#### Key takeaways

> *To be written by the team (no AI tools allowed in this part).*

#### Methodological explanation

> *To be written by the team (no AI tools allowed in this part).*

Points to cover:

- Method used and why it was chosen
- Alternatives considered and trade-offs made
- Filtering strategy and how events are matched to IMDb
- **Handling of late data** (required): event time, watermark, window size, deduplication
- Known limits (false positives of exact title matches)

## 5. Open question

### 5.1 Question

> *To be written by the team (no AI tools allowed in this part).*

Points to cover: the question, why it is interesting, extra data used (if any).

### 5.2 Results

Summary (`output/open_question/open_question_macro_summary.md`):

| total_creative_duos | positive_synergy_duos | negative_synergy_duos | positive_synergy_pct | average_synergy_delta | average_vote_multiplier |
| --- | --- | --- | --- | --- | --- |
| 6055 | 3773 | 2230 | 62.31 | 0.16 | 1.32 |

Duos with the highest synergy (`output/open_question/open_question_top_synergy_duos.md`):

| director_name | actor_name | collab_count | duo_avg_rating | expected_baseline_rating | synergy_rating_delta | vote_multiplier | sample_titles_str |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Vicky Jenson | Mike Myers | 3 | 7.90 | 3.48 | 4.42 | -1.35 | Shrek |
| Ali Abdel-Khalek | Mahmoud Abdel Aziz | 7 | 7.83 | 3.95 | 3.88 | -3.09 | Gary el-Wuhoosh; Idam Mayyet; The High |
| Barry Mahon | Hal Smith | 3 | 6.60 | 3.04 | 3.56 | 1.49 | Santa and the Three Bears |
| Viktor Shamirov | Gosha Kutsenko | 4 | 6.98 | 3.56 | 3.42 | 0.59 | And Here's What's Happening to Me; Dikari; Game of Truth |
| Terry Jones | Graham Chapman | 6 | 8.05 | 4.78 | 3.27 | -1.11 | Monty Python and the Holy Grail; Monty Python's Life of Brian |
| Ashok G. | Nani | 3 | 7.80 | 4.66 | 3.14 | 0.71 | Pilla Zamindar |
| Jon Watts | Tom Holland | 6 | 7.60 | 4.47 | 3.13 | -0.78 | Spider-Man: Far from Home; Spider-Man: Homecoming; Spider-Man: No Way Home |
| James Frawley | Jim Henson | 3 | 7.60 | 4.50 | 3.10 | -1.90 | The Muppet Movie |
| Nina Wels | Dela Dabulamanzi | 3 | 6.20 | 3.15 | 3.05 | 0.48 | Tafiti: Across the Desert |
| T.K. Rajeev Kumar | Mohanlal | 3 | 6.97 | 3.93 | 3.04 | 0.53 | Oru Naal Varum; Pavithram |
| Tso Nam Lee | Shan Charng | 3 | 7.00 | 4.10 | 2.90 | 3.59 | Shaolin vs. Lama |
| Kelly Makin | Dave Foley | 3 | 6.90 | 4.05 | 2.85 | 0.32 | Kids in the Hall: Brain Candy |
| Josef Rusnak | Craig Bierko | 3 | 7.00 | 4.29 | 2.71 | -33.71 | The Thirteenth Floor |
| Jeff Wamester | Jeff Bennett | 3 | 7.80 | 5.18 | 2.62 | 1.73 | Batman: Knightfall - Part 1: Knightfall |
| Kinji Fukasaku | Tatsuo Umemiya | 3 | 7.20 | 4.60 | 2.60 | 0.26 | Cops vs. Thugs; Graveyard of Honor; Police Tactics |
| Joe Russo | Robert Downey Jr. | 6 | 8.20 | 5.62 | 2.58 | -5.35 | Avengers: Endgame; Avengers: Infinity War; Captain America: Civil War |
| Anthony Russo | Robert Downey Jr. | 6 | 8.20 | 5.62 | 2.58 | -5.35 | Avengers: Endgame; Avengers: Infinity War; Captain America: Civil War |
| Francis Ford Coppola | Diane Keaton | 3 | 8.57 | 6.05 | 2.52 | 28.65 | The Godfather; The Godfather Part II; The Godfather Part III |
| Hulki Saner | Çolpan İlhan | 3 | 7.00 | 4.48 | 2.52 | 1.33 | Turist Ömer: Dümenciler Krali; Ömer the Tourist; Ömer the Tourist in Germany |
| Singeetam Srinivasa Rao | Kamal Haasan | 11 | 7.85 | 5.34 | 2.51 | -0.77 | Apoorva Sagodharargal; Kaathala Kaathala; Michael Madana Kama Rajan |

Duos with the lowest synergy (`output/open_question/open_question_negative_synergy_duos.md`):

| director_name | actor_name | collab_count | duo_avg_rating | expected_baseline_rating | synergy_rating_delta | vote_multiplier | sample_titles_str |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Rex Piano | Tom Kenny | 3 | 1.90 | 6.67 | -4.77 | 0.03 | Elf Bowling the Movie: The Great North Pole Elf Strike |
| Ribhu Dasgupta | Parineeti Chopra | 3 | 3.67 | 7.68 | -4.01 | 0.71 | Code Name: Tiranga; The Girl on the Train |
| Wiktor Grodecki | Cezary Pazura | 3 | 4.40 | 8.38 | -3.98 | 0.12 | Insatiability |
| Bob Clark | Jon Voight | 3 | 1.93 | 5.84 | -3.90 | 0.52 | Superbabies: Baby Geniuses 2; The Karate Dog |
| Y.V.S. Chowdary | Simran | 3 | 3.00 | 6.86 | -3.86 | 0.22 | Okka Magadu; Yuvaraju |
| P. Vasu | Nandamuri Balakrishna | 3 | 2.10 | 5.61 | -3.51 | 0.16 | Maharadhi |
| Rodolfo Ledo | Emilio Disi | 3 | 1.67 | 5.09 | -3.43 | 0.74 | Bañeros 4: Los rompeolas; Bañeros III, todopoderosos; Explosive Brigade: Pirate Mission |
| Ramesh Varma | Ravi Teja | 3 | 4.27 | 7.58 | -3.32 | 0.95 | Khiladi; Veera |
| Harry L. Fraser | Ray Corrigan | 3 | 2.80 | 6.11 | -3.31 | 0.69 | The White Gorilla |
| Moacyr Góes | Xuxa | 5 | 2.86 | 6.01 | -3.15 | 0.81 | Bruja and the Lost City of Gold; Xuxa in Abracadabra; Xuxinha and Guto Against the Space Monsters |
| Doga Can Anafarta | Altan Erkekli | 3 | 2.27 | 5.42 | -3.15 | 0.74 | Enes Batur: Gerçek Kahraman; The Chaos Class Again; The Chaos Class: Summer Games |
| A.P. Arjun | Dhruva Sarja | 3 | 3.57 | 6.68 | -3.11 | -1.53 | Addhuri; Martin |
| Kartal Tibet | Mehmet Ali Erbil | 3 | 2.20 | 5.21 | -3.01 | 2.81 | The Chaos Class Welcome; Turks in Space |
| Jack Hill | Julissa | 3 | 3.53 | 6.48 | -2.94 | 0.25 | Fear Chamber; House of Evil; Isle of the Snake People |
| Joyce Bernal | Vice Ganda | 3 | 4.00 | 6.87 | -2.87 | 0.37 | Gandarrapiddo: The Revenger Squad; The Super Parental Guardians |
| Alfonso Brescia | Yanti Somer | 4 | 2.90 | 5.76 | -2.86 | 0.14 | Battle of the Stars; Star Odyssey; The War of the Robots |
| Sangeeth Sivan | Dharmendra | 3 | 3.50 | 6.21 | -2.71 | 0.72 | Yamla Pagla Deewana 2 |
| Christophe Gans | Hannah Emily Anderson | 3 | 4.00 | 6.52 | -2.52 | 0.47 | Return to Silent Hill |
| Steven M. Smith | Mark Wingett | 3 | 2.60 | 5.02 | -2.42 | 0.05 | Dead Again; Doll House; I Am Hooligan |
| Meher Ramesh | Venkatesh Daggubati | 3 | 3.50 | 5.92 | -2.42 | 0.17 | Shadow |

#### Key takeaways

> *To be written by the team (no AI tools allowed in this part).*

#### Methodological explanation

> *To be written by the team (no AI tools allowed in this part).*

Points to cover:

- Method used and why it was chosen
- Alternatives considered and trade-offs made

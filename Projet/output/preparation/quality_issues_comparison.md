| check | table | rule | before | before_pct | after | after_pct | handling |
| --- | --- | --- | --- | --- | --- | --- | --- |
| malformed_rows_title_basics | title_basics | rows without the expected number of tab-separated fields | 0 | 0.0000 |  |  | row dropped |
| malformed_rows_title_akas | title_akas | rows without the expected number of tab-separated fields | 0 | 0.0000 |  |  | row dropped |
| malformed_rows_title_crew | title_crew | rows without the expected number of tab-separated fields | 0 | 0.0000 |  |  | row dropped |
| malformed_rows_title_episode | title_episode | rows without the expected number of tab-separated fields | 0 | 0.0000 |  |  | row dropped |
| malformed_rows_title_principals | title_principals | rows without the expected number of tab-separated fields | 0 | 0.0000 |  |  | row dropped |
| malformed_rows_title_ratings | title_ratings | rows without the expected number of tab-separated fields | 0 | 0.0000 |  |  | row dropped |
| malformed_rows_name_basics | name_basics | rows without the expected number of tab-separated fields | 0 | 0.0000 |  |  | row dropped |
| untrimmed_primary_title | title_basics | primaryTitle with leading/trailing spaces | 0 | 0.0000 |  |  | value trimmed |
| untrimmed_primary_name | name_basics | primaryName with leading/trailing spaces | 0 | 0.0000 |  |  | value trimmed |
| duplicate_key_title_basics | title_basics | duplicate primary key (tconst) | 0 | 0.0000 | 0 | 0.0000 | first row kept |
| duplicate_key_title_akas | title_akas | duplicate primary key (titleId, ordering) | 0 | 0.0000 | 0 | 0.0000 | first row kept |
| duplicate_key_title_crew | title_crew | duplicate primary key (tconst) | 0 | 0.0000 | 0 | 0.0000 | first row kept |
| duplicate_key_title_episode | title_episode | duplicate primary key (tconst) | 0 | 0.0000 | 0 | 0.0000 | first row kept |
| duplicate_key_title_principals | title_principals | duplicate primary key (tconst, ordering) | 0 | 0.0000 | 0 | 0.0000 | first row kept |
| duplicate_key_title_ratings | title_ratings | duplicate primary key (tconst) | 0 | 0.0000 | 0 | 0.0000 | first row kept |
| duplicate_key_name_basics | name_basics | duplicate primary key (nconst) | 0 | 0.0000 | 0 | 0.0000 | first row kept |
| death_before_birth | name_basics | deathYear < birthYear | 27 | 0.0002 | 0 | 0.0000 | birthYear and deathYear set to null, row flagged |
| implausible_lifespan | name_basics | deathYear - birthYear > 120 | 2 | 0.0000 | 2 | 0.0000 | kept, row flagged |
| alive_older_than_max_lifespan | name_basics | no deathYear but born before 1906 | 2793 | 0.0177 | 2793 | 0.0177 | kept, row flagged (death year probably missing) |
| future_birth_or_death | name_basics | birthYear or deathYear after 2026 | 0 | 0.0000 | 0 | 0.0000 | value set to null, row flagged |
| end_before_start | title_basics | endYear < startYear | 0 | 0.0000 | 0 | 0.0000 | endYear set to null, row flagged |
| start_year_too_early | title_basics | startYear < 1874 | 0 | 0.0000 | 0 | 0.0000 | startYear set to null, row flagged |
| runtime_not_positive | title_basics | runtimeMinutes <= 0 | 4 | 0.0000 | 0 | 0.0000 | runtimeMinutes set to null, row flagged |
| runtime_outlier | title_basics | runtimeMinutes > 1440 | 210 | 0.0016 | 210 | 0.0016 | kept, row flagged |
| is_adult_invalid | title_basics | isAdult not in (0, 1) | 0 | 0.0000 | 0 | 0.0000 | isAdult set to null, row flagged |
| rating_out_of_range | title_ratings | averageRating outside [1, 10] or numVotes <= 0 | 0 | 0.0000 | 0 | 0.0000 | row dropped |
| episode_is_own_parent | title_episode | tconst = parentTconst | 0 | 0.0000 | 0 | 0.0000 | row dropped |
| season_or_episode_not_positive | title_episode | seasonNumber <= 0 or episodeNumber <= 0 | 7113 | 0.0712 | 0 | 0.0000 | value set to null, row flagged |
| episode_parent_not_series | title_episode | parent title is not a tvSeries / tvMiniSeries | 0 | 0.0000 | 0 | 0.0000 | kept, row flagged |
| episode_before_series_start | title_episode | episode startYear < series startYear | 3507 | 0.0351 | 3507 | 0.0351 | kept, row flagged |
| principal_born_after_release | title_principals | title startYear < person birthYear | 3104 | 0.0030 | 3102 | 0.0030 | kept, row flagged |
| principal_long_after_death | title_principals | title released > 10 years after death (not archive footage/sound) | 165250 | 0.1611 | 165250 | 0.1611 | kept, row flagged |
| orphan_akas_title | title_akas | titleId not in title_basics | 0 | 0.0000 | 0 | 0.0000 | row dropped |
| orphan_crew_title | title_crew | tconst not in title_basics | 2774 | 0.0215 | 0 | 0.0000 | row dropped |
| orphan_crew_director | title_crew | director nconst not in name_basics | 420 | 0.0033 | 0 | 0.0000 | id removed from the array |
| orphan_crew_writer | title_crew | writer nconst not in name_basics | 1147 | 0.0089 | 0 | 0.0000 | id removed from the array |
| orphan_episode_title | title_episode | tconst not in title_basics | 0 | 0.0000 | 0 | 0.0000 | row dropped |
| orphan_episode_parent | title_episode | parentTconst not in title_basics | 5 | 0.0001 | 0 | 0.0000 | row dropped |
| orphan_principal_title | title_principals | tconst not in title_basics | 0 | 0.0000 | 0 | 0.0000 | row dropped |
| orphan_principal_name | title_principals | nconst not in name_basics | 7192 | 0.0070 | 0 | 0.0000 | row dropped |
| orphan_rating_title | title_ratings | tconst not in title_basics | 0 | 0.0000 | 0 | 0.0000 | row dropped |
| orphan_known_for_title | name_basics | knownForTitles id not in title_basics | 725 | 0.0046 | 0 | 0.0000 | id removed from the array |

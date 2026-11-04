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

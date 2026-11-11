# Data quality samples (silver)

## implausible_lifespan

| nconst | primaryName | birthYear | deathYear | primaryProfession | knownForTitles | quality_flags |
| --- | --- | --- | --- | --- | --- | --- |
| nm0243164 | Tatzumbia Dupea | 1849 | 1970 | ['actress'] | None | ['implausible_lifespan'] |
| nm0130625 | Jeanne Louise Calment | 1875 | 1997 | ['soundtrack' 'archive_footage'] | ['tt2433670' 'tt0128058' 'tt0081721' 'tt10875582'] | ['implausible_lifespan'] |

## alive_older_than_max_lifespan

| nconst | primaryName | birthYear | deathYear | primaryProfession | knownForTitles | quality_flags |
| --- | --- | --- | --- | --- | --- | --- |
| nm0005637 | Victor Arménise | 1896 | nan | ['cinematographer' 'camera_department'] | ['tt0143856' 'tt0029760' 'tt0035075' 'tt0204181'] | ['missing_death_year'] |
| nm0011637 | Srirama Murthy Addanki | 1898 | nan | ['actor'] | ['tt0256069' 'tt0154228' 'tt0280897' 'tt0256367'] | ['missing_death_year'] |
| nm0037656 | Sofia Ortega | 1877 | nan | ['actress'] | ['tt0019344' 'tt0022689'] | ['missing_death_year'] |

## runtime_outlier

| tconst | titleType | primaryTitle | originalTitle | isAdult | startYear | endYear | runtimeMinutes | genres | quality_flags |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| tt0387271 | movie | A Twist of Fate | Hanmanheun namilsaeng | 0 | 1970 | nan | 1970 | ['Drama'] | ['runtime_outlier'] |
| tt11865554 | tvEpisode | The Newport Revenge Murder | The Newport Revenge Murder | 0 | 2020 | nan | 2020 | ['Crime' 'Documentary'] | ['runtime_outlier'] |
| tt15689760 | tvEpisode | P3aksjonen 2021 | P3aksjonen 2021 | 0 | 2021 | nan | 6000 | ['Talk-Show'] | ['runtime_outlier'] |

## episode_before_series_start

| tconst | parentTconst | seasonNumber | episodeNumber | quality_flags | parentType | parentStartYear | startYear |
| --- | --- | --- | --- | --- | --- | --- | --- |
| tt0445529 | tt0072472 | 1 | nan | ['episode_not_positive' 'episode_before_series_start'] | tvSeries | 1975 | 1974 |
| tt0802360 | tt0390751 | 1 | 2.0 | ['episode_before_series_start'] | tvSeries | 2002 | 2001 |
| tt0864068 | tt0094548 | 1 | nan | ['episode_not_positive' 'episode_before_series_start'] | tvSeries | 1988 | 1984 |

## principal_born_after_release

| nconst | tconst | category | primaryTitle | startYear | primaryName | birthYear | deathYear |
| --- | --- | --- | --- | --- | --- | --- | --- |
| nm0002301 | tt0019935 | composer | The Godless Girl | 1928 | Carl Davis | 1936 | 2023 |
| nm0002301 | tt0016884 | composer | Flesh and the Devil | 1926 | Carl Davis | 1936 | 2023 |
| nm0002301 | tt0014341 | composer | Our Hospitality | 1923 | Carl Davis | 1936 | 2023 |

## principal_long_after_death

| nconst | tconst | category | primaryTitle | startYear | primaryName | birthYear | deathYear |
| --- | --- | --- | --- | --- | --- | --- | --- |
| nm0000004 | tt1093389 | actor | Saturday Night Live: The Best of Dan Aykroyd | 2005 | John Belushi | 1949 | 1982 |
| nm0000004 | tt0435730 | actor | Saturday Night Live: Presidential Bash 2004 | 2004 | John Belushi | 1949 | 1982 |
| nm0000004 | tt1613125 | self | Saturday Night Live Presents: Sports All-Stars | 2010 | John Belushi | 1949 | 1982 |

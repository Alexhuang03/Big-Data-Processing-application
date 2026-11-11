# Data quality samples (typed)

## death_before_birth

| nconst | primaryName | birthYear | deathYear | primaryProfession | knownForTitles |
| --- | --- | --- | --- | --- | --- |
| nm14419164 | Balbir Singh Dosanjh | 1924 | 1923 | None | ['tt0397174' 'tt25397060'] |
| nm0392955 | Homer | 850 | 800 | ['writer' 'miscellaneous'] | ['tt33764258' 'tt0332452' 'tt0190590' 'tt35148005'] |
| nm0262381 | Euripides | 484 | 406 | ['writer' 'composer'] | ['tt0054650' 'tt29474378' 'tt0257740' 'tt0258203'] |

## implausible_lifespan

| nconst | primaryName | birthYear | deathYear | primaryProfession | knownForTitles |
| --- | --- | --- | --- | --- | --- |
| nm0243164 | Tatzumbia Dupea | 1849 | 1970 | ['actress'] | None |
| nm0130625 | Jeanne Louise Calment | 1875 | 1997 | ['soundtrack' 'archive_footage'] | ['tt2433670' 'tt0128058' 'tt0081721' 'tt10875582'] |

## alive_older_than_max_lifespan

| nconst | primaryName | birthYear | deathYear | primaryProfession | knownForTitles |
| --- | --- | --- | --- | --- | --- |
| nm0338458 | George Greenbaum | 1889 | nan | ['cinematographer' 'director'] | ['tt0006315' 'tt0361528' 'tt0361629' 'tt0440667'] |
| nm0617008 | Rowland Myles | 1884 | nan | ['actor'] | ['tt0176512' 'tt0150579' 'tt0012489' 'tt0299899'] |
| nm2746366 | Katy Fibingerová | 1887 | nan | ['actress'] | ['tt0250651' 'tt0167308'] |

## runtime_not_positive

| tconst | titleType | primaryTitle | originalTitle | isAdult | startYear | endYear | runtimeMinutes | genres |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| tt0926344 | short | V-r | V-r | 0 | 2006 | nan | 0 | ['Short'] |
| tt0830188 | short | The Last Flight | The Last Flight | 0 | 2005 | nan | 0 | ['Comedy' 'Short'] |
| tt0348126 | short | Storm P. tegner de Tree Små Mænd | Storm P. tegner de Tree Små Mænd | 0 | 1920 | nan | 0 | ['Animation' 'Short'] |

## runtime_outlier

| tconst | titleType | primaryTitle | originalTitle | isAdult | startYear | endYear | runtimeMinutes | genres |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| tt5909192 | tvSpecial | FNS 27 HRS Television 30 | FNS 27 HRS Television 30 | 0 | 2016.0 | nan | 1620 | ['Talk-Show'] |
| tt7984326 | tvSeries | Highlights of 20th Century Music: 1899-1972 | Highlights of 20th Century Music: 1899-1972 | 0 | 2015.0 | 2016.0 | 2800 | ['Music'] |
| tt38947320 | tvSeries | Love in Araya | Love in Araya | 0 | nan | nan | 2025 | ['Romance'] |

## season_or_episode_not_positive

| tconst | parentTconst | seasonNumber | episodeNumber |
| --- | --- | --- | --- |
| tt12589172 | tt12547904 | 1 | 0 |
| tt0726300 | tt0103568 | 1 | 0 |
| tt23782130 | tt10284766 | 18 | 0 |

## episode_before_series_start

| tconst | parentTconst | seasonNumber | episodeNumber | parentType | parentStartYear | startYear |
| --- | --- | --- | --- | --- | --- | --- |
| tt0110571 | tt1781127 | nan | nan | tvSeries | 1997 | 1995 |
| tt0802360 | tt0390751 | 1.0 | 2.0 | tvSeries | 2002 | 2001 |
| tt0948276 | tt0478486 | 1.0 | 1.0 | tvSeries | 2001 | 2000 |

## principal_born_after_release

| nconst | tconst | category | primaryTitle | startYear | primaryName | birthYear | deathYear |
| --- | --- | --- | --- | --- | --- | --- | --- |
| nm0125204 | tt0019060 | composer | Kruzheva | 1928 | Yuri Butsko | 1938 | 2015.0 |
| nm0251963 | tt15680392 | self | KRCR News Channel 7 | 1963 | Rich Eisen | 1969 | nan |
| nm0281792 | tt3328830 | actor | Kasperltheater | 1957 | Stefan Fleming | 1959 | nan |

## principal_long_after_death

| nconst | tconst | category | primaryTitle | startYear | primaryName | birthYear | deathYear |
| --- | --- | --- | --- | --- | --- | --- | --- |
| nm0000861 | tt28077625 | actor | Vol.1 (RESTORED EDITION) | 2023 | Freddie Bartholomew | 1924 | 1992 |
| nm0001365 | tt33400818 | self | Whitney Houston - The Concert for a New South Africa (Durban) | 2024 | Whitney Houston | 1963 | 2012 |
| nm0001365 | tt28282285 | self | ET Canada Presents: One on One with Kylie Minogue | 2023 | Whitney Houston | 1963 | 2012 |

## orphan_crew_title

| tconst | directors | writers |
| --- | --- | --- |
| tt50234124 | ['nm2110181'] | ['nm2110181'] |
| tt50251348 | None | None |
| tt50251429 | ['nm1977027'] | None |

## orphan_crew_director

| __ref | tconst |
| --- | --- |
| nm17752595 | tt38608042 |
| nm17754813 | tt38623844 |
| nm17756713 | tt39651481 |

## orphan_crew_writer

| __ref | tconst |
| --- | --- |
| nm17754813 | tt38623844 |
| nm17758193 | tt38628280 |
| nm17759913 | tt38645546 |

## orphan_episode_parent

| tconst | parentTconst | seasonNumber | episodeNumber |
| --- | --- | --- | --- |
| tt28611150 | tt28606619 | nan | nan |
| tt34954661 | tt28606619 | nan | nan |
| tt28638568 | tt28606619 | nan | nan |

## orphan_principal_name

| tconst | ordering | nconst | category | job | characters |
| --- | --- | --- | --- | --- | --- |
| tt38607265 | 4 | nm17752582 | self | None | ['Self - Directeur PBL'] |
| tt1929495 | 9 | nm17754858 | self | None | ['Self - Goliad visitor'] |
| tt16380332 | 8 | nm17754920 | actress | None | ['His wife'] |

## orphan_known_for_title

| __ref | nconst |
| --- | --- |
| tt50255091 | nm19061687 |
| tt50255091 | nm15199656 |
| tt50255091 | nm18369999 |

from pathlib import Path

from app.domain.matching import (
    parse_movie_from_path,
    parse_tv_from_path,
    radarr_movie_match_score,
    sonarr_series_match_score,
)


def test_parse_tv_season_episode():
    p = Path("/data/TV/Show.Name/Season 01/Show.Name.S01E02.1080p.mkv")
    r = parse_tv_from_path(p)
    assert r.season == 1
    assert r.episode == 2


def test_parse_movie_year_in_folder():
    p = Path("/movies/Alien 1979/Alien (1979).mkv")
    r = parse_movie_from_path(p)
    assert r.year == 1979


def test_sonarr_series_match_score_avoids_short_false_positive():
    series, score = sonarr_series_match_score(
        "Mister Rogers Neighborhood",
        [
            {"id": 1, "title": "ER", "alternateTitles": []},
            {"id": 2, "title": "Mister Rogers' Neighborhood", "alternateTitles": []},
        ],
    )
    assert series["id"] == 2
    assert score >= 90


def test_sonarr_series_match_score_avoids_single_letter_substring_false_positive():
    series, score = sonarr_series_match_score(
        "Wolfblood {tvdb-262554}",
        [
            {"id": 1, "title": "V", "alternateTitles": []},
            {"id": 2, "title": "Wolfblood", "alternateTitles": []},
        ],
    )
    assert series["id"] == 2
    assert score >= 60


def test_sonarr_series_match_score_strips_tvdb_tag_for_short_titles():
    series, score = sonarr_series_match_score(
        "Cops {tvdb-74709}",
        [
            {"id": 1, "title": "Cops", "alternateTitles": []},
        ],
    )
    assert series["id"] == 1
    assert score >= 90


def test_radarr_movie_match_score_avoids_single_letter_substring_false_positive():
    movie, score = radarr_movie_match_score(
        "The Matrix",
        1999,
        [
            {"id": 1, "title": "M", "year": 1931},
            {"id": 2, "title": "The Matrix", "year": 1999},
        ],
    )
    assert movie["id"] == 2
    assert score >= 75

from pathlib import Path

from app.domain.enums import MediaKind
from app.domain.scoring import score_finding
from app.domain.value_objects import ProbeResult


def test_extras_keywords_do_not_create_findings():
    probe = ProbeResult(True, 900.0, 1920, 1080, "h264", ["aac"], {}, None)
    result = score_finding(
        file_path="/media/movies/Film.Sample.2024.mkv",
        media_kind=MediaKind.MOVIE,
        size_bytes=800_000_000,
        probe=probe,
        min_tv_size_bytes=50_000,
        min_movie_size_bytes=100_000,
        min_duration_tv=60,
        min_duration_movie=300,
        extras_keywords=("sample", "trailer"),
        excluded_keywords=("sample",),
        has_manager_match=True,
    )
    assert result.score == 0
    assert result.reasons == []


def test_ignore_pattern_becomes_ignore_action():
    probe = ProbeResult(True, 1200.0, 1920, 1080, "h264", ["aac"], {}, None)
    result = score_finding(
        file_path="/media/tv/Show/Season 01/Show.S01E01.mkv",
        media_kind=MediaKind.TV,
        size_bytes=1_000_000,
        probe=probe,
        min_tv_size_bytes=50_000,
        min_movie_size_bytes=100_000,
        min_duration_tv=60,
        min_duration_movie=300,
        ignored_pattern_lines="**/Show.S01E01.mkv",
    )
    assert result.proposed_action.value == "ignore"

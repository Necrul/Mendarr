from app.domain.enums import MediaKind
from app.domain.scoring import score_finding
from app.domain.value_objects import ProbeResult


def test_zero_byte_scores_high():
    pr = ProbeResult(True, 120.0, 1920, 1080, "h264", ["aac"], {}, None)
    r = score_finding(
        file_path="/media/x.mkv",
        media_kind=MediaKind.MOVIE,
        size_bytes=0,
        probe=pr,
        min_tv_size_bytes=50_000,
        min_movie_size_bytes=100_000,
        min_duration_tv=60,
        min_duration_movie=300,
        has_manager_match=True,
    )
    assert r.score >= 90
    assert any(x.code == "FS_ZERO_BYTE" for x in r.reasons)


def test_probe_failure():
    pr = ProbeResult(False, None, None, None, None, [], None, "failed")
    r = score_finding(
        file_path="/media/x.mkv",
        media_kind=MediaKind.TV,
        size_bytes=500_000,
        probe=pr,
        min_tv_size_bytes=50_000,
        min_movie_size_bytes=100_000,
        min_duration_tv=60,
        min_duration_movie=300,
    )
    assert any(x.code == "MD_PROBE_FAILED" for x in r.reasons)

from pathlib import Path

from app.services.scan_service import (
    _compact_ffprobe_json,
    _compact_radarr_movie_rows,
    _compact_sonarr_series_rows,
    _sibling_video_files,
)


def test_compact_ffprobe_json_keeps_only_scoring_fields():
    compact = _compact_ffprobe_json(
        {
            "streams": [
                {
                    "codec_type": "video",
                    "codec_name": "h264",
                    "width": 1920,
                    "height": 1080,
                    "tags": {"language": "eng"},
                },
                {
                    "codec_type": "audio",
                    "codec_name": "aac",
                    "channels": 2,
                },
            ],
            "format": {
                "duration": "30.0",
                "bit_rate": "64000",
                "filename": "/media/file.mkv",
            },
            "chapters": [{"id": 1}],
        }
    )

    assert compact == {
        "streams": [
            {"codec_type": "video", "codec_name": "h264", "width": 1920, "height": 1080},
            {"codec_type": "audio", "codec_name": "aac"},
        ],
        "format": {"duration": "30.0", "bit_rate": "64000"},
    }


def test_compact_sonarr_series_rows_drops_unused_fields():
    compact = _compact_sonarr_series_rows(
        [
            {
                "id": 77,
                "title": "Broken Show",
                "alternateTitles": [{"title": "Broken Show Alt", "seasonCount": 4}],
                "images": [{"coverType": "poster"}],
            }
        ]
    )

    assert compact == [
        {
            "id": 77,
            "title": "Broken Show",
            "alternateTitles": [{"title": "Broken Show Alt"}],
        }
    ]


def test_compact_radarr_movie_rows_drops_unused_fields():
    compact = _compact_radarr_movie_rows(
        [
            {
                "id": 81,
                "title": "Broken Movie",
                "year": 2024,
                "images": [{"coverType": "poster"}],
                "genres": ["Drama"],
            }
        ]
    )

    assert compact == [{"id": 81, "title": "Broken Movie", "year": 2024}]


def test_sibling_video_files_filters_non_video_entries(tmp_path: Path):
    folder = tmp_path / "Show" / "Season 01"
    folder.mkdir(parents=True)
    video = folder / "Episode 01.mkv"
    extra = folder / "Episode 01.nfo"
    nested = folder / "Subs"
    nested.mkdir()
    video.write_bytes(b"x")
    extra.write_text("meta")

    siblings = _sibling_video_files(video)

    assert siblings == [video]

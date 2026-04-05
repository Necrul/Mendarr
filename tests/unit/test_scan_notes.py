from app.domain.scan_notes import parse_scan_notes, scan_note_pairs, scan_progress_percent, serialize_scan_notes


def test_parse_scan_notes_supports_legacy_format():
    payload = parse_scan_notes("libraries=2; files_seen=14; findings=3; current_file=/media/Show/E01.mkv")

    assert payload["libraries"] == "2"
    assert payload["files_seen"] == "14"
    assert payload["current_file"] == "/media/Show/E01.mkv"


def test_scan_note_pairs_supports_json_progress():
    raw = serialize_scan_notes(
        {
            "files_seen": 12,
            "total_files": 40,
            "findings": 2,
            "current_library": "/mnt/RAYNAS/TV Shows",
            "current_file": "/mnt/RAYNAS/TV Shows/Season 01/Episode 01.mkv",
        }
    )

    pairs = scan_note_pairs(raw, status="running")

    assert ("Progress", "30% (12/40)") in pairs
    assert ("Current library", "/mnt/RAYNAS/TV Shows") in pairs
    assert ("Current file", "/mnt/RAYNAS/TV Shows/Season 01/Episode 01.mkv") in pairs
    assert scan_progress_percent(parse_scan_notes(raw), status="running") == 30


def test_scan_progress_percent_shows_minimum_one_once_files_are_moving():
    raw = serialize_scan_notes({"files_seen": 240, "total_files": 108482})

    assert scan_progress_percent(parse_scan_notes(raw), status="running") == 1

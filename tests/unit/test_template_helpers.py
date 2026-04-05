from types import SimpleNamespace

from app.web.templates import _finding_secondary_name


def test_finding_secondary_name_suppresses_short_alias_inside_filename():
    finding = SimpleNamespace(
        title="ER",
        file_name="Mister Rogers' Neighborhood - S04E36 - Show 1166.mkv",
    )

    assert _finding_secondary_name(finding) == ""

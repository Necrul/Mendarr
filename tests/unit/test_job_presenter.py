from types import SimpleNamespace

from app.web.job_presenter import remediation_result_label, remediation_result_message


def test_job_presenter_marks_search_success_as_accepted_not_completed():
    job = SimpleNamespace(
        status="succeeded",
        last_error=None,
        finding=SimpleNamespace(manager_kind="sonarr"),
        attempts=[SimpleNamespace(id=1, step_name="EpisodeSearch")],
    )

    assert remediation_result_label(job) == "Search accepted"
    assert "accepted the search request" in remediation_result_message(job)


def test_job_presenter_marks_refresh_success_as_rescan_accepted():
    job = SimpleNamespace(
        status="succeeded",
        last_error=None,
        finding=SimpleNamespace(manager_kind="radarr"),
        attempts=[SimpleNamespace(id=1, step_name="RefreshMovie")],
    )

    assert remediation_result_label(job) == "Rescan accepted"
    assert "accepted the rescan request" in remediation_result_message(job)


def test_job_presenter_marks_cutoff_met_failure_explicitly():
    job = SimpleNamespace(
        status="failed",
        last_error="Sonarr reports cutoff already met for this episode; replacement search will not force an upgrade",
        finding=SimpleNamespace(manager_kind="sonarr"),
        attempts=[SimpleNamespace(id=1, step_name="error")],
    )

    assert remediation_result_label(job) == "Cutoff already met"
    assert "would not force an upgrade" in remediation_result_message(job)


def test_job_presenter_marks_delete_search_success_explicitly():
    job = SimpleNamespace(
        status="succeeded",
        action_type="delete_search_replacement",
        last_error=None,
        finding=SimpleNamespace(manager_kind="sonarr"),
        attempts=[SimpleNamespace(id=1, step_name="DeleteEpisodeFile")],
    )

    assert remediation_result_label(job) == "Delete and search accepted"
    assert "delete-and-search request" in remediation_result_message(job)

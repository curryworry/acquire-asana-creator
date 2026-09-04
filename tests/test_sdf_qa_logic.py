from datetime import date

from api.qa_service import (
    _current_io_budget_window,
    _line_item_has_inclusion,
    _parse_io_budget_windows,
)


def test_parse_io_budget_windows_handles_sdf_budget_segments():
    windows = _parse_io_budget_windows("(100; 09/01/2026; 09/30/2026; 123; September)")

    assert windows == [(date(2026, 9, 1), date(2026, 9, 30))]


def test_current_io_budget_window_requires_active_status_and_current_segment():
    io_row = {
        "Status": "Active",
        "Budget Segments": "(100; 08/01/2026; 08/31/2026; 1; Old);(200; 09/01/2026; 09/30/2026; 2; Current)",
    }

    assert _current_io_budget_window(io_row, date(2026, 9, 4)) == (date(2026, 9, 1), date(2026, 9, 30))
    assert _current_io_budget_window(io_row, date(2026, 10, 1)) is None
    assert _current_io_budget_window({**io_row, "Status": "Paused"}, date(2026, 9, 4)) is None


def test_line_item_inclusion_uses_sdf_or_qa_fields():
    assert _line_item_has_inclusion({"Site Targeting - Include": "example.com"}, None)
    assert _line_item_has_inclusion({}, {"App Targeting - Include Qa": "App: Trade Me"})
    assert not _line_item_has_inclusion(
        {"Channel Targeting - Include": "", "Site Targeting - Include": "", "App Targeting - Include": ""},
        {"Channel Targeting - Include Qa": "", "Site Targeting - Include Qa": "", "App Targeting - Include Qa": ""},
    )

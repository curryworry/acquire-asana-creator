from datetime import date

from bid_manager_client import SPEND_GROUP_BYS
from api.qa_service import (
    _current_io_budget_window,
    _dv360_advertiser_url,
    _dv360_campaign_url,
    _dv360_insertion_order_url,
    _dv360_line_item_url,
    _inventory_source_include_count,
    _line_item_has_broad_inventory_source_include,
    _line_item_has_inclusion,
    _parse_io_budget_windows,
    parse_video_trademe_attachment,
)
from gmail_client import GmailAttachment


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


def test_inventory_source_include_count_uses_sdf_semicolon_list():
    assert _inventory_source_include_count({"Inventory Source Targeting - Include": ""}) == 0
    assert _inventory_source_include_count({"Inventory Source Targeting - Include": "29377601;"}) == 1
    assert _inventory_source_include_count({"Inventory Source Targeting - Include": "1; 2; 6; 8; 9;"}) == 5


def test_broad_inventory_source_include_requires_more_than_five_sources():
    assert not _line_item_has_broad_inventory_source_include(
        {"Inventory Source Targeting - Include": "1; 2; 6; 8; 9;"}
    )
    assert _line_item_has_broad_inventory_source_include(
        {"Inventory Source Targeting - Include": "1; 2; 6; 8; 9; 10;"}
    )


def test_parse_video_trademe_attachment_reads_expanded_dv360_columns():
    csv = (
        "Advertiser,Advertiser ID,Campaign,Campaign ID,Insertion Order,Insertion Order ID,Line Item,Line Item ID,Impressions\n"
        "Example Advertiser,725811497,Example Campaign,56929417,Example IO,1028293889,Example LI,23882683852,\"1,234\"\n"
        "Report Time:,2026/09/08 19:03 GMT,,,,,,,\n"
    )
    attachment = GmailAttachment(
        filename="trademe.csv",
        content=csv.encode("utf-8"),
        message_id="message-1",
        received_at="2026-09-08T19:03:23+00:00",
        subject="TradeMe On Video - Last 7 Days",
    )

    report = parse_video_trademe_attachment(attachment)

    assert report["meta"]["total_impressions"] == "1234"
    assert report["meta"]["report_time"] == "2026/09/08 19:03 GMT"
    assert report["rows"] == [
        {
            "ROW_ID": "message-1:1",
            "ADVERTISER": "Example Advertiser",
            "ADVERTISER_ID": "725811497",
            "CAMPAIGN": "Example Campaign",
            "CAMPAIGN_ID": "56929417",
            "INSERTION_ORDER": "Example IO",
            "INSERTION_ORDER_ID": "1028293889",
            "LINE_ITEM": "Example LI",
            "LINE_ITEM_ID": "23882683852",
            "IMPRESSIONS": 1234,
        }
    ]


def test_dv360_video_trademe_urls_match_expected_formats():
    assert _dv360_advertiser_url("360441", "725811497") == (
        "https://displayvideo.google.com/ng_nav/p/360441/a/725811497/cs"
    )
    assert _dv360_campaign_url("360441", "725811497", "56929417") == (
        "https://displayvideo.google.com/ng_nav/p/360441/a/725811497/c/56929417/explorer"
    )
    assert _dv360_insertion_order_url("360441", "725811497", "56929417", "1028293889") == (
        "https://displayvideo.google.com/ng_nav/p/360441/a/725811497/c/56929417/io/1028293889/explorerlis"
    )
    assert _dv360_line_item_url("360441", "725811497", "56929417", "1028293889", "23882683852") == (
        "https://displayvideo.google.com/ng_nav/p/360441/a/725811497/c/56929417/io/1028293889/li/23882683852/details"
    )


def test_missing_inclusion_spend_report_includes_campaign_dimensions():
    assert "FILTER_MEDIA_PLAN" in SPEND_GROUP_BYS
    assert "FILTER_MEDIA_PLAN_NAME" in SPEND_GROUP_BYS

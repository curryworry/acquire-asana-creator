import unittest
from datetime import date
from unittest.mock import patch

from api.dashboard_service import going_live_soon_dashboard


class _QueryResult:
    def __init__(self, rows):
        self._rows = rows

    def result(self):
        return self._rows


class _Client:
    def __init__(self, rows):
        self.rows = rows
        self.query_text = ""

    def query(self, query_text):
        self.query_text = query_text
        return _QueryResult(self.rows)


class GoingLiveSoonDashboardTests(unittest.TestCase):
    def test_returns_enhanced_booking_period_rows_and_window_metadata(self) -> None:
        client = _Client(
            [
                {
                    "row_key": "12345|9001|2026-10-28|2026-11-30",
                    "our_ref": "12345",
                    "job_number": "9001",
                    "start_date": date(2026, 10, 28),
                    "end_date": date(2026, 11, 30),
                    "days_until_live": 1,
                    "advertiser": "Example advertiser",
                    "campaign": "Example campaign",
                    "account_manager": "Example manager",
                    "location_text": "Example line item",
                    "property_name": "Programmatic - Display",
                    "booking_status": "Booked",
                    "specification_text": "300x250",
                    "special_requirements": "Target finance decision makers",
                    "trafficking_instructions": "Use supplied click tracker",
                    "our_cost": 1250.0,
                    "trafficking_matched": True,
                    "buy_source_conflict": False,
                }
            ]
        )

        with patch("api.dashboard_service.bq_context", return_value=(client, "project", "dataset")), patch(
            "api.dashboard_service.today_nz", return_value=date(2026, 10, 27)
        ):
            payload = going_live_soon_dashboard()

        self.assertEqual(payload["rows"][0]["OUR_REF"], "12345")
        self.assertEqual(payload["rows"][0]["SPECIAL_REQUIREMENTS"], "Target finance decision makers")
        self.assertEqual(payload["rows"][0]["OUR_COST"], 1250.0)
        self.assertEqual(payload["rows"][0]["DAYS_UNTIL_LIVE"], 1)
        self.assertEqual(payload["meta"]["source_table"], "BUY_DATA_ENHANCED")
        self.assertEqual(payload["meta"]["window_start"], "2026-10-27")
        self.assertEqual(payload["meta"]["window_end"], "2026-11-03")
        self.assertIn("BUY_DATA_ENHANCED", client.query_text)
        self.assertIn("SPECIAL_REQUIREMENTS", client.query_text)
        self.assertIn("BOOKING_STATUS", client.query_text)
        self.assertIn("INTERVAL 7 DAY", client.query_text)


if __name__ == "__main__":
    unittest.main()

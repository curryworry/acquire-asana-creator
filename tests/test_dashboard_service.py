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
    def test_returns_one_our_ref_grain_buy_data_rows_and_window_metadata(self) -> None:
        client = _Client(
            [
                {
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
                }
            ]
        )

        with patch("api.dashboard_service.bq_context", return_value=(client, "project", "dataset")), patch(
            "api.dashboard_service.today_nz", return_value=date(2026, 10, 27)
        ):
            payload = going_live_soon_dashboard()

        self.assertEqual(payload["rows"][0]["OUR_REF"], "12345")
        self.assertEqual(payload["rows"][0]["DAYS_UNTIL_LIVE"], 1)
        self.assertEqual(payload["meta"]["source_table"], "B27F8_BUY_DATA")
        self.assertEqual(payload["meta"]["window_start"], "2026-10-27")
        self.assertEqual(payload["meta"]["window_end"], "2026-11-03")
        self.assertIn("GROUP BY 1", client.query_text)
        self.assertIn("BOOKINGSTATUS", client.query_text)
        self.assertIn("INTERVAL 7 DAY", client.query_text)


if __name__ == "__main__":
    unittest.main()

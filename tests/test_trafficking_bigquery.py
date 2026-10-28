import unittest
from datetime import datetime, timezone
from unittest.mock import patch

import pandas as pd

from gmail_client import GmailAttachment
from trafficking_bigquery import (
    buy_data_enhanced_sql,
    master_enhanced_sql,
    prepare_trafficking_records,
    trafficking_merge_sql,
)
from scripts.sync_trafficking_bigquery import latest_report_query


class TraffickingBigQueryTests(unittest.TestCase):
    def attachment(self) -> GmailAttachment:
        return GmailAttachment(
            filename="Trafficking-Report.tsv",
            content=b"",
            message_id="message-123",
            received_at="2026-10-28T17:00:37+00:00",
            subject="FW: Trafficking Report - acquirenz",
        )

    def source_row(self, **overrides):
        row = {
            "OurRef": "33316",
            "JobNumber": "9004",
            "Status": "Ready",
            "CreativeReceived": "Yes",
            "CreativeTrafficked": "No",
            "CampaignComplete": "No",
            "PropertyName": "Programmatic - Social",
            "LocationText": "National | Meta",
            "SpecificationText": "Static image",
            "SpecialRequirements": "Target new students",
            "AdServing": "Platform",
            "StartDate": "28/10/2026",
            "EndDate": "08/12/2026",
            "AdvertiserName": "Example advertiser",
            "CampaignName": "Example campaign",
            "SalesChannel": "Agency",
            "ClientContact": "Example contact",
            "ClientEmail": "contact@example.com",
            "NumUnits": "140,000",
            "RatingMethod": "CPM",
            "OurCost": "$1,250.50",
            "TraffickingInstructions": "Use supplied tracking",
            "AccountManagerName": "Example manager",
            "TraffickerName": "Example trafficker",
        }
        row.update(overrides)
        return row

    def test_prepares_latest_known_record_with_metadata(self) -> None:
        records = prepare_trafficking_records(
            pd.DataFrame([self.source_row()]),
            self.attachment(),
            loaded_at=datetime(2026, 10, 28, 18, 0, tzinfo=timezone.utc),
        )

        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertEqual(record["OUR_REF"], "33316")
        self.assertEqual(record["START_DATE"], "2026-10-28")
        self.assertEqual(record["END_DATE"], "2026-12-08")
        self.assertEqual(record["OUR_COST"], 1250.5)
        self.assertEqual(record["NUM_UNITS"], 140000.0)
        self.assertEqual(record["SOURCE_MESSAGE_ID"], "message-123")
        self.assertEqual(record["LOAD_RUN_AT"], "2026-10-28T18:00:00+00:00")
        self.assertEqual(len(record["ROW_HASH"]), 64)

    def test_rejects_duplicate_our_refs_in_one_report(self) -> None:
        df = pd.DataFrame([self.source_row(), self.source_row(Status="Updated")])

        with self.assertRaisesRegex(ValueError, "duplicate OurRef values: 33316"):
            prepare_trafficking_records(df, self.attachment())

    def test_merge_updates_seen_rows_and_never_invalidates_absent_rows(self) -> None:
        sql = trafficking_merge_sql("`p.d.target`", "`p.d.stage`")

        self.assertIn("ON T.OUR_REF = S.OUR_REF", sql)
        self.assertIn("WHEN MATCHED THEN", sql)
        self.assertIn("WHEN NOT MATCHED THEN", sql)
        self.assertIn("T.LAST_SEEN_AT = S.LOAD_RUN_AT", sql)
        self.assertNotIn("NOT MATCHED BY SOURCE", sql)
        self.assertNotIn("DELETE", sql)
        self.assertNotIn("IS_CURRENT", sql)

    def test_enhanced_view_sql_has_expected_grains_and_joins(self) -> None:
        buy_sql = buy_data_enhanced_sql("project", "dataset")
        master_sql = master_enhanced_sql("project", "dataset")

        self.assertIn("BUY_ROW_KEY", buy_sql)
        self.assertIn("STARTDATE, ENDDATE", buy_sql)
        self.assertIn("WHEN 'booked' THEN 0", buy_sql)
        self.assertIn("t.SPECIAL_REQUIREMENTS", buy_sql)
        self.assertIn("TRAFFICKING_MATCHED", buy_sql)
        self.assertIn("GROUP BY 1, 2, 3", master_sql)
        self.assertIn("LEFT JOIN delivery_rollup", master_sql)

    def test_manual_sync_query_includes_already_processed_reports(self) -> None:
        with patch.dict("os.environ", {}, clear=True):
            query = latest_report_query("Trafficking Report - acquirenz")

        self.assertEqual(
            query,
            'in:anywhere subject:"Trafficking Report - acquirenz" has:attachment',
        )
        self.assertNotIn("-label:processed", query)


if __name__ == "__main__":
    unittest.main()

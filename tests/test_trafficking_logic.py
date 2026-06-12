import unittest

import pandas as pd

from trafficking_logic import (
    build_candidate_rows,
    build_subtask_blueprints,
    build_subtask_rows,
    existing_subtask_matches,
    extract_five_digit_number,
    find_existing_parent_task,
    parent_due_from_blueprints,
)


class TraffickingLogicTests(unittest.TestCase):
    def test_build_candidate_rows_normalizes_and_dedupes(self) -> None:
        df = pd.DataFrame(
            [
                {"CampaignName": " Launch Campaign, ", "JobNumber": "123.0"},
                {"CampaignName": "Launch   Campaign", "JobNumber": "123"},
                {"CampaignName": "", "JobNumber": "999"},
                {"CampaignName": "Other", "JobNumber": ""},
            ]
        )

        candidates, unmatched = build_candidate_rows(df)

        self.assertEqual(
            candidates,
            [
                {
                    "campaign_name": "Launch Campaign",
                    "job_number": "123",
                    "task_name": "Launch Campaign (123)",
                }
            ],
        )
        self.assertEqual(
            unmatched,
            [
                {"item": "Trafficking rows", "reason": "1 rows missing CampaignName"},
                {"item": "Trafficking rows", "reason": "1 rows missing JobNumber"},
            ],
        )

    def test_blueprints_add_control_subtasks_and_parent_due_date(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "CampaignName": "Campaign A",
                    "JobNumber": "200",
                    "OurRef": "REF-1",
                    "PropertyName": "Site A",
                    "LocationText": "Homepage",
                    "SpecificationText": "Billboard",
                    "StartDate": "2026-06-15",
                },
                {
                    "CampaignName": "Campaign A",
                    "JobNumber": "200",
                    "OurRef": "REF-2",
                    "PropertyName": "Site B",
                    "LocationText": "Sports",
                    "SpecificationText": "MPU",
                    "StartDate": "2026-06-18",
                },
            ]
        )

        blueprints = build_subtask_blueprints(df)
        rows = blueprints[("Campaign A", "200")]

        self.assertEqual(len(rows), 5)
        self.assertEqual(parent_due_from_blueprints(rows), "2026-06-09")
        self.assertIn("chase creative", [row["subtask_name"] for row in rows])
        self.assertIn("Check live status", [row["subtask_name"] for row in rows])
        self.assertIn("Create and send Dash", [row["subtask_name"] for row in rows])

    def test_build_subtask_rows_preserves_subtask_kinds(self) -> None:
        candidates = [{"campaign_name": "Campaign A", "job_number": "200", "task_name": "Campaign A (200)"}]
        blueprint_map = {
            ("Campaign A", "200"): [
                {
                    "our_ref": "REF-1",
                    "subtask_name": "(REF-1) Site A - Homepage: Billboard",
                    "subtask_due_on": "2026-06-15",
                    "start_date_raw": "2026-06-15",
                    "subtask_kind": "source",
                },
                {
                    "our_ref": "",
                    "subtask_name": "Create and send Dash",
                    "subtask_due_on": "2026-06-18",
                    "start_date_raw": "2026-06-15",
                    "subtask_kind": "control_dash",
                },
            ]
        }

        subtask_rows = build_subtask_rows(candidates, blueprint_map, {"200": "would_create"})

        self.assertEqual([row["subtask_kind"] for row in subtask_rows], ["source", "control_dash"])
        self.assertTrue(all(row["subtask_status"] == "would_create" for row in subtask_rows))

    def test_extract_five_digit_number(self) -> None:
        self.assertEqual(extract_five_digit_number("(REF 12345) Site - Home: Unit"), "12345")
        self.assertEqual(extract_five_digit_number("Create and send Dash"), "")
        self.assertEqual(extract_five_digit_number("ABC123456XYZ"), "")

    def test_find_existing_parent_task_prefers_exact_name(self) -> None:
        tasks = [
            {"gid": "1", "name": "Other Campaign (12345)"},
            {"gid": "2", "name": "Campaign A (123)"},
        ]

        match = find_existing_parent_task(tasks, "Campaign A", "123")

        self.assertEqual(match, {"gid": "2", "name": "Campaign A (123)"})

    def test_existing_source_subtask_match_uses_five_digit_number(self) -> None:
        subtask = {
            "our_ref": "REF-12345-A",
            "subtask_name": "(REF-12345-A) Site A - Homepage: Billboard",
            "subtask_kind": "source",
        }
        existing_subtasks = [
            {"gid": "10", "name": "(Legacy 12345) Old Name - Old Location: Old Unit"},
            {"gid": "11", "name": "Check live status"},
        ]

        self.assertTrue(existing_subtask_matches(subtask, existing_subtasks))

    def test_existing_control_subtask_match_uses_exact_name(self) -> None:
        subtask = {
            "our_ref": "",
            "subtask_name": "Check live status",
            "subtask_kind": "control",
        }
        existing_subtasks = [
            {"gid": "11", "name": "Check live status"},
        ]

        self.assertTrue(existing_subtask_matches(subtask, existing_subtasks))


if __name__ == "__main__":
    unittest.main()

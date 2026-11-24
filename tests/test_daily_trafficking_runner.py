import os
import unittest
from contextlib import ExitStack, redirect_stdout
from io import StringIO
from unittest.mock import MagicMock, patch

import pandas as pd

from scripts import daily_trafficking_dry_run as runner


class SourceEmailProcessingTests(unittest.TestCase):
    def test_only_live_run_marks_email_processed_after_sending_report(self):
        for dry_run in ("true", "false"):
            with self.subTest(dry_run=dry_run), ExitStack() as stack:
                env = {name: "123" for name in (
                    "ASANA_ACCESS_TOKEN", "ASANA_WORKSPACE_GID", "ASANA_PROJECT_GID",
                    "ASANA_DEDUPE_PROJECT_GIDS", "GMAIL_CLIENT_ID", "GMAIL_CLIENT_SECRET",
                    "GMAIL_REFRESH_TOKEN", "REPORT_EMAIL_TO",
                )}
                env.update(DRY_RUN_MODE=dry_run, GMAIL_PROCESSED_LABEL=" ")
                stack.enter_context(patch.dict(os.environ, env, clear=True))
                inbox = stack.enter_context(patch.object(runner, "GmailInboxClient")).return_value
                inbox.fetch_latest_attachment.return_value = runner.GmailAttachment(
                    "report.tsv", b"", "message-1", "2026-11-23", "Trafficking Report - acquirenz"
                )
                inbox.ensure_label.return_value = "label-1"
                stack.enter_context(patch.object(runner, "read_table_from_attachment", return_value=pd.DataFrame()))
                for name, value in (
                    ("REQUIRED_TRAFFICKING_COLUMNS", []), ("REQUIRED_TRAFFICKING_BQ_COLUMNS", []),
                ):
                    stack.enter_context(patch.object(runner, name, value))
                for name in ("build_bigquery_client", "prepare_trafficking_records", "ensure_enhanced_views", "AsanaClient"):
                    stack.enter_context(patch.object(runner, name))
                stack.enter_context(patch.object(runner, "upsert_trafficking_records", return_value=0))
                stack.enter_context(patch.object(runner, "build_candidate_rows", return_value=([], [])))
                stack.enter_context(patch.object(runner, "build_subtask_blueprints", return_value={}))
                stack.enter_context(patch.object(runner, "fetch_dedupe_project_tasks", return_value=[]))
                calls = MagicMock()
                calls.attach_mock(inbox.send_email, "send")
                calls.attach_mock(inbox.ensure_label, "label")
                calls.attach_mock(inbox.mark_read_and_label, "mark")
                with redirect_stdout(StringIO()):
                    self.assertEqual(runner.main(), 0)
                inbox.send_email.assert_called_once()
                if dry_run == "true":
                    inbox.ensure_label.assert_not_called()
                    inbox.mark_read_and_label.assert_not_called()
                else:
                    inbox.ensure_label.assert_called_once_with("processed")
                    inbox.mark_read_and_label.assert_called_once_with(message_id="message-1", label_id="label-1")
                    self.assertEqual([call[0] for call in calls.mock_calls], ["send", "label", "mark"])

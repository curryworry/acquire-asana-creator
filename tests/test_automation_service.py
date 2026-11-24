import unittest
from unittest.mock import patch

from api.automation_service import run_daily_trafficking


class DailyTraffickingModeTests(unittest.TestCase):
    def test_checkbox_overrides_backend_mode_and_defaults_blank_label(self):
        for backend_mode in ("true", "false"):
            for checked in (True, False):
                with self.subTest(backend_mode=backend_mode, checked=checked):
                    secrets = {"DRY_RUN_MODE": backend_mode, "GMAIL_PROCESSED_LABEL": "  "}
                    with patch("api.automation_service.get_secret", side_effect=lambda name, default="": secrets.get(name, default)), patch("api.automation_service.run_script") as run:
                        run_daily_trafficking(checked)
                    script, env = run.call_args.args
                    self.assertEqual(script, "scripts/daily_trafficking_dry_run.py")
                    self.assertEqual(env["DRY_RUN_MODE"], "true" if checked else "false")
                    self.assertEqual(env["GMAIL_PROCESSED_LABEL"], "processed")

import unittest
from datetime import date

from supermetrics_sync import Fingerprint, changed_partitions


class ChangedPartitionsTest(unittest.TestCase):
    def test_returns_missing_and_changed_source_partitions(self) -> None:
        first = date(2026, 11, 21)
        second = date(2026, 11, 22)
        third = date(2026, 11, 23)
        source = {
            first: Fingerprint(10, 100),
            second: Fingerprint(12, 120),
            third: Fingerprint(14, 140),
        }
        destination = {
            first: Fingerprint(10, 100),
            second: Fingerprint(12, 999),
        }

        self.assertEqual(changed_partitions(source, destination), [second, third])

    def test_ignores_destination_only_partitions(self) -> None:
        source_date = date(2026, 11, 22)
        extra_date = date(2026, 11, 23)
        fingerprint = Fingerprint(10, 100)

        self.assertEqual(
            changed_partitions(
                {source_date: fingerprint},
                {source_date: fingerprint, extra_date: fingerprint},
            ),
            [],
        )


if __name__ == "__main__":
    unittest.main()

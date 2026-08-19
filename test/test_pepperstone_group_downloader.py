from __future__ import annotations

import csv
import json
from pathlib import Path

from downloader.download_pepperstone import GROUP_CONFIG, _write_root_summary


def test_root_summary_aggregates_all_existing_group_receipts(tmp_path: Path) -> None:
    for index, group in enumerate(GROUP_CONFIG, start=1):
        group_root = tmp_path / group
        group_root.mkdir(parents=True)
        (group_root / "download_summary.json").write_text(
            json.dumps(
                {
                    "symbol_count": index,
                    "row_count": index * 100,
                    "status_counts": {"updated": index},
                }
            ),
            encoding="utf-8",
        )
        with (group_root / "download_report.csv").open(
            "w", newline="", encoding="utf-8"
        ) as handle:
            writer = csv.DictWriter(
                handle, fieldnames=("last_date", "checked_through_date")
            )
            writer.writeheader()
            writer.writerow(
                {
                    "last_date": "2026-08-17",
                    "checked_through_date": "2026-08-18",
                }
            )

    payload = _write_root_summary(tmp_path, selected_groups=list(GROUP_CONFIG))

    assert payload["state"] == "complete"
    assert payload["completed_group_count"] == 4
    assert payload["symbol_count"] == 10
    assert payload["row_count"] == 1000
    assert payload["status_counts"] == {"updated": 10}
    assert payload["end_date"] == "2026-08-17"
    assert payload["checked_through_date"] == "2026-08-18"
    persisted = json.loads((tmp_path / "download_summary.json").read_text())
    assert persisted["missing_groups"] == []

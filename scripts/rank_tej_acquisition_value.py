"""Inspect all TEJ fields, publish value/gap ranking and apply queue order.

No licensed observations or credentials are published. No source requests or
desktop actions are sent. Source scopes/cursors/states remain unchanged.
"""
from __future__ import annotations

import argparse
from collections import Counter
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json
from downloader.tej_value_priority import apply_snapshot, build_ranking
from scripts.build_tej_smart_wizard_inventory import dump_csv


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT / "data_tej")
    parser.add_argument("--policy", type=Path, default=ROOT / "configs/tej_value_priority.json")
    parser.add_argument("--output", type=Path, required=True, help="New audit directory; never overwrite evidence")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError("Preserve previous ranking evidence; choose a new output directory")
    snapshot, fields, local = build_ranking(ROOT, args.root.resolve(), json.loads(args.policy.read_text()))
    columns = ("rank", "channel", "table_id", "table", "category", "field", "source_field", "field_kind",
               "source_unit", "cadence", "priority", "value_score", "value_basis", "acquisition_stage",
               "local_coverage_state", "local_match_ids", "exported_non_null_cells", "first_query_period",
               "last_query_period", "actual_first", "actual_last", "coverage_is_complete", "exclusion_enabled_by_ranking")
    args.output.mkdir(parents=True)
    dump_csv(args.output / "ranked_fields.csv", [{k: f.get(k) for k in columns} for f in fields])
    dump_csv(args.output / "ranked_tables.csv", snapshot["tables"])
    dump_csv(args.output / "local_feature_evidence.csv", [{k: v for k, v in r.items() if not k.startswith('_')} for r in local])
    atomic_write_json(args.output / "ranking_snapshot.json", snapshot, durable=True)
    result = {"contract": snapshot["contract"], "fingerprint": snapshot["fingerprint"],
              "fields": len(fields), "tables": len(snapshot["tables"]),
              "channels": dict(Counter(f["channel"] for f in fields)),
              "stage_counts": snapshot["field_stage_counts"], "local_features_examined": len(local),
              "provider_requests_sent": 0, "scope_or_states_changed": False,
              "installed": apply_snapshot(args.root, snapshot) if args.apply else []}
    atomic_write_json(args.output / "application.json", result, durable=True)
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()

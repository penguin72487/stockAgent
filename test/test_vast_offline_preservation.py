from copy import deepcopy
import json
from pathlib import Path

import pytest

from scripts.return_remote_legacy_archives import preservation_policy, recovery_ack_contract
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.legacy_artifact_archive import (
    MANUAL_VAST_OFFLINE_CAPTURE_CONTRACT, load_legacy_specs, source_plan,
)


def policy():
    return json.loads((Path(__file__).resolve().parents[1] /
                       "configs/data_sync/vastai_reviewed_offline_return_20261006.json").read_text())


@pytest.mark.parametrize("change", ["empty", "duplicate", "nested", "source", "contract", "scope", "nan"])
def test_offline_policy_cannot_enroll_unknown_roots_or_waive_stability(change):
    value = deepcopy(policy())
    if change == "empty":
        value["relative_roots"] = []
    elif change == "duplicate":
        value["relative_roots"] *= 2
    elif change == "nested":
        value["relative_roots"] = ["operations/history/another-root"]
    elif change == "source":
        value["relative_roots"] = ["data_tw_minute/current"]
    elif change == "contract":
        value["capture_contract"] = "unknown"
    elif change == "scope":
        value["scopes"].append("live")
    else:
        value["minimum_stable_hours"] = float("nan")
    with pytest.raises(SnapshotError):
        preservation_policy(value)


def test_reviewed_offline_policy_preserves_bounded_ack_and_exact_scope():
    value = preservation_policy(policy())
    assert recovery_ack_contract(value) == ("d_verified_remote_legacy_return_v3", 1500)
    assert value["archive_only"] and value["capture_contract"] == MANUAL_VAST_OFFLINE_CAPTURE_CONTRACT
    assert set(value["scopes"]) == {"smoke", "operations"}


def test_offline_catalog_requires_its_contract_and_manual_capture(tmp_path):
    row = {"dataset": "legacy-offline-test", "relative_root": "smoke/retained",
           "minimum_stable_days": 7, "manual_capture_min_stable_hours": 12,
           "archive_only": True, "compression": "gzip-1-adaptive-over-8m",
           "stage_root": str(tmp_path / "stage")}
    catalog = {"schema_version": 1, "authority_node_id": "penguin", "archives": [row]}
    path = tmp_path / "catalog.json"
    path.write_text(json.dumps(catalog))
    with pytest.raises(SnapshotError):
        load_legacy_specs(path)
    row["capture_contract"] = MANUAL_VAST_OFFLINE_CAPTURE_CONTRACT
    path.write_text(json.dumps(catalog))
    spec = load_legacy_specs(path)[row["dataset"]]
    with pytest.raises(SnapshotError, match="explicit manual"):
        source_plan(tmp_path, spec)
    row["relative_root"] = "live/current-account"
    path.write_text(json.dumps(catalog))
    with pytest.raises(SnapshotError, match="explicitly reviewed"):
        load_legacy_specs(path)

from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import pytest

from stockagent.live.data_monitor_inventory import (
    DATASET_MEMBERSHIP_VERSION,
    INVENTORY_VERSION,
    _dataset_memberships,
    _membership_root,
    inventory_dataset_delta,
)


def _selection() -> dict[str, list[Path]]:
    return {
        "dataset:a": [Path("/tmp/a.parquet")],
        "dataset:b": [Path("/tmp/b.parquet")],
        "dataset:empty": [],
    }


def _cache(selected: dict[str, list[Path]]) -> dict:
    members = _dataset_memberships(selected)
    return {
        "version": INVENTORY_VERSION,
        "dataset_membership_version": DATASET_MEMBERSHIP_VERSION,
        "dataset_memberships": members,
        "selection_membership": _membership_root(members),
        "schemas": {"schema": [["x", "int64"]]},
        "files": {
            str(path): {
                "file_identity": [1, 2, 3, 4, 5],
                "stats": {"schema_id": "schema", "count": 1, "non_null": [1]},
            }
            for paths in selected.values() for path in paths
        },
        "datasets": {name: {"count": len(paths)} for name, paths in selected.items()},
    }


@pytest.mark.parametrize("operation", ["add_file", "remove_file", "add_dataset", "remove_dataset", "remove_empty"])
def test_membership_changes_invalidate_only_affected_and_retired_owners(operation: str) -> None:
    before_selected = _selection()
    after_selected = deepcopy(before_selected)
    if operation == "add_file":
        after_selected["dataset:a"].append(Path("/tmp/new.parquet"))
        expected = {"dataset:a"}
    elif operation == "remove_file":
        after_selected["dataset:a"] = []
        expected = {"dataset:a"}
    elif operation == "add_dataset":
        after_selected["dataset:new_empty"] = []
        expected = {"dataset:new_empty"}
    elif operation == "remove_dataset":
        del after_selected["dataset:b"]
        expected = {"dataset:b"}
    else:
        del after_selected["dataset:empty"]
        expected = {"dataset:empty"}
    assert inventory_dataset_delta(
        _cache(before_selected), _cache(after_selected), after_selected,
    ) == expected


def test_same_unique_files_reassigned_between_owners_invalidate_both() -> None:
    before_selected = _selection()
    after_selected = deepcopy(before_selected)
    after_selected["dataset:a"], after_selected["dataset:b"] = (
        after_selected["dataset:b"], after_selected["dataset:a"],
    )
    before, after = _cache(before_selected), _cache(after_selected)
    assert before["files"] == after["files"]
    assert before["datasets"] == after["datasets"]
    assert inventory_dataset_delta(before, after, after_selected) == {"dataset:a", "dataset:b"}


def test_shared_physical_file_change_invalidates_all_owners() -> None:
    selected = _selection()
    selected["dataset:shared"] = list(selected["dataset:a"])
    before = _cache(selected)
    after = deepcopy(before)
    after["files"]["/tmp/a.parquet"]["stats"]["non_null"] = [0]
    assert inventory_dataset_delta(before, after, selected) == {"dataset:a", "dataset:shared"}


def test_file_order_is_conservative_but_dataset_dictionary_order_is_not() -> None:
    selected = _selection()
    selected["dataset:a"].append(Path("/tmp/second.parquet"))
    before = _cache(selected)
    reordered = dict(reversed(list(selected.items())))
    assert inventory_dataset_delta(before, _cache(reordered), reordered) == set()
    reordered["dataset:a"] = list(reversed(reordered["dataset:a"]))
    assert inventory_dataset_delta(before, _cache(reordered), reordered) == {"dataset:a"}


def test_missing_file_observation_is_affected_not_permission_to_claim_complete() -> None:
    selected = _selection()
    before = _cache(selected)
    after = deepcopy(before)
    del after["files"]["/tmp/a.parquet"]
    assert inventory_dataset_delta(before, after, selected) == {"dataset:a"}


def test_same_footer_with_different_five_part_source_identity_is_affected() -> None:
    selected = _selection()
    before = _cache(selected)
    after = deepcopy(before)
    after["files"]["/tmp/a.parquet"]["file_identity"][4] += 1
    assert inventory_dataset_delta(before, after, selected) == {"dataset:a"}


def test_schema_definition_change_invalidates_every_referencing_owner() -> None:
    selected = _selection()
    before = _cache(selected)
    after = deepcopy(before)
    after["schemas"]["schema"] = [["x", "double"]]
    assert inventory_dataset_delta(before, after, selected) == {"dataset:a", "dataset:b"}


def test_rollup_datasets_are_legal_but_do_not_own_feature_rows() -> None:
    selected = _selection()
    before = _cache(selected)
    before["datasets"]["group:example"] = {"count": 2}
    after = deepcopy(before)
    after["files"]["/tmp/a.parquet"]["stats"]["non_null"] = [0]
    assert inventory_dataset_delta(before, after, selected) == {"dataset:a"}


@pytest.mark.parametrize("side", ["before", "after"])
@pytest.mark.parametrize("corruption", [
    "missing_members", "missing_member_version", "bool_member_version", "future_member_version",
    "float_outer_version", "future_outer_version", "unbound_member_hash", "malformed_member_hash",
    "missing_dataset_aggregate", "malformed_dataset_aggregate", "malformed_files",
    "malformed_entry", "malformed_schemas", "invalid_schema_id",
])
def test_unknown_or_corrupt_contract_cannot_advertise_safe_delta(side: str, corruption: str) -> None:
    selected = _selection()
    before = _cache(selected)
    after = deepcopy(before)
    target = before if side == "before" else after
    if corruption == "missing_members":
        del target["dataset_memberships"]
    elif corruption == "missing_member_version":
        del target["dataset_membership_version"]
    elif corruption == "bool_member_version":
        target["dataset_membership_version"] = True
    elif corruption == "future_member_version":
        target["dataset_membership_version"] += 1
    elif corruption == "float_outer_version":
        target["version"] = float(INVENTORY_VERSION)
    elif corruption == "future_outer_version":
        target["version"] += 1
    elif corruption == "unbound_member_hash":
        target["dataset_memberships"]["dataset:a"] = "f" * 32
    elif corruption == "malformed_member_hash":
        target["dataset_memberships"]["dataset:a"] = "not-a-fingerprint"
    elif corruption == "missing_dataset_aggregate":
        del target["datasets"]["dataset:a"]
    elif corruption == "malformed_dataset_aggregate":
        target["datasets"]["dataset:a"] = []
    elif corruption == "malformed_files":
        target["files"] = []
    elif corruption == "malformed_entry":
        target["files"]["/tmp/a.parquet"] = None
    elif corruption == "malformed_schemas":
        target["schemas"] = []
    else:
        target["files"]["/tmp/a.parquet"]["stats"]["schema_id"] = None
    assert inventory_dataset_delta(before, after, selected) is None


def test_current_selection_must_match_new_generation_member_proof() -> None:
    selected = _selection()
    cache = _cache(selected)
    selected["dataset:a"] = []
    assert inventory_dataset_delta(cache, cache, selected) is None

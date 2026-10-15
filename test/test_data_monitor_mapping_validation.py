"""Dict-first checks retain the complete Mapping contract and source scan."""

from collections import UserDict
from collections.abc import Mapping
from datetime import date
from types import MappingProxyType
from typing import Any, get_type_hints

import pytest

from stockagent.live import data_monitor_inventory as inventory


class DictSubclass(dict):
    pass


@pytest.mark.parametrize("value", [{}, DictSubclass(), UserDict(), MappingProxyType({}),
                                  None, [], (), True, 7, "not a mapping", object()])
def test_runtime_guard_keeps_the_original_mapping_acceptance(value):
    assert isinstance(value, inventory._RUNTIME_MAPPING_TYPES) == isinstance(value, Mapping)


def test_runtime_guard_does_not_replace_annotation_mapping():
    assert inventory.Mapping is Mapping
    assert get_type_hints(inventory.FeatureDataset)["dataset_info"] == Mapping[str, Any]


def test_full_feature_and_delta_accept_non_dict_mapping_implementations(tmp_path, monkeypatch):
    import pyarrow as pa
    import pyarrow.parquet as pq

    path = tmp_path / "data_yahoo/crypto/A_features.parquet"
    path.parent.mkdir(parents=True)
    pq.write_table(pa.table({"date": [date(2026, 10, 1)], "shares": [2**53 + 1]}), path)
    frame = inventory.InventorySnapshot(tmp_path)
    result = inventory.build_record_inventory(tmp_path, refresh=True, snapshot=frame)
    assert result["mapping_validation_types"] == ["dict", "Mapping"]
    expected = inventory.build_feature_inventory(tmp_path, snapshot=frame)
    payload = frame.payload
    wrapped = MappingProxyType({**payload, "files": UserDict({
        key: MappingProxyType({**entry, "stats": UserDict(entry["stats"])})
        for key, entry in payload["files"].items()
    }), "schemas": UserDict(payload["schemas"]), "datasets": UserDict(payload["datasets"])})
    custom = inventory.InventorySnapshot(tmp_path, payload=wrapped, selected=frame.selected)
    assert inventory.build_feature_inventory(tmp_path, snapshot=custom) == expected
    assert inventory.inventory_dataset_delta(payload, wrapped, frame.selected) == set()
    monkeypatch.setattr(inventory, "_RUNTIME_MAPPING_TYPES", (Mapping,))
    legacy = inventory.InventorySnapshot(tmp_path, payload=wrapped, selected=frame.selected)
    assert inventory.build_feature_inventory(tmp_path, snapshot=legacy) == expected
    assert inventory.inventory_dataset_delta(payload, wrapped, frame.selected) == set()
    assert inventory.build_record_inventory(tmp_path)["mapping_validation_types"] == ["Mapping"]

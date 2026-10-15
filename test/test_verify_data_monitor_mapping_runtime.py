"""Native readback must bind the exact cache generation, not merely a 200."""

from copy import deepcopy

import pytest

from scripts.verify_data_monitor_mapping_runtime import accept_event


def fixture():
    cache = {"source_observation_root": "a" * 32, "feature_revision": "b" * 32, "files": {"a": {}}}
    event = {"event": "data_monitor_timing", "inventory_mapping_validation_types": ["dict", "Mapping"],
             "inventory_source_observation_root": cache["source_observation_root"],
             "inventory_feature_revision": cache["feature_revision"], "cached_files": 1,
             "feature_source_observation": {"matches_record": True, "inventory_matches_cache": True}}
    return cache, {"invocation_id": "native", "timestamp_us": "123", "event": event}


def test_latest_matching_full_generation_is_selected():
    cache, first = fixture()
    second = {**first, "invocation_id": "next"}
    assert accept_event([first, second], cache) is second


@pytest.mark.parametrize("key,value", [
    ("event", "not a timing"), ("inventory_mapping_validation_types", ["Mapping"]),
    ("inventory_source_observation_root", "c" * 32), ("inventory_feature_revision", "d" * 32),
    ("cached_files", 0), ("feature_source_observation", {"matches_record": False, "inventory_matches_cache": True}),
    ("feature_source_observation", {"matches_record": True, "inventory_matches_cache": False}),
])
def test_changed_or_incomplete_publication_cannot_pass(key, value):
    cache, row = fixture()
    bad = deepcopy(row)
    bad["event"][key] = value
    with pytest.raises(ValueError, match="no complete native event"):
        accept_event([bad], cache)

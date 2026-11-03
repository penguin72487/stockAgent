from copy import deepcopy
from stockagent.data_sync.storage_monitor import CONTRACT, epoch, public_status


def snapshot():
    def cap(available):
        return {"state": "measured", "source": "statvfs", "observed_at_utc": "2026-10-31T00:00:00+00:00",
                "total_bytes": 1000, "used_bytes": 1000 - available, "free_bytes": available,
                "available_bytes": available, "reserved_free_bytes": 0}
    return {"contract": CONTRACT, "volumes": [
        {"id": "penguin-wsl", "contained_in": "penguin-c", "capacity": cap(800)},
        {"id": "penguin-c", "capacity": cap(100)}]}


def wsl(raw):
    public = public_status(raw, now=epoch("2026-10-31T00:01:00+00:00"))
    return public["nodes"][0]["volumes"][0]


def test_headroom_uses_smaller_physical_backing_without_changing_raw_guest():
    raw = snapshot()
    raw["volumes"][0]["writable_headroom"] = {"available_bytes": 999999}
    row = wsl(raw)
    assert row["capacity"]["available_bytes"] == 800
    assert row["writable_headroom"]["available_bytes"] == 100
    raw["volumes"][0]["capacity"].update(used_bytes=950, free_bytes=50, available_bytes=50)
    assert wsl(raw)["writable_headroom"]["available_bytes"] == 50


def test_unknown_stale_failed_or_invalid_backing_is_not_false_free_space():
    original = snapshot()
    for change in ({"observed_at_utc": "2026-10-30T00:00:00+00:00"},
                   {"state": "unavailable"}, {"free_bytes": 200}, {"total_bytes": True}):
        raw = deepcopy(original)
        raw["volumes"][1]["capacity"].update(change)
        assert wsl(raw)["writable_headroom"]["available_bytes"] is None
    raw = snapshot()
    raw["volumes"].pop()
    assert wsl(raw)["writable_headroom"]["state"] == "unknown"
    raw = snapshot()
    raw["volumes"][0]["contained_in"] = "nas-disk"
    assert wsl(raw)["writable_headroom"]["available_bytes"] is None


def test_service_placement_does_not_present_old_directory_names_as_current():
    raw = snapshot()
    raw["volumes"][0]["classification"] = {"state": "complete", "observed_at_utc": "2026-10-31T00:00:00+00:00"}
    raw["service_layout_generation"] = {"contract": "stockagent.service_layout_generation.v1",
                                         "changed_at_utc": "2026-10-31T00:00:30+00:00"}
    row = wsl(raw)
    assert row["classification"]["layout_changed"] is True
    assert row["classification"]["stale"] is True
    raw["volumes"][0]["classification"]["observed_at_utc"] = "2026-10-31T00:00:45+00:00"
    assert wsl(raw)["classification"]["layout_changed"] is False

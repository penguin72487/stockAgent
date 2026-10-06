import json
import os
import time
from pathlib import Path

import pytest

from scripts import deduplicate_inactive_panel_caches as compact
from stockagent.data_sync import remote_legacy_return as remote


def cohort(tmp_path, monkeypatch):
    monkeypatch.setattr(compact, "current_service_blockers", lambda *_: [])
    monkeypatch.setattr(compact, "process_references_many", lambda *_: [])
    monkeypatch.setattr(remote, "artifact_process_references", lambda *_: [])
    monkeypatch.setattr(remote, "active_configuration_references", lambda *_: [])
    rows = []
    for name in ("one", "two"):
        root = tmp_path / "artifacts/cache" / name
        panel = root / "panel_cache_v2"
        generation = panel / "generations" / ("a" * 32)
        generation.mkdir(parents=True)
        (panel / ".write.lock").touch()
        (panel / "meta.json").write_text(json.dumps({"arrays": {"features": {}}}))
        array = generation / "features.npy"
        array.write_bytes(b"identical immutable generated array" * 100)
        os.utime(array, ns=(1, 1))
        inventory = remote.metadata_tree(root)
        rows.append({"name": name, "path": str(root), "fingerprint": inventory["fingerprint"],
                     "service_error": None, "service_references": [], "process_references": [],
                     "metadata": [{"path": "panel_cache_v2/meta.json"}], "rows": inventory["rows"]})
    return {"schema_version": 1, "observed_at_epoch": time.time(),
            "direct_process_references": [], "caches": rows}


def test_exact_compaction_preserves_all_names(tmp_path, monkeypatch):
    inventory = cohort(tmp_path, monkeypatch)
    args = {"receipt_dir": tmp_path / "receipts", "repo": tmp_path}
    plan = compact.compact_current_inventory(inventory, apply=False, **args)
    assert plan["would_free_allocated_bytes"] > 0
    assert plan["reclaimed_allocated_bytes"] == 0
    result = compact.compact_current_inventory(inventory, apply=True, **args)
    assert result["replaced_files"] == 1
    assert result["unique_payloads_removed"] == result["logical_paths_removed"] == 0
    arrays = list((tmp_path / "artifacts/cache").rglob("features.npy"))
    assert len(arrays) == 2 and arrays[0].samefile(arrays[1])


@pytest.mark.parametrize("failure", ["stale", "changed", "consumer", "schema"])
def test_compaction_fails_closed(tmp_path, monkeypatch, failure):
    inventory = cohort(tmp_path, monkeypatch)
    if failure == "stale":
        inventory["observed_at_epoch"] -= 7201
    elif failure == "schema":
        inventory["caches"][0]["service_error"] = "UnreadableConfiguration"
    elif failure == "changed":
        (Path(inventory["caches"][0]["path"]) / "new-original.parquet").write_bytes(b"unique")
    elif failure == "consumer":
        monkeypatch.setattr(compact, "current_service_blockers", lambda *_: ["active-service"])
    else:
        (Path(inventory["caches"][0]["path"]) / "panel_cache_v2/.write.lock").unlink()
        inventory["caches"][0]["fingerprint"] = remote.metadata_tree(Path(inventory["caches"][0]["path"]))["fingerprint"]
    with pytest.raises((ValueError, FileNotFoundError)):
        compact.compact_current_inventory(inventory, apply=True, receipt_dir=tmp_path / "receipts", repo=tmp_path)
    assert len(list((tmp_path / "artifacts/cache").rglob("features.npy"))) == 2


def test_protected_service_root_is_not_selected(tmp_path, monkeypatch):
    inventory = cohort(tmp_path, monkeypatch)
    inventory["caches"][0]["service_references"] = ["current-service"]
    result = compact.compact_current_inventory(inventory, apply=True, receipt_dir=tmp_path / "receipts", repo=tmp_path)
    assert result["replaced_files"] == 0
    arrays = list((tmp_path / "artifacts/cache").rglob("features.npy"))
    assert not arrays[0].samefile(arrays[1])


def test_new_active_training_is_protected_after_inventory(tmp_path, monkeypatch):
    inventory = cohort(tmp_path, monkeypatch)
    monkeypatch.setattr(remote, "active_configuration_references",
                        lambda path, _: ["active-training"] if path.name == "one" else [])
    result = compact.compact_current_inventory(inventory, apply=True, receipt_dir=tmp_path / "receipts", repo=tmp_path)
    assert result["replaced_files"] == 0
    assert result["protected_roots"][0]["name"] == "one"


def test_missing_writer_lock_preserves_root(tmp_path, monkeypatch):
    inventory = cohort(tmp_path, monkeypatch)
    root = Path(inventory["caches"][0]["path"])
    (root / "panel_cache_v2/.write.lock").unlink()
    inventory["caches"][0]["fingerprint"] = remote.metadata_tree(root)["fingerprint"]
    result = compact.compact_current_inventory(inventory, apply=True, receipt_dir=tmp_path / "receipts", repo=tmp_path)
    assert result["replaced_files"] == 0
    assert result["protected_roots"][0]["reason"] == "canonical-writer-lock-unavailable"

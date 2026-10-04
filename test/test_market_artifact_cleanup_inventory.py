from __future__ import annotations

import json
import os
from pathlib import Path
import time

from scripts import audit_market_artifact_cleanup as audit


def test_inventory_deduplicates_inodes_and_never_authorizes_eviction(
    tmp_path, monkeypatch
):
    repo = tmp_path / "repo"
    first = repo / "artifacts/markets/first"
    second = repo / "artifacts/markets/second"
    first.mkdir(parents=True)
    second.mkdir()
    payload = first / "payload"
    payload.write_bytes(b"unique bytes" * 600)
    os.link(payload, second / "alias")
    os.utime(payload, (time.time() - 10 * 86400,) * 2)
    os.mkfifo(second / "fifo")
    monkeypatch.setattr(
        audit,
        "artifact_service_references",
        lambda roots, _repo: {str(root): [] for root in roots},
    )
    monkeypatch.setattr(audit, "artifact_process_references", lambda *_: [])
    monkeypatch.setattr(audit, "load_cold_artifact_registry", lambda _: {})
    monkeypatch.setattr(audit, "load_legacy_specs", lambda _: {})
    output = tmp_path / "report"
    result = audit.inventory(repo, output)
    assert result["unique_file_allocated_bytes"] == payload.stat().st_blocks * 512
    assert not result["eviction_authorized_by_inventory"]
    rows = {Path(row["root"]).name: row for row in result["roots"]}
    assert rows["first"]["classification"] == "archive-before-retirement"
    assert rows["second"]["classification"] == "keep-non-regular-or-managed-link"
    assert rows["second"]["non_regular"] == ["fifo"]
    assert all(not row["eviction_authorized"] for row in result["roots"])
    assert (second / "alias").read_bytes() == payload.read_bytes()
    assert json.loads((output / "inventory.json").read_text())["root_count"] == 2

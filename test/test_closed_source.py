from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import hashlib
import json
from pathlib import Path
import subprocess
import sys

import pytest

from scripts import download_cftc_legacy_pre2000 as collector
from scripts import publish_data_releases as releases
from stockagent.data_sync import closed_source as closed
from stockagent.data_sync import packed_snapshots as packed
from stockagent.data_sync.desync_snapshots import SnapshotError


@pytest.fixture
def cohort(tmp_path, request):
    source, cold, cache = (tmp_path / name for name in (getattr(request, "param", "source"), "cold", "cache"))
    source.mkdir()
    values = {name: b"fixed original " + name.encode() for name in closed.SERVICE_VIEWS}
    values.update({"README.md": b"official closed history", "manifest.json": b'{"rows": 12}',
                   closed.LOCK: b""})
    raws = []
    for mode in ("legacy_futures_only", "legacy_futures_and_options_combined"):
        content = (mode + " fixed ZIP").encode()
        digest = hashlib.sha256(content).hexdigest()
        name = f"raw/{mode}_{digest}.zip"
        raws.append(name)
        values[name] = content
        values[f"receipts/{mode}.json"] = json.dumps({
            "raw_path": name, "raw_bytes": len(content), "raw_sha256": digest}).encode()
    for name, content in values.items():
        path = source / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
    resolved = packed.publish_packed_snapshot(cold, closed.DATASET, source,
        node_id="test-node", pack_buckets=2, file_chunk_bytes=16)
    config = tmp_path / "retention.json"
    config.write_text(json.dumps({"schema_version": 1, "authority_node_id": "test-node",
        "sync_root": str(cold), "archive_root": str(tmp_path / "archive"),
        "materialized_root": str(cache), "backup_config": "unused.json",
        "state_dir": str(tmp_path / "state"), "folder_id": "fixture", "required_peer_names": []}))
    return source, cold, cache, resolved, raws, config


@contextmanager
def original_owner(source):
    with (source / closed.LOCK).open("r+b") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield handle.fileno()


def adopt(cohort):
    source, cold, cache, resolved, _, _ = cohort
    with original_owner(source) as fd:
        return closed.adopt_closed_source(source, cold, cache,
            snapshot_id=resolved.manifest["snapshot_id"], source_lock_fd=fd)


def test_adoption_recovers_every_original_and_preserves_source_names(cohort):
    source, cold, cache, expected, _, _ = cohort
    original = {str(path.relative_to(source)): path.read_bytes()
                for path in source.rglob("*") if path.is_file()}
    result, status = adopt(cohort)
    binding = json.loads((source / closed.BINDING).read_text())
    assert result.manifest_sha256 == expected.manifest_sha256
    assert status["raw_files_local"] == 2
    assert binding["recovery"]["independently_reconstructed_files"] == 10
    assert binding["recovery"]["materialized_verified"]
    assert binding["retirement_authorized_by_binding"] is False
    assert all((source / name).read_bytes() == content for name, content in original.items())
    assert closed.resolve_closed_source(source, cold, cache)[0].manifest_sha256 == expected.manifest_sha256


def test_daily_closed_collector_never_calls_api_or_rewrites_after_raw_retirement(cohort, monkeypatch, capsys):
    source, cold, _, _, raws, config = cohort
    adopt(cohort)
    # Test fixture only: production retirement retains its independent gates.
    for name in raws:
        (source / name).unlink()
    before = {p: (p.stat().st_mtime_ns, p.read_bytes()) for p in source.rglob("*") if p.is_file()}
    monkeypatch.setattr(collector, "ResilientHttpTransport", lambda *_: pytest.fail("no API client"))
    monkeypatch.setattr(collector, "_write_parquet", lambda *_: pytest.fail("no Parquet rewrite"))
    assert collector.main(["--output-dir", str(source), "--sync-root", str(cold),
                           "--retention-config", str(config)]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["provider_API_calls"] == result["parquet_rewrites"] == result["raw_downloads"] == 0
    assert result["storage"]["raw_workspace_empty"]
    assert not result["storage"]["raw_retirement_complete"]
    assert result["storage"]["service_files"] == 3
    assert all((p.stat().st_mtime_ns, p.read_bytes()) == value for p, value in before.items())


def test_publisher_reuses_whole_fixed_release_instead_of_publishing_partial_workspace(cohort, monkeypatch):
    source, cold, _, expected, raws, config = cohort
    adopt(cohort)
    for name in raws:
        (source / name).unlink()
    monkeypatch.setattr(releases, "_running_commands", lambda: [])
    monkeypatch.setattr(releases, "publish_packed_snapshot", lambda *_a, **_k: pytest.fail("no partial publication"))
    entry = {"dataset": closed.DATASET, "source": str(source), "role": "fixture",
        "publish": True, "note": "closed fixture", "closed_source_contract": closed.CONTRACT,
        "source_coordination_lock_scope": "source_parent",
        "source_coordination_lock": "source/state/collector.lock"}
    status, result = releases._publish_entry(entry,
        argparse.Namespace(retention_config=config), cold)
    assert result.manifest_sha256 == expected.manifest_sha256
    assert status["fixed_closed_release_reused"]
    assert status["source_storage"]["raw_workspace_empty"]
    assert not status["source_storage"]["raw_retirement_complete"]
    assert result.manifest["source"]["files"] == 10


def test_canonical_cli_reports_zero_new_transfer_for_closed_release_reuse(cohort, monkeypatch, capsys):
    source, cold, _, _, _, config = cohort
    adopt(cohort)
    entry = {"dataset": closed.DATASET, "source": str(source), "role": "fixture",
        "publish": True, "note": "closed fixture", "closed_source_contract": closed.CONTRACT,
        "source_coordination_lock_scope": "source_parent",
        "source_coordination_lock": "source/state/collector.lock"}
    monkeypatch.setattr(releases, "_load_catalog", lambda *_: [entry])
    monkeypatch.setattr(releases, "_running_commands", lambda: [])
    assert releases.main(["publish", closed.DATASET, "--sync-root", str(cold),
                          "--retention-config", str(config), "--node-id", "test-node"]) == 0
    result = json.loads(capsys.readouterr().out)["published"][0]
    assert result["new_sync_objects"] == result["new_sync_bytes"] == result["changed_files"] == 0
    assert result["reused_files"] == result["source_files"] == 10
    assert result["fixed_closed_release_reused"]


@pytest.mark.parametrize("failure", ["changed-view", "missing-view", "new-file", "new-directory",
                                    "redirected-view", "bad-binding", "bad-pin", "missing-object"])
def test_changed_or_incomplete_binding_fails_without_fallback(cohort, failure):
    source, cold, cache, resolved, _, _ = cohort
    adopt(cohort)
    view = source / sorted(closed.SERVICE_VIEWS)[0]
    if failure == "changed-view":
        content = view.read_bytes()
        view.write_bytes(b"X" + content[1:])
    elif failure == "missing-view":
        view.unlink()
    elif failure == "new-file":
        (source / "unpublished-raw.zip").write_bytes(b"unique data")
    elif failure == "new-directory":
        (source / "unexpected").mkdir()
    elif failure == "redirected-view":
        view.unlink()
        view.symlink_to(source / sorted(closed.SERVICE_VIEWS)[1])
    elif failure == "bad-binding":
        (source / closed.BINDING).write_text("{}")
    elif failure == "bad-pin":
        closed._pin_path(cache).write_text("{}")
    elif failure == "missing-object":
        (cold / resolved.manifest["archive"]["objects"][0]["relpath"]).unlink()
    with pytest.raises((SnapshotError, OSError)):
        closed.resolve_closed_source(source, cold, cache)


def test_no_binding_never_authorizes_partial_original_publication(cohort):
    source, cold, cache, _, raws, _ = cohort
    assert closed.resolve_closed_source(source, cold, cache) is None
    closed.require_unbound_originals(source)
    (source / raws[0]).unlink()
    with pytest.raises((SnapshotError, OSError)):
        closed.require_unbound_originals(source)


def test_adoption_requires_actual_held_original_lock(cohort):
    source, cold, cache, resolved, _, _ = cohort
    with (source / closed.LOCK).open("r+b") as handle:
        with pytest.raises(SnapshotError, match="exclusive original"):
            closed.adopt_closed_source(source, cold, cache,
                snapshot_id=resolved.manifest["snapshot_id"], source_lock_fd=handle.fileno())
    assert not (source / closed.BINDING).exists()


def test_repeat_adoption_keeps_exact_pin_and_binding(cohort):
    source, _, cache, _, _, _ = cohort
    adopt(cohort)
    paths = (source / closed.BINDING, closed._pin_path(cache))
    before = [(p.stat().st_ino, p.stat().st_mtime_ns, p.read_bytes()) for p in paths]
    adopt(cohort)
    assert before == [(p.stat().st_ino, p.stat().st_mtime_ns, p.read_bytes()) for p in paths]


def test_local_monitor_never_reads_raw_or_cold_payload(cohort, monkeypatch):
    source, _, _, _, _, _ = cohort
    adopt(cohort)
    monkeypatch.setattr(packed, "resolve_packed_pin", lambda *_a, **_k: pytest.fail("no D lookup"))
    monkeypatch.setattr(closed, "_hash", lambda *_a: pytest.fail("no payload hashing"))
    result = closed.local_closed_source_status(source)
    assert result["verification_scope"] == "adoption_receipt_and_current_local_sizes"
    assert result["raw_files_local"] == 2
    assert result["service_files"] == 3
    assert not result["workspace_hashes_verified"]
    assert "source" not in result


def test_panel_explains_retained_raw_and_keeps_observation_rows(cohort):
    from stockagent.live.data_monitor_dashboard import _generic_group
    source, _, _, _, _, _ = cohort
    adopt(cohort)
    group = _generic_group(source.parent, {"dataset": closed.DATASET, "source": source.name,
        "role": "fixture", "publish": True, "note": "fixture"}, now=datetime.now(timezone.utc))
    assert group["rows"] == 12
    assert group["source_storage"]["service_files"] == 3
    assert any("等待獨立回收驗收" in item for item in group["warnings"])


def test_closed_collector_import_keeps_dataframe_engines_lazy():
    result = subprocess.run([sys.executable, "-c",
        "import sys; import scripts.download_cftc_legacy_pre2000; "
        "assert 'pandas' not in sys.modules; assert 'pyarrow' not in sys.modules"],
        capture_output=True, text=True)
    assert result.returncode == 0, result.stderr

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from scripts.publish_data_releases import _load_catalog, main
import scripts.publish_data_releases as publisher
from stockagent.data_sync.dataset_version_layout import (
    dataset_roots, inventory_version_layout, publication_version_options, source_root_entries, version_policy,
)
from stockagent.data_sync.desync_snapshots import SnapshotError


REPO = Path(__file__).resolve().parents[1]


@pytest.fixture
def layout_repo(tmp_path):
    """Use checked-in policy, never depend on a developer's private data lake."""
    root = tmp_path / "checkout"
    (root / "configs/data_sync").mkdir(parents=True)
    document = json.loads((REPO / "configs/columnar_storage.json").read_text())
    (root / "configs/columnar_storage.json").write_text(json.dumps(document))
    names = set(document["source_versioning"]["roots"])
    assert {path.name for path in REPO.glob("data_*") if path.is_dir()}.issubset(names)
    for name in names:
        (root / name).mkdir()
    catalog = json.loads((REPO / "configs/data_sync/packed_datasets.json").read_text())
    for entry in catalog["datasets"]:
        parts = Path(entry["source"]).parts
        index = next((i for i, part in enumerate(parts) if part in names), None)
        if index is not None:
            entry["source"] = str(Path(*parts[index:]))
    (root / "configs/data_sync/packed_datasets.json").write_text(json.dumps(catalog))
    return root


def test_every_existing_data_root_registered_with_its_existing_publication_owner(layout_repo):
    entries = _load_catalog(layout_repo / "configs/data_sync/packed_datasets.json")
    result = inventory_version_layout(layout_repo, entries)
    assert result["state"] == "accepted", result["errors"]
    assert result["root_count"] == len(version_policy(layout_repo)["roots"])
    assert all(row["publication_bindings"] for row in result["roots"])
    assert result["network_calls"] == result["source_payload_reads"] == result["source_payload_writes"] == 0
    assert result["legacy_deletions"] == 0


def test_all_source_catalog_entries_share_v2_policy_without_enabling_private_publication(layout_repo):
    entries = _load_catalog(layout_repo / "configs/data_sync/packed_datasets.json", True)
    policy = version_policy(layout_repo)
    roots = set()
    for entry in entries:
        source = Path(entry["source"])
        source = source if source.is_absolute() else layout_repo / source
        names = dataset_roots(layout_repo, source, policy)
        roots.update(names)
        options = publication_version_options(layout_repo, entry, reader_contract="packed-v2-chunks")
        assert options == ({"file_chunk_bytes": 8 * 1024**2}
                           if names or "file_chunk_bytes" in entry else {})
        assert publication_version_options(layout_repo, entry, reader_contract="packed-v1") == (
            {"file_chunk_bytes": entry["file_chunk_bytes"]} if "file_chunk_bytes" in entry else {})
    assert roots == set(policy["roots"])
    private = {entry["dataset"]: entry["publish"] for entry in entries}
    assert all(private[name] is False for name in (
        "finlab-research", "finmind-free", "tej-research", "news-training-local", "news-fulltext-local"))


def test_actual_source_entries_are_enrolled_without_enrolling_artifact_families(layout_repo):
    entries = _load_catalog(layout_repo / "configs/data_sync/packed_datasets.json", True)
    for entry in entries:
        options = publication_version_options(layout_repo, entry, reader_contract=None)
        assert options == ({"file_chunk_bytes": 8 * 1024**2}
                           if "publication_family" not in entry else {})


def test_rollout_readback_distinguishes_policy_from_an_actual_committed_head(layout_repo, monkeypatch):
    from types import SimpleNamespace
    import stockagent.data_sync.dataset_version_layout as layout
    entries = _load_catalog(layout_repo / "configs/data_sync/packed_datasets.json")
    before = inventory_version_layout(layout_repo, entries)
    row = next(item for item in before["roots"] if item["root"] == "data_finlab")
    assert row["rollout_state"] == "policy_enrolled_head_pending"
    assert row["configured_v2_publication_branches"] == ["finlab-research"]
    assert row["shared_v2_head_observed"] is False
    cold = layout_repo / "cold"
    (cold / "heads" / "finlab-research").mkdir(parents=True)
    monkeypatch.setattr(layout, "resolve_latest_packed", lambda *args, **kwargs: SimpleNamespace(
        manifest={"snapshot_id": "fixture", "schema_version": 2,
                  "archive": {"file_chunk_bytes": 8 * 1024**2}}, manifest_sha256="a" * 64))
    after = inventory_version_layout(layout_repo, entries, sync_root=cold)
    row = next(item for item in after["roots"] if item["root"] == "data_finlab")
    assert row["rollout_state"] == "shared_v2_head_observed"
    assert row["shared_v2_head_observed"] is True
    assert row["publication_bindings"][0]["head"]["objects_verified"] is False
    assert row["publication_bindings"][0]["publish"] is False
    assert row["native_workspace_migrated"] is False


@pytest.mark.parametrize("accepted", [False, True])
def test_native_news_migration_requires_bound_complete_acceptance_receipt(layout_repo, accepted):
    root = layout_repo / "data_news"
    manifest = {"contract_version": 2, "storage_layout": "news_incremental_parts_v2", "state": "ready",
                "generation": "versions/fixed", "row_counts": {"articles": 7},
                "parts": [{"bytes": 23, "rows": 7}]}
    (root / "manifest.json").write_text(json.dumps(manifest))
    entries = _load_catalog(layout_repo / "configs/data_sync/packed_datasets.json")
    before = inventory_version_layout(layout_repo, entries)
    row = next(item for item in before["roots"] if item["root"] == "data_news")
    assert row["native_workspace_migrated"] is False
    assert row["rollout_state"] == "local_news_v2_acceptance_pending"
    proof = {"state": "verified", "generation": manifest["generation"], "row_counts": manifest["row_counts"],
             "parts_verified": 1, "bytes_verified": 23, "document_hashes_verified": True,
             "quality": {"state": "verified"}, "verified_at_utc": "2026-10-17T00:00:00Z",
             "manifest_content_sha256": hashlib.sha256(json.dumps(manifest, sort_keys=True).encode()).hexdigest()}
    if not accepted:
        proof["generation"] = "versions/another"
    (root / "verification.json").write_text(json.dumps(proof))
    result = inventory_version_layout(layout_repo, entries)
    row = next(item for item in result["roots"] if item["root"] == "data_news")
    assert row["native_workspace_migrated"] is accepted
    assert row["native_acceptance"]["payload_reverified_this_inventory"] is False
    assert row["rollout_state"] == ("local_news_v2_verified_receipt" if accepted else "local_news_v2_acceptance_pending")
    assert result["source_payload_reads"] == result["source_payload_writes"] == 0


def test_unknown_new_data_root_is_not_silently_left_out(tmp_path):
    (tmp_path / "configs").mkdir()
    document = json.loads((REPO / "configs/columnar_storage.json").read_text())
    document["source_versioning"]["roots"] = {}
    (tmp_path / "configs/columnar_storage.json").write_text(json.dumps(document))
    (tmp_path / "data_new_provider").mkdir()
    result = inventory_version_layout(tmp_path, [])
    assert result["state"] == "failed"
    assert result["errors"] == ["unregistered data root: data_new_provider"]


def test_metadata_inventory_never_reads_market_rows_or_recurses_sources(monkeypatch, layout_repo):
    entries = _load_catalog(layout_repo / "configs/data_sync/packed_datasets.json")
    monkeypatch.setattr(Path, "rglob", lambda *a, **kw: pytest.fail("unbounded source recursion"))
    result = inventory_version_layout(layout_repo, entries)
    assert result["state"] == "accepted"


@pytest.mark.parametrize("field,value", [("file_chunk_bytes", True), ("file_chunk_bytes", 0),
                                        ("legacy_payload_deletion", True),
                                        ("local_versioning_grants_replication", True)])
def test_unsafe_policy_rejected(tmp_path, field, value):
    (tmp_path / "configs").mkdir()
    document = json.loads((REPO / "configs/columnar_storage.json").read_text())
    document["source_versioning"][field] = value
    (tmp_path / "configs/columnar_storage.json").write_text(json.dumps(document))
    with pytest.raises(SnapshotError):
        version_policy(tmp_path)


def test_existing_publisher_can_read_back_all_root_layout_without_creating_versions(capsys, monkeypatch, layout_repo):
    monkeypatch.setattr(publisher, "REPO_ROOT", layout_repo)
    assert main(["layout", "data_finlab", "--catalog", str(layout_repo / "configs/data_sync/packed_datasets.json")]) == 0
    output = json.loads(capsys.readouterr().out)
    assert output["root_count"] == 1
    row = output["roots"][0]
    assert row["native_layout"] == "content_addressed_whole_table_revisions"
    assert row["native_workspace_migrated"] is False
    assert row["shared_v2_head_observed"] is False


def test_per_entry_chunk_format_requires_explicit_reader_enrollment(tmp_path):
    path = tmp_path / "catalog.json"
    entry = {"dataset": "one", "source": "data_finlab", "file_chunk_bytes": 8 * 1024**2}
    path.write_text(json.dumps({"schema_version": 1, "datasets": [entry]}))
    with pytest.raises(SnapshotError, match="packed-v2-chunks"):
        _load_catalog(path)
    entry["reader_contract"] = "packed-v2-chunks"
    path.write_text(json.dumps({"schema_version": 1, "datasets": [entry]}))
    assert _load_catalog(path)[0]["file_chunk_bytes"] == 8 * 1024**2


def test_source_rollout_filter_keeps_private_gates_and_excludes_artifact_families(layout_repo):
    entries = _load_catalog(layout_repo / "configs/data_sync/packed_datasets.json", True)
    selected = source_root_entries(layout_repo, entries)
    assert selected and len(selected) < len(entries)
    assert not any("publication_family" in item for item in selected)
    assert any(item["dataset"] == "finlab-research" and item["publish"] is False for item in selected)
    assert all(dataset_roots(layout_repo,
        Path(item["source"]) if Path(item["source"]).is_absolute() else layout_repo / item["source"],
        version_policy(layout_repo)) for item in selected)


@pytest.mark.parametrize("arguments", [
    ["status", "--source-roots-only"], ["layout", "--all-ready", "--source-roots-only"],
    ["publish", "one", "--source-roots-only"],
    ["publish", "one", "--all-ready", "--source-roots-only"],
])
def test_source_filter_refuses_ambiguous_scope_before_any_write(arguments, monkeypatch):
    monkeypatch.setattr(publisher, "initialize_packed_layout", lambda *a, **kw: pytest.fail("invalid scope wrote state"))
    assert main(arguments) == 2


def test_source_filter_is_applied_before_original_publication_owner(layout_repo, monkeypatch, capsys):
    catalog = layout_repo / "configs/data_sync/packed_datasets.json"
    entries = _load_catalog(catalog, True)
    expected = {item["dataset"] for item in source_root_entries(layout_repo, entries)}
    observed = []
    monkeypatch.setattr(publisher, "REPO_ROOT", layout_repo)
    def publish(entry, *_):
        observed.append(entry["dataset"])
        return {"dataset": entry["dataset"], "publish_ready": False}, None
    monkeypatch.setattr(publisher, "_publish_entry", publish)
    assert main(["publish", "--all-ready", "--source-roots-only", "--catalog", str(catalog),
                 "--sync-root", str(layout_repo / "cold"), "--node-id", "test-node"]) == 0
    assert set(observed) == expected
    assert len(json.loads(capsys.readouterr().out)["skipped"]) == len(expected)

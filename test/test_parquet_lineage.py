"""Reader-first research receipts retain byte identity and complete membership."""
from contextlib import nullcontext
from datetime import UTC, date, datetime, timedelta
import json
import os
from pathlib import Path
import shutil
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from stockagent.data import partitioned_parquet as parts
from stockagent.data.parquet_lineage import (
    ParquetLineage, assert_receipt, inventory_hashes, receipt_fields, release_inventory,
)
from stockagent.data_sync.desync_snapshots import sha256_file


@pytest.fixture(autouse=True)
def private_capacity(tmp_path, monkeypatch):
    from stockagent.data_sync import cache_capacity
    controls = tmp_path / "controls"
    controls.mkdir()
    monkeypatch.setattr(cache_capacity, "CONTROL_ROOT", controls)
    monkeypatch.setattr(cache_capacity, "_admit_backing_volume", lambda *_: {})


def formal_source(tmp_path, partitioned=True):
    root = tmp_path / "data_tw_public"
    path = root / "features/tw_public_stock_daily.parquet"
    table = pa.table({"date": pa.array([date(2024, 1, 2), date(2025, 1, 2)], type=pa.date32()),
                      "symbol": ["2330", "2330"], "value": [-0.0, None]})
    if partitioned:
        parts.write_partitioned_table(table, path, source_identity={"fixture": "original"})
    else:
        path.parent.mkdir(parents=True)
        pq.write_table(table, path)
    return root, path, table


@pytest.mark.parametrize("partitioned", [False, True])
def test_byte_lineage_roundtrip_and_relocation(tmp_path, partitioned):
    root, path, _ = formal_source(tmp_path, partitioned)
    before = ParquetLineage(path)
    fields = receipt_fields(before.identity, hash_key="base_feature_sha256", source_key="base_feature_source")
    assert ("base_feature_sha256" in fields) is not partitioned
    if not partitioned:
        assert fields["base_feature_sha256"] == sha256_file(path)
    expected = inventory_hashes(before.identity, "features/tw_public_stock_daily.parquet")
    assert release_inventory(fields) == expected
    assert len(expected) == (3 if partitioned else 1)
    restored = tmp_path / "relocated"
    shutil.copytree(root, restored)
    after = ParquetLineage(restored / path.relative_to(root))
    assert after.identity == before.identity
    assert_receipt(after.identity, fields, hash_key="base_feature_sha256", source_key="base_feature_source")


@pytest.mark.parametrize("failure", ["missing", "corrupt_same_size_mtime", "mixed_abi"])
def test_all_old_members_are_required_before_receipt_acceptance(tmp_path, failure):
    _, path, table = formal_source(tmp_path)
    initial = ParquetLineage(path)
    member = initial.source.members[0]
    before = member.stat()
    if failure == "missing":
        member.rename(member.with_suffix(".offline"))
    elif failure == "mixed_abi":
        pq.write_table(table, path)
    else:
        raw = bytearray(member.read_bytes())
        raw[len(raw)//2] ^= 1
        member.chmod(0o600)
        member.write_bytes(raw)
        member.chmod(0o444)
        os.utime(member, ns=(before.st_atime_ns, before.st_mtime_ns))
    with pytest.raises((OSError, ValueError)):
        ParquetLineage(path)
    with pytest.raises((OSError, ValueError)):
        initial.verify()


def test_typed_inventory_is_not_a_replacement_for_old_sha_fields(tmp_path):
    _, path, _ = formal_source(tmp_path)
    identity = ParquetLineage(path).identity
    assert_receipt(identity, {"source": identity})
    for receipt in ({"sha256": "0"*64}, {"sha256": None, "source": identity}):
        with pytest.raises(RuntimeError, match="lineage mismatch"):
            assert_receipt(identity, receipt)
    with pytest.raises(ValueError, match="ambiguous"):
        release_inventory({"base_feature_source": identity, "base_feature_sha256": "0"*64})
    with pytest.raises(ValueError):
        inventory_hashes(identity, "../features/tw_public_stock_daily.parquet")
    broken = {**identity, "files": [*identity["files"], {"path": "../../outside", "sha256": "0"*64}]}
    with pytest.raises(ValueError, match="namespace"):
        inventory_hashes(broken, "features/tw_public_stock_daily.parquet")


def test_stage_expands_every_exact_cold_member_not_old_generations(tmp_path):
    from scripts.stage_tw_public_research_release import (
        FORMAL_MEMBERS, _required_formal_members, _action_members, _assert_formal_inventory,
    )
    root, path, table = formal_source(tmp_path)
    old = ParquetLineage(path)
    parts.write_partitioned_table(table.append_column("new_field", pa.array([None, None], type=pa.float64())),
                                root / "other.parquet", source_identity={"fixture": "unrelated"})
    summary = {"raw_receipt_manifest": {"relative_path": "raw/actions.jsonl"}}
    members = _required_formal_members(summary, root, feature_identity=old.identity)
    required = inventory_hashes(old.identity, FORMAL_MEMBERS[0])
    assert set(members) == {*required, *FORMAL_MEMBERS[1:], "raw/actions.jsonl"}
    assert _action_members(members, old.identity) == (*FORMAL_MEMBERS[1:], "raw/actions.jsonl")
    _assert_formal_inventory(old.identity, required)
    for missing in required:
        with pytest.raises(RuntimeError, match="cold inventory"):
            _assert_formal_inventory(old.identity, {key: value for key, value in required.items() if key != missing})


@pytest.mark.parametrize("partitioned", [False, True])
def test_finlab_stage_and_remote_readiness_share_exact_formal_lineage(tmp_path, monkeypatch, capsys, partitioned):
    from scripts import stage_tw_public_finlab_research_release as stage
    from scripts import check_finlab_remote_training_ready as remote
    from scripts.stage_tw_public_research_release import FORMAL_MEMBERS
    root, path, table = formal_source(tmp_path, partitioned)
    stocks = root / "stocks"
    stocks.mkdir()
    parts.write_partitioned_table(table, stocks / "2330_features.parquet", source_identity={"test": "BASE"})
    formal = ParquetLineage(path)
    for name in FORMAL_MEMBERS[1:]:
        target = root / name
        target.write_text(json.dumps({"raw_receipt_manifest": {"relative_path": "raw/actions.jsonl"}}))
    (root / "raw").mkdir()
    (root / "raw/actions.jsonl").write_text('{"original":"evidence"}\n')
    base = tmp_path / "all.parquet"
    pq.write_table(table, base)
    base.with_suffix(".all_features.json").write_text(json.dumps({
        "output_sha256": sha256_file(base), "inputs": {"official": receipt_fields(formal.identity)}}))
    feature = tmp_path / "finlab.parquet"
    finlab_names = [f"twfl_{index}" for index in range(104)]
    values = {"date": table["date"], "symbol": table["symbol"],
              **{f"twpub_{index}": [None, -0.0] for index in range(204)},
              **{name: [-0.0, None] for name in finlab_names}}
    pq.write_table(pa.table(values), feature)
    feature.with_suffix(".finlab_research.json").write_text(json.dumps({"research_only": True,
        "historical_point_in_time": False, "output_sha256": sha256_file(feature), "rows": 2,
        "feature_columns": finlab_names, "inputs": {"base_sha256": sha256_file(base), "sources": {}}}))
    deadline = (datetime.now(UTC) + timedelta(hours=1)).isoformat()
    readiness = {"contract": stage.CATALOG_SCOPE, "ready": True, "hashes_verified": True,
        "valid_until_utc": deadline, "checked_at_utc": datetime.now(UTC).isoformat(),
        "catalog_total": 350, "general_history_total": 350, "partitioned_endpoints": [],
        "verified_source_empty": [], "source_receipts_sha256": "fixture-catalog"}
    monkeypatch.setattr(stage, "stage_source_guards", lambda *_: nullcontext())
    monkeypatch.setattr(stage, "catalog_readiness", lambda *_a, **_kw: readiness)
    target = tmp_path / "private-stage"
    argv = ["--source", str(feature), "--base", str(base), "--live-root", str(root), "--staged-root", str(target)]
    assert stage.main(argv) == 0
    release_bytes = (target / "release_receipt.json").read_bytes()
    release = json.loads(release_bytes)
    assert release["schema_version"] == (2 if partitioned else 1)
    assert release_inventory(release) == inventory_hashes(formal.identity, "features/tw_public_stock_daily.parquet")
    assert release["use_restriction"].endswith("no_public_raw_delivery")
    assert stage.main(argv) == 0
    assert (target / "release_receipt.json").read_bytes() == release_bytes
    monkeypatch.setattr(remote, "load_config", lambda *_: SimpleNamespace(data=SimpleNamespace(
        tw_public_feature_path=str(target / "features/finlab.parquet"), parquet_root=str(stocks))))
    assert remote.check(tmp_path / "unused.yaml")["status"] == "inputs_exact_hash_verified"
    if partitioned:
        member = formal.source.members[0]
        member.rename(member.with_suffix(".offline"))
        with pytest.raises((OSError, ValueError)):
            remote.check(tmp_path / "unused.yaml")
    capsys.readouterr()


def test_fixed_packed_release_reconstructs_same_lineage_and_all_values(tmp_path, monkeypatch, capsys):
    from scripts import publish_data_releases as publisher
    from stockagent.data_sync.packed_snapshots import fetch_packed_snapshot, resolve_packed_snapshot_id
    from stockagent.data.tw_public_incremental import table_content_sha256
    root, path, table = formal_source(tmp_path)
    original = ParquetLineage(path).identity
    monkeypatch.setattr(publisher, "REPO_ROOT", tmp_path)
    catalog = tmp_path / "catalog.json"
    catalog.write_text(json.dumps({"schema_version": 1, "datasets": [{"dataset": "lineage-fixture",
        "source": str(root), "role": "private offline test", "publish": True, "pack_buckets": 1,
        "loose_threshold_mib": 8, "note": "offline exact lineage roundtrip",
        "source_coordination_lock": "source/.lock"}]}))
    cold = tmp_path / "cold"
    assert publisher.main(["publish", "lineage-fixture", "--catalog", str(catalog), "--sync-root", str(cold),
                           "--node-id", "fixture-node"]) == 0
    published = json.loads(capsys.readouterr().out)["published"][0]
    fixed = resolve_packed_snapshot_id(cold, published["dataset"], published["snapshot_id"])
    restored = fetch_packed_snapshot(cold, tmp_path / "restored", fixed)
    snapshot = ParquetLineage(restored / path.relative_to(root))
    assert snapshot.identity == original
    assert table_content_sha256(snapshot.source.table()) == table_content_sha256(table)
    assert snapshot.source.path.read_bytes() == parts.manifest_path(path).read_bytes()


@pytest.mark.parametrize("kind", ["wide", "all_observed"])
@pytest.mark.parametrize("partitioned", [False, True])
def test_wide_and_all_observed_stagers_bind_exact_source_inventory(tmp_path, monkeypatch, capsys, partitioned, kind):
    import sys
    import yaml
    from scripts import stage_tw_public_research_release as wide
    from scripts import stage_tw_public_all_observed_release as all_observed
    from scripts.stage_tw_public_research_release import FORMAL_MEMBERS
    root, formal_path, table = formal_source(tmp_path, partitioned)
    identity = ParquetLineage(formal_path).identity
    for name in FORMAL_MEMBERS[1:]:
        (root / name).write_text(json.dumps({"raw_receipt_manifest": {"relative_path": "raw/actions.jsonl"}}))
    (root / "raw").mkdir()
    (root / "raw/actions.jsonl").write_text("{}\n")
    stager = wide if kind == "wide" else all_observed
    target = tmp_path / "staged"
    feature = tmp_path / "research.parquet"
    receipt = tmp_path / "research.receipt.json"
    if kind == "wide":
        selected = yaml.safe_load(wide.SELECTOR.read_text())["data"]["feature_include"]
        names = list(dict.fromkeys(selected))
        contents = {"contract_version": 6 if partitioned else 5,
            **receipt_fields(identity, hash_key="base_sha256", source_key="base_source")}
        monkeypatch.setattr(wide, "RESEARCH_RECEIPT", receipt)
    else:
        names = [f"twpub_{index}" for index in range(204)]
        auxiliary = tmp_path / "wide.parquet"
        auxiliary_receipt = tmp_path / "wide.receipt.json"
        pq.write_table(table, auxiliary)
        auxiliary_receipt.write_text("{}")
        monkeypatch.setattr(stager, "WIDE_SOURCE", auxiliary)
        monkeypatch.setattr(stager, "WIDE_RECEIPT", auxiliary_receipt)
        monkeypatch.setattr(stager, "SOURCE_RECEIPT", receipt)
        contents = {"contract_version": 3 if partitioned else 2, "inputs": {
            "official": receipt_fields(identity), "wide": {"sha256": sha256_file(auxiliary)}}}
    pq.write_table(pa.table({"date": table["date"], "symbol": table["symbol"],
                            **{name: [-0.0, None] for name in names}}), feature)
    receipt.write_text(json.dumps({**contents, "output_sha256": sha256_file(feature)}))
    monkeypatch.setattr(stager, "SOURCE", feature)
    monkeypatch.setattr(stager, "LIVE_ROOT", root)
    monkeypatch.setattr(stager, "FORMAL_FEATURE", formal_path)
    monkeypatch.setattr(stager, "LOCK", tmp_path / "locks/source.lock")
    monkeypatch.setattr(stager, "_formal_inventory_hashes", lambda *_a, required_members, **_kw: {
        member: sha256_file(root / member) for member in required_members})
    monkeypatch.setattr(sys, "argv", ["stage", "--base-snapshot-id", "fixed-fixture", "--staged-root", str(target)])
    assert stager.main() == 0
    release = json.loads((target / "release_receipt.json").read_bytes())
    assert release_inventory(release) == inventory_hashes(identity, "features/tw_public_stock_daily.parquet")
    assert set(release["formal_action_members_sha256"]) == {*FORMAL_MEMBERS[1:], "raw/actions.jsonl"}
    assert release["research_only"] and not release["historical_vintage_verified"]
    capsys.readouterr()


@pytest.mark.parametrize("missing_base_member", [False, True])
def test_cold_audit_matches_every_partition_and_exact_staged_receipt(tmp_path, monkeypatch, missing_base_member):
    import gzip
    from scripts import audit_finlab_research_cold as cold
    _, path, _ = formal_source(tmp_path)
    identity = ParquetLineage(path).identity
    expected_base = inventory_hashes(identity, "features/tw_public_stock_daily.parquet")
    staged = tmp_path / "staged"
    staged.mkdir()
    feature = staged / "feature.parquet"
    feature.write_bytes(b"explicit offline fixture")
    release = {"status": "complete", "private_personal_research": True,
        "feature_rows": 2, "model_channels": 636,
        "staged_files_sha256": {"feature.parquet": sha256_file(feature)},
        "formal_action_members_sha256": {}, "base_feature_source": identity}
    release_path = staged / "release_receipt.json"
    release_path.write_text(json.dumps(release))
    sync = tmp_path / "cold"
    sync.mkdir()
    with gzip.open(sync / "inventory.jsonl.gz", "wt") as stream:
        for member in (release_path, feature):
            stream.write(json.dumps({"kind": "file", "path": member.name, "sha256": sha256_file(member)}) + "\n")
    resolved = SimpleNamespace(manifest={"snapshot_id": "fixed-fixture", "archive": {
        "inventory": {"relpath": "inventory.jsonl.gz"}}})
    monkeypatch.setattr(cold, "resolve_latest_packed", lambda *_a, **_kw: resolved)
    monkeypatch.setattr(cold, "verify_packed_snapshot", lambda *_: {"snapshot_id": "fixed-fixture",
        "manifest_sha256": "fixture", "inventory_sha256": "fixture", "verified_object_bytes": 7})
    def base_inventory(*_args, required_members, **_kwargs):
        assert set(required_members) == set(expected_base)
        result = dict(expected_base)
        if missing_base_member:
            result.pop(next(reversed(result)))
        return result
    monkeypatch.setattr(cold, "_formal_inventory_hashes", base_inventory)
    assert cold.audit(sync, staged)["base_cold_feature_match"] is not missing_base_member
    release_path.write_text(json.dumps({**release, "unpublished_change": True}))
    with pytest.raises(RuntimeError, match="staged receipt"):
        cold.audit(sync, staged)

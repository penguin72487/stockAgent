from __future__ import annotations

from datetime import UTC, datetime, timedelta
import fcntl
import hashlib
import json
import os
from pathlib import Path

import pytest

from scripts.finlab_release_gate import CONTRACT, catalog_readiness
from stockagent.data.finlab_acquisition_contract import read_verified_empty_evidence, safe_stem


def _write(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def _catalog(root: Path, now: datetime) -> dict:
    keys = ["monthly:a", "monthly:b", "events:empty", "tw_minute:2330", "tw_tick:2330"]
    _write(root / "catalog/discovery.json", {"keys": keys, "observed_at_utc": now.isoformat()})
    payload = root / "datasets/a.parquet"
    payload.parent.mkdir()
    payload.write_bytes(b"original signed values: -7, NULL, 2.5")
    alias = payload.with_name("b.parquet")
    os.link(payload, alias)
    for key, path in zip(keys[:2], (payload, alias)):
        _write(root / "receipts" / f"{safe_stem(key)}.json", {
            "dataset": key, "status": "downloaded_unverified_for_pit",
            "parquet_path": str(path.relative_to(root)),
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "source_check_mode": "upstream_incremental",
            "source_checked_at_utc": (now - timedelta(days=2)).isoformat(),
            "next_source_check_at_utc": (now + timedelta(days=1)).isoformat(),
        })
    raw = root / "empty_evidence/source.feather"
    raw.parent.mkdir()
    raw.write_bytes(b"retained original all-null source")
    _write(root / "empty_evidence/proof.json", {
        "dataset": keys[2], "status": "source_all_null", "usable_observations": False,
        "raw_non_null_values": 0, "raw_path": "empty_evidence/source.feather",
        "raw_sha256": hashlib.sha256(raw.read_bytes()).hexdigest(),
        "observed_at_utc": (now - timedelta(minutes=1)).isoformat(),
    })
    attempt = {"dataset": keys[2], "status": "provider_empty", "raw_non_null_values": 0,
               "empty_evidence_path": "empty_evidence/proof.json", "failure_streak": 1,
               "attempted_at_utc": (now - timedelta(minutes=1)).isoformat()}
    _write(root / "attempts" / f"{safe_stem(keys[2])}.json", attempt)
    return attempt


def test_general_catalog_uses_native_expiry_and_explicit_partition_scope(tmp_path):
    now = datetime.now(UTC)
    _catalog(tmp_path, now)
    result = catalog_readiness(tmp_path, now=now, verify_hashes=True)
    assert result["contract"] == CONTRACT
    assert result["ready"] and result["hashes_verified"]
    assert result["catalog_total"] == 5 and result["general_history_total"] == 3
    assert result["verified"] == 3 and result["missing"] == result["stale"] == result["invalid"] == 0
    assert result["partitioned_endpoints"] == ["tw_minute:2330", "tw_tick:2330"]
    assert result["partitioned_history_verified_by_this_gate"] is False
    assert result["verified_source_empty"] == ["events:empty"]
    assert result["physical_files_hashed"] == 1
    assert result["hash_bytes_read"] == (tmp_path / "datasets/a.parquet").stat().st_size
    assert datetime.fromisoformat(result["valid_until_utc"]) == now + timedelta(minutes=29)
    expired = catalog_readiness(tmp_path, now=now + timedelta(minutes=30))
    assert expired["missing"] == 1 and not expired["ready"]


def test_alias_mutation_is_detected_for_every_logical_key(tmp_path, monkeypatch):
    from stockagent.data_sync import immutable_cache_files
    now = datetime.now(UTC)
    _catalog(tmp_path, now)
    original = immutable_cache_files.stable_payload_hash

    def mutate(path, before):
        digest = original(path, before)
        path.write_bytes(b"modified during verification")
        return digest

    monkeypatch.setattr(immutable_cache_files, "stable_payload_hash", mutate)
    result = catalog_readiness(tmp_path, now=now, verify_hashes=True)
    assert not result["ready"] and not result["hashes_verified"]
    assert result["invalid"] == 2 and result["verified"] == 1


@pytest.mark.parametrize("replacement", ["symlink", "fifo", "list", "raw_changed"])
def test_source_empty_requires_regular_stable_backing_evidence(tmp_path, replacement):
    now = datetime.now(UTC)
    attempt = _catalog(tmp_path, now)
    proof = tmp_path / "empty_evidence/proof.json"
    if replacement == "symlink":
        moved = proof.with_suffix(".saved")
        proof.rename(moved)
        proof.symlink_to(moved.name)
    elif replacement == "fifo":
        proof.unlink()
        os.mkfifo(proof)
    elif replacement == "list":
        proof.write_text("[]")
    else:
        (tmp_path / "empty_evidence/source.feather").write_bytes(b"not the observed source")
    assert read_verified_empty_evidence("events:empty", tmp_path, attempt) is None
    result = catalog_readiness(tmp_path, now=now)
    assert result["missing"] == 1 and not result["ready"]


def test_source_check_due_still_blocks_release(tmp_path):
    now = datetime.now(UTC)
    _catalog(tmp_path, now)
    receipt_path = tmp_path / "receipts" / f"{safe_stem('monthly:a')}.json"
    receipt = json.loads(receipt_path.read_text())
    receipt["next_source_check_at_utc"] = now.isoformat()
    _write(receipt_path, receipt)
    result = catalog_readiness(tmp_path, now=now)
    assert result["stale"] == 1 and not result["ready"]


def test_stage_refreshes_only_expired_full_hash_completion(tmp_path):
    from scripts.stage_tw_public_finlab_research_release import (
        catalog_completion_fields, refresh_unchanged_completion,
    )
    now = datetime.now(UTC)
    _catalog(tmp_path, now)
    readiness = catalog_readiness(tmp_path, now=now, verify_hashes=True)
    previous = {"status": "complete", "staged_files_sha256": {"feature": "fixed-sha"}}
    receipt = tmp_path / "release_receipt.json"
    _write(receipt, previous)
    assert refresh_unchanged_completion(receipt, previous, readiness)
    updated = json.loads(receipt.read_text())
    assert updated["catalog_latest_verified"] and updated["source_hashes_verified"]
    assert updated["staged_files_sha256"] == previous["staged_files_sha256"]
    assert updated["catalog_partitioned_history_verified"] is False
    stamp = receipt.stat().st_mtime_ns
    assert not refresh_unchanged_completion(receipt, updated, readiness)
    assert receipt.stat().st_mtime_ns == stamp
    with pytest.raises(RuntimeError, match="hash evidence"):
        catalog_completion_fields({**readiness, "hashes_verified": False})
    with pytest.raises(RuntimeError, match="elapsed"):
        catalog_completion_fields({**readiness, "valid_until_utc": now.isoformat()})


def test_finlab_stage_coexists_with_formal_reader_but_excludes_real_writers(tmp_path):
    from scripts.stage_tw_public_finlab_research_release import stage_source_guards
    from stockagent.data.finlab_acquisition_contract import FINLAB_RESEARCH_STAGE_LOCK
    from stockagent.live.tw_public_source_guard import TWPublicSourceUpdatePending
    public = tmp_path / "live/data_tw_public"
    public.mkdir(parents=True)
    (public / "twse_daily_ohlcv.parquet").touch()
    public_lock = public.parent / ".locks/tw-public-refresh.lock"
    public_lock.parent.mkdir()
    staged = public.with_name("data_tw_public_research_finlab_2014_v4")
    with public_lock.open("a+") as reader:
        fcntl.flock(reader, fcntl.LOCK_SH | fcntl.LOCK_NB)
        with stage_source_guards(staged, public):
            with (public.parent / FINLAB_RESEARCH_STAGE_LOCK).open("a+") as writer:
                with pytest.raises(BlockingIOError):
                    fcntl.flock(writer, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(reader, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(TWPublicSourceUpdatePending):
            with stage_source_guards(staged, public):
                pytest.fail("formal writer lease was bypassed")


def test_canonical_base_refresh_reuses_original_builder_and_rejects_custom_inputs(tmp_path, monkeypatch):
    from scripts import build_finlab_research_overlay as builder
    calls = []
    monkeypatch.setattr(builder.subprocess, "run", lambda *a, **kw: calls.append((a, kw)))
    builder.refresh_canonical_base(builder.CANONICAL_BASE)
    assert len(calls) == 1 and calls[0][1]["check"] is True
    command = calls[0][0][0]
    assert command[1] == str(builder.ROOT / "scripts/build_tw_public_research_all_features.py")
    assert command[command.index("--output-path") + 1] == str(builder.CANONICAL_BASE)
    with pytest.raises(ValueError, match="custom research input"):
        builder.refresh_canonical_base(tmp_path / "custom.parquet")
    assert len(calls) == 1

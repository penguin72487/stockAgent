"""Independent, bounded native readback never invents completion or candles."""
from datetime import UTC, datetime, timedelta
import json
from types import SimpleNamespace

import polars as pl
import pytest

from downloader.ohlcv_hot_tail import hot_tail_path
from scripts import verify_recovered_bybit_runtime as proof


def frame(dates):
    count = len(dates)
    # Provider persisted ABI is a UTC, zone-free timestamp string, not ISO +00:00.
    return pl.DataFrame({"date": [day.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S") for day in dates], "open": [10.0]*count,
        "max": [11.0]*count, "min": [9.0]*count, "close": [10.5]*count,
        "Trading_Volume": [0.0]*count})


def setup(tmp_path, bad=None):
    now = datetime.now(UTC).replace(second=0, microsecond=0)
    base = tmp_path / "SAMPLE_features.parquet"
    tail = hot_tail_path(base)
    tail.parent.mkdir()
    frame([now-timedelta(minutes=4)]).write_parquet(base)
    fresh = frame([now-timedelta(minutes=3), now-timedelta(minutes=2)])
    if bad == "future":
        fresh = frame([now-timedelta(minutes=1), now])
    elif bad == "duplicate":
        fresh = frame([now-timedelta(minutes=2)]*2)
    elif bad == "ohlc":
        fresh = fresh.with_columns(pl.lit(99.0).alias("min"))
    elif bad == "null":
        fresh = fresh.with_columns(pl.lit(None, dtype=pl.Float64).alias("close"))
    elif bad == "nan":
        fresh = fresh.with_columns(pl.lit(float("nan")).alias("close"))
    elif bad == "volume":
        fresh = fresh.with_columns(pl.lit(-1.0).alias("Trading_Volume"))
    fresh.write_parquet(tail)
    return base, now


def test_real_closed_candles_and_zero_volume_remain_usable(tmp_path):
    base, now = setup(tmp_path)
    result = proof.verify_increment(base, finished_at=now-timedelta(days=1), now=now)
    assert result["accepted"]
    assert all(result["checks"].values())
    assert result["tail_rows"] == 2
    assert result["logical_rows"] == 3


@pytest.mark.parametrize("bad", ("future", "duplicate", "ohlc", "null", "nan", "volume"))
def test_invalid_or_unclosed_source_is_never_promoted_to_complete(tmp_path, bad):
    base, now = setup(tmp_path, bad)
    with pytest.raises(ValueError, match="readback failed"):
        proof.verify_increment(base, finished_at=now-timedelta(days=1), now=now)


def test_old_tail_is_not_evidence_of_a_new_native_increment(tmp_path):
    base, now = setup(tmp_path)
    with pytest.raises(ValueError, match="fresh"):
        proof.verify_increment(base, finished_at=now+timedelta(days=1), now=now)


def test_identity_change_during_readback_is_rejected(tmp_path, monkeypatch):
    base, now = setup(tmp_path)
    identity = proof.source_identity
    calls = [0]
    def changing(path):
        calls[0] += 1
        return (*identity(path), calls[0] > 2)
    monkeypatch.setattr(proof, "source_identity", changing)
    with pytest.raises(ValueError, match="generation changed"):
        proof.verify_increment(base, finished_at=now-timedelta(days=1), now=now)


@pytest.mark.parametrize("live,require_live", ((True, False), (False, False), (False, True)))
def test_complete_provenance_result_uses_canonical_manifest_id_not_a_fake_attribute(
        tmp_path, monkeypatch, live, require_live):
    live_root = tmp_path / "live"
    destination = live_root / "data_bybit"
    minute = destination / "1m"
    minute.mkdir(parents=True)
    now = datetime.now(UTC).replace(second=0, microsecond=0)
    entries = []
    for symbol in ("A", "B", "C"):
        base = minute / f"{symbol}_features.parquet"
        frame([now-timedelta(minutes=4)]).write_parquet(base)
        tail = hot_tail_path(base)
        tail.parent.mkdir(exist_ok=True)
        frame([now-timedelta(minutes=3), now-timedelta(minutes=2)]).write_parquet(tail)
        entries.append({"path": str(base.relative_to(destination)), "kind": "file",
                        "size": base.stat().st_size, "sha256": proof.file_sha(base)})
    source = tmp_path / "data_bybit"
    source.symlink_to(destination, target_is_directory=True)
    counts = {"files": 3, "logical_bytes": sum(row["size"] for row in entries), "directories": 0, "symlinks": 0}
    manifest = {"snapshot_id": "original-fixed", "source": counts}
    resolved = SimpleNamespace(manifest=manifest, manifest_sha256="a"*64)
    receipt = {"state": "restored_mutable_workspace", "dataset": "bybit", "full_canonical_recovery_verified": True,
        "workspace_installed": True, "provider_calls": 0, "cold_objects_modified": False,
        "destination": str(destination), "new_link": proof.link_identity(source), "snapshot_id": "original-fixed",
        "manifest_sha256": "a"*64, "source_inventory": counts, "recovered_files": 3,
        "recovered_logical_bytes": counts["logical_bytes"], "finished_at_utc": (now-timedelta(days=1)).isoformat()}
    path = tmp_path / "recovery.json"
    path.write_text(json.dumps(receipt))
    monkeypatch.setattr(proof, "ROOT", tmp_path)
    monkeypatch.setattr(proof, "LIVE_ROOT", live_root)
    monkeypatch.setattr(proof, "resolve_packed_snapshot_id", lambda *a: resolved)
    monkeypatch.setattr(proof, "_load_inventory", lambda *a: entries)
    monkeypatch.setattr(proof, "_validate_inventory", lambda *a: {"counts": counts})
    monkeypatch.setattr(proof, "unit_identity", lambda *a: {
        "ActiveState": "activating" if live else "failed", "ControlGroup": "formal" if live else ""})
    monkeypatch.setattr(proof, "native_bybit_owner", lambda *a: {"owner": {"pid": 123}})
    if require_live and not live:
        with pytest.raises(ValueError, match="not live"):
            proof.verify(path, require_live_owner=True)
        return
    result = proof.verify(path, require_live_owner=require_live)
    assert result["snapshot_id"] == "original-fixed"
    assert result["state"] == ("accepted_fixed_recovery_and_native_increment" if live else
                               "accepted_source_readback_native_owner_pending")
    assert result["native_owner_accepted"] == live
    assert not result["native_lifecycle_complete_claim"]
    assert len(result["native_increment_readbacks"]) == 3
    assert result["all_original_paths_present"]
    assert not result["all_mutable_file_content_reverified"]
    assert not result["full_history_complete_claim"]

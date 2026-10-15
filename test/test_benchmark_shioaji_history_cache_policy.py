from datetime import UTC, date, datetime
import json
import os
import shutil

import polars as pl
import pytest

from downloader.artifact_io import atomic_write_json, sha256_file
from scripts import benchmark_shioaji_history_cache_policy as probe


@pytest.mark.parametrize("policy", tuple(probe.POLICIES))
def test_isolated_policy_preserves_signatures_payloads_and_restores_decorators(tmp_path, monkeypatch, policy):
    collector, repair = probe.audit.current, probe.audit.repair
    original_receipts, original_hashes = collector._valid_receipt_at_signature, repair._signature_sha
    partition = tmp_path / "start=2026-10-13_end=2026-10-13"
    partition.mkdir()
    receipt, data = partition / "receipt.json", partition / "data.parquet"
    data.write_bytes(b"original bytes")
    payload = {
        "schema_version": collector.RECEIPT_SCHEMA_VERSION, "source": collector.SOURCE,
        "method": "kbars", "contract": "TXFJ6", "status": "complete",
        "start": "2026-10-13", "end": "2026-10-13", "sha256": sha256_file(data),
    }
    atomic_write_json(receipt, payload)

    def exercise_real_validators(fixture, variant):
        assert fixture == tmp_path and variant == "candidate"
        sizes = probe.POLICIES[policy]
        assert collector._valid_receipt_at_signature.cache_info().maxsize == sizes[0]
        assert repair._signature_sha.cache_info().maxsize == sizes[1]
        assert collector._valid_receipt(receipt, data, method="kbars", code="TXFJ6") == payload
        assert collector._valid_receipt(receipt, data, method="kbars", code="TXFJ6") == payload
        assert collector._valid_receipt_at_signature.cache_info().hits == 1
        # Calling SHA independently must reuse only the unchanged file identity.
        assert repair.verified_sha(data) == payload["sha256"]
        assert repair._signature_sha.cache_info().hits == 1
        returned = collector._valid_receipt(receipt, data, method="kbars", code="TXFJ6")
        returned["status"] = "modified by consumer"
        assert collector._valid_receipt(receipt, data, method="kbars", code="TXFJ6") == payload

        # Same-size in-place replacement with restored mtime must not reuse SHA:
        # ctime remains part of the canonical five-field identity.
        stamp = data.stat()
        data.write_bytes(b"replaced bytes")
        os.utime(data, ns=(stamp.st_atime_ns, stamp.st_mtime_ns))
        assert data.stat().st_size == stamp.st_size
        assert collector._valid_receipt(receipt, data, method="kbars", code="TXFJ6") is None
        updated = {**payload, "sha256": sha256_file(data)}
        atomic_write_json(receipt, updated)
        assert collector._valid_receipt(receipt, data, method="kbars", code="TXFJ6") == updated
        return {"receipt_cache": collector._valid_receipt_at_signature.cache_info()._asdict()}

    monkeypatch.setattr(probe.audit, "worker", exercise_real_validators)
    result = probe.worker(tmp_path, policy)
    assert result["policy"] == policy
    assert result["memo_capacity"] == dict(zip(("receipts", "hashes"), probe.POLICIES[policy], strict=True))
    assert result["hash_cache"]["currsize"] == 2
    assert collector._valid_receipt_at_signature is original_receipts
    assert repair._signature_sha is original_hashes


def test_failed_isolated_worker_restores_both_decorators(tmp_path, monkeypatch):
    original_receipts = probe.audit.current._valid_receipt_at_signature
    original_hashes = probe.audit.repair._signature_sha

    def fail(*_args):
        raise RuntimeError("preserve failure and release isolated memo policy")

    monkeypatch.setattr(probe.audit, "worker", fail)
    with pytest.raises(RuntimeError, match="preserve failure"):
        probe.worker(tmp_path, "compact")
    assert probe.audit.current._valid_receipt_at_signature is original_receipts
    assert probe.audit.repair._signature_sha is original_hashes


def test_real_full_local_audit_matches_for_both_memo_policies(tmp_path):
    collector = probe.audit.current
    archive = tmp_path / "archive"
    day = date(2026, 10, 13)
    contract = collector.HistoryContract(
        collection="exact_futures", priority=2, security_type="FUT", asset_class="futures",
        code="TXFJ6", root="TXF", name="TX", exchange="TAIFEX", begin_date=day,
        end_date=day, delivery_date=date(2026, 10, 21), delivery_month="202610",
    )
    collector._write_inventory(archive, [contract], completed_session=day)
    _, receipt = collector._kbar_paths(archive, contract, day, day)
    atomic_write_json(receipt, {
        "schema_version": collector.RECEIPT_SCHEMA_VERSION, "source": collector.SOURCE,
        "method": "kbars", "contract": contract.code, "status": "source_empty",
        "start": str(day), "end": str(day), "observed_trading_dates": [], "rows": 0, "size": 0,
    })
    pl.DataFrame({
        "date": [day], "product": ["TX"], "contract": ["202610"],
        "resolved_last_trade_date": [contract.delivery_date], "shioaji_roots": ["TXF"], "volume": [1],
    }).write_parquet(tmp_path / "activity.parquet")
    shutil.copyfile(probe.audit.COLLECTOR, tmp_path / "reference.py")
    atomic_write_json(tmp_path / "manifest.json", {
        "freeze_at_utc": datetime(2026, 10, 14, tzinfo=UTC).isoformat(),
    })
    results = [probe.worker(tmp_path, policy) for policy in probe.POLICIES]
    assert len({row["task_sha256"] for row in results}) == 1
    assert len({tuple(row["summary_sha256"]) for row in results}) == 1
    assert results[0]["final_summary"] == results[1]["final_summary"]
    assert all(row["pending_queries"] == 2 and row["provider_requests"] == 0 for row in results)
    assert json.loads((archive / "summary.json").read_bytes()) == results[0]["final_summary"]


@pytest.mark.parametrize("state", ["failed", "running", "accepted_isolated_cache_policy_only"])
def test_probe_rejects_unaccepted_full_audit_before_running_trials(tmp_path, monkeypatch, state):
    monkeypatch.setattr(probe.audit, "bounded_new_path", lambda path: path)
    reference = tmp_path / "control.json"
    manifest = {"contract": "actual full audit fixture"}
    atomic_write_json(tmp_path / "manifest.json", manifest)
    atomic_write_json(reference, {"fixture": manifest, "state": state})
    monkeypatch.setattr(probe.subprocess, "run", lambda *_args, **_kwargs: pytest.fail("unaccepted work must not run"))
    with pytest.raises(ValueError, match="exact accepted real workload"):
        probe.compare(tmp_path, reference, tmp_path / "result.json")
    assert not (tmp_path / "result.json").exists()

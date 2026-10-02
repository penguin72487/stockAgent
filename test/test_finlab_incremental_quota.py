"""Quota-free local admission is only for verified SDK source refreshes."""
from datetime import UTC, datetime
import hashlib
import json
from unittest.mock import Mock

import pandas as pd
import pytest

from scripts import download_finlab_history as history
from stockagent.data.finlab_acquisition_contract import WHOLE_TABLE_KEYS, incremental_quota_exempt


def baseline(root, key):
    relative = f"datasets/{history.safe_stem(key)}.parquet"
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"source_index": ["2026-09-30"], "2330": [0.0]}).to_parquet(path, index=False)
    receipt = {"dataset": key, "status": "downloaded_unverified_for_pit", "rows": 1,
               "rows_with_values": 1, "parquet_path": relative,
               "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
               "parquet_size_bytes": path.stat().st_size,
               "source_checked_at_utc": "2026-09-30T10:00:00+00:00", "source_check_mode": "upstream_forced"}
    receipts = root / "receipts"
    receipts.mkdir(exist_ok=True)
    (receipts / f"{history.safe_stem(key)}.json").write_text(json.dumps(receipt))
    return path


def run_sync(monkeypatch, root, pending, *, room=(30.0, 5000.0), limit=1, error=None):
    monkeypatch.setattr(history, "credential_available", lambda: True)
    monkeypatch.setattr(history, "sync_selection", lambda *args, **kwargs: pending)
    monkeypatch.setattr(history, "record_core_acquisition_status", lambda *args, **kwargs:
                        {"required_complete": False, "required_pending": len(pending), "required_blocked": 0})
    quota = Mock(return_value=room)
    fetch = Mock(side_effect=error, return_value={"last_check_result": "unchanged", "rows_with_values": 1})
    monkeypatch.setattr(history, "quota_room_mb", quota)
    monkeypatch.setattr(history, "fetch_one", fetch)
    result = history.sync_catalog(pending, {}, root, limit=limit, refresh_days=1,
                                  min_quota_remaining_mb=50.0, retry_unavailable=False)
    return result, fetch, quota


@pytest.mark.parametrize("room", [(30.0, 5000.0), (0.0, 5000.0), None])
def test_verified_refresh_uses_sdk_even_below_local_reserve_or_missing_sample(tmp_path, monkeypatch, room):
    key = "price:收盤價"
    path = baseline(tmp_path, key)
    before = path.read_bytes()
    result, fetch, quota = run_sync(monkeypatch, tmp_path, [key], room=room)
    fetch.assert_called_once_with(key, tmp_path, refresh=True, incremental=True)
    quota.assert_not_called()  # official usage remains with the separate minute worker
    assert result["state"] == "pass_complete"
    assert result["incremental_quota_exempt_attempted"] == 1
    assert result["quota_managed_deferred"] == 0
    assert path.read_bytes() == before


def test_idle_sweep_does_not_request_account_quota(tmp_path, monkeypatch):
    result, fetch, quota = run_sync(monkeypatch, tmp_path, [])
    assert result["state"] == "pass_complete" and result["attempted"] == 0
    fetch.assert_not_called()
    quota.assert_not_called()


@pytest.mark.parametrize("corrupt", [False, True])
def test_missing_or_corrupt_history_cannot_claim_incremental_exemption(tmp_path, monkeypatch, corrupt):
    key = "price:收盤價"
    if corrupt:
        baseline(tmp_path, key).write_bytes(b"corrupt")
    result, fetch, _ = run_sync(monkeypatch, tmp_path, [key])
    fetch.assert_not_called()
    assert result["state"] == "quota_margin_reached"
    assert result["incremental_quota_exempt_attempted"] == 0
    assert result["quota_managed_deferred"] == 1


@pytest.mark.parametrize("key", sorted(WHOLE_TABLE_KEYS))
def test_whole_object_adapters_retain_reserve_even_with_old_data(tmp_path, monkeypatch, key):
    baseline(tmp_path, key)
    result, fetch, _ = run_sync(monkeypatch, tmp_path, [key])
    fetch.assert_not_called()
    assert result["state"] == "quota_margin_reached"
    assert not incremental_quota_exempt(key, downloaded=True)


def test_blocked_history_prefix_does_not_consume_actual_attempt_limit(tmp_path, monkeypatch):
    key = "financial_statement:EPS"
    baseline(tmp_path, key)
    result, fetch, _ = run_sync(monkeypatch, tmp_path, ["missing:history", key])
    fetch.assert_called_once_with(key, tmp_path, refresh=True, incremental=True)
    assert result["attempted"] == 1 and result["quota_managed_deferred"] == 1
    assert result["state"] == "quota_margin_reached"
    assert result["required_complete"] is False


def test_unknown_backfill_quota_does_not_block_later_verified_refresh(tmp_path, monkeypatch):
    key = "price:收盤價"
    baseline(tmp_path, key)
    result, fetch, _ = run_sync(monkeypatch, tmp_path, ["missing:history", key], room=None)
    fetch.assert_called_once()
    assert result["state"] == "quota_unknown"
    assert result["incremental_quota_exempt_attempted"] == 1


@pytest.mark.parametrize("message,state", [("quota exceeded; token=SECRET", "quota_exhausted"),
                                         ("unauthorized session", "authentication_failed")])
def test_actual_provider_denial_stops_sweep_and_remains_redacted(tmp_path, monkeypatch, message, state):
    keys = ["price:a", "price:b"]
    for key in keys:
        baseline(tmp_path, key)
    result, fetch, _ = run_sync(monkeypatch, tmp_path, keys, limit=2, error=RuntimeError(message))
    fetch.assert_called_once()
    assert result["state"] == state
    attempt = json.loads((tmp_path / "attempts" / f"{history.safe_stem(keys[0])}.json").read_text())
    assert attempt["status"] == state and "SECRET" not in json.dumps(attempt)


def test_unexpired_release_is_not_added_just_because_quota_is_exempt(tmp_path):
    key = "price:收盤價"
    baseline(tmp_path, key)
    path = tmp_path / "receipts" / f"{history.safe_stem(key)}.json"
    receipt = json.loads(path.read_text())
    receipt.update(source_checked_at_utc="2026-10-02T07:00:00+00:00",
                   source_check_mode="upstream_incremental",
                   next_source_check_at_utc="2026-10-02T10:00:00+00:00")
    path.write_text(json.dumps(receipt))
    assert history.sync_selection([key], {}, tmp_path, now=datetime(2026, 10, 2, 8, tzinfo=UTC),
                                  refresh_days=1, retry_unavailable=False) == []

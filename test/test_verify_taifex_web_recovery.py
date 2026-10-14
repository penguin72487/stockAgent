from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
from pathlib import Path

import pytest

from scripts.verify_taifex_web_recovery import prefix_hash, validate_history_clock


def _clock_fixture():
    now = datetime(2026, 10, 13, 14, 30, tzinfo=timezone.utc)
    ns = int((now - timedelta(seconds=60)).timestamp() * 1e9)
    status = {"source_updated_at_utc": now.isoformat(), "strategies": [{"strategy_id": "a"}, {"strategy_id": "b"}]}
    history = {"anchor_at_utc": (now - timedelta(seconds=60)).isoformat(),
               "coverage_end_utc": (now - timedelta(seconds=60)).isoformat(),
               "history": [{"strategy_id": "a", "decision_ts_ns": ns},
                           {"strategy_id": "b", "decision_ts_ns": ns}]}
    return status, history


def test_prefix_hash_verifies_original_bytes_not_new_appends(tmp_path: Path) -> None:
    path = tmp_path / "ledger"
    path.write_bytes(b"original\nnew\n")
    assert prefix_hash(path, 9) == hashlib.sha256(b"original\n").hexdigest()
    with pytest.raises(ValueError, match="truncated"):
        prefix_hash(path, path.stat().st_size + 1)


def test_history_acceptance_requires_current_endpoints_for_every_strategy() -> None:
    status, history = _clock_fixture()
    assert validate_history_clock(status, history) == 60.0
    history["history"][-1]["decision_ts_ns"] -= 24 * 3600 * 1_000_000_000
    with pytest.raises(ValueError, match="current strategy endpoint"):
        validate_history_clock(status, history)


def test_history_acceptance_rejects_old_curve_despite_healthy_status() -> None:
    status, history = _clock_fixture()
    history["coverage_end_utc"] = "2026-09-18T05:45:00+00:00"
    with pytest.raises(ValueError, match="caught up"):
        validate_history_clock(status, history)


def test_history_acceptance_rejects_noncausal_anchor_or_naive_clock() -> None:
    status, history = _clock_fixture()
    history["anchor_at_utc"] = "2026-10-13T14:00:00+00:00"
    with pytest.raises(ValueError, match="caught up"):
        validate_history_clock(status, history)
    status, history = _clock_fixture()
    status["source_updated_at_utc"] = "2026-10-13T14:30:00"
    with pytest.raises(ValueError, match="timezone"):
        validate_history_clock(status, history)


def test_history_acceptance_rejects_duplicate_out_of_order_endpoints() -> None:
    status, history = _clock_fixture()
    history["history"].append({"strategy_id": "a", "decision_ts_ns": 1})
    with pytest.raises(ValueError, match="monotonic"):
        validate_history_clock(status, history)

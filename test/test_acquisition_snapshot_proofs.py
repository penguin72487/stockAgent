from datetime import UTC, datetime, timedelta
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from downloader.artifact_io import sha256_file
from downloader import acquisition_snapshot_proofs as proof

NOW = datetime(2026, 9, 26, 16, 50, tzinfo=UTC)  # 00:50 Taipei
CALENDAR_ID = "finmind:TaiwanStockTradingDate"
MASTER_ID = "finmind:TaiwanStockInfoWithWarrant"


def _calendar(root: Path, **changes) -> Path:
    path = root / "calendar.json"
    path.write_text(json.dumps({
        "schema_version": 1, "source_dataset": proof.CALENDAR,
        "observed_at_utc": (NOW - timedelta(hours=1)).isoformat(),
        "dates": ["2005-01-03", "2026-09-24", "2026-12-31"], **changes,
    }))
    return path


def _master(root: Path, *, observed: datetime = NOW - timedelta(hours=10), **changes) -> tuple[Path, Path]:
    day = observed.astimezone(proof.TAIPEI).date()
    relative = f"snapshots/{proof.MASTER}/snapshot={day}-full.parquet"
    source = root / relative
    source.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({"stock_id": ["2330", "1234"]}), source)
    receipt = root / "receipts" / proof.MASTER / f"{day}.json"
    receipt.parent.mkdir(parents=True, exist_ok=True)
    receipt.write_text(json.dumps({
        "schema_version": 1, "dataset": proof.MASTER, "status": "complete",
        "query_scope": "full_table_snapshot", "snapshot_date_taipei": str(day),
        "fetched_at_utc": observed.isoformat(), "parquet_path": relative,
        "parquet_size_bytes": source.stat().st_size, "sha256": sha256_file(source),
        "rows": 2, **changes,
    }))
    return receipt, source


def test_unknown_endpoint_is_not_a_snapshot_proof(tmp_path):
    assert proof.finmind_snapshot_ready(tmp_path, "finmind:TaiwanStockPrice", NOW) is None
    assert proof.finmind_snapshot_ready(tmp_path, CALENDAR_ID, NOW) is False
    assert proof.finmind_snapshot_ready(tmp_path, MASTER_ID, NOW) is False


def test_calendar_current_and_future_planned_dates_are_allowed(tmp_path):
    _calendar(tmp_path)
    assert proof.finmind_snapshot_ready(tmp_path, CALENDAR_ID, NOW) is True


@pytest.mark.parametrize("dates", [
    [], ["2026-09-24", "2026-09-24"], ["2026-09-24", "2005-01-03"],
    ["2026-09-24T00:00:00"], ["20260924"], ["2026-02-30"], [None], ["2004-01-01"],
])
def test_calendar_rejects_bad_individual_dates(tmp_path, dates):
    _calendar(tmp_path, dates=dates)
    assert proof.finmind_snapshot_ready(tmp_path, CALENDAR_ID, NOW) is False


@pytest.mark.parametrize("stamp", [
    (NOW + timedelta(seconds=1)).isoformat(), (NOW - timedelta(hours=20)).isoformat(),
    NOW.replace(tzinfo=None).isoformat(), "not-a-time",
])
def test_calendar_rejects_future_stale_naive_observation(tmp_path, stamp):
    _calendar(tmp_path, observed_at_utc=stamp)
    assert proof.finmind_snapshot_ready(tmp_path, CALENDAR_ID, NOW) is False


def test_calendar_source_identity_and_aware_clock_are_required(tmp_path):
    _calendar(tmp_path, source_dataset="Other")
    assert proof.finmind_snapshot_ready(tmp_path, CALENDAR_ID, NOW) is False
    _calendar(tmp_path)
    assert proof.finmind_snapshot_ready(tmp_path, CALENDAR_ID, NOW.replace(tzinfo=None)) is False


def test_master_verifies_whole_snapshot_and_does_not_read_rows(tmp_path, monkeypatch):
    _master(tmp_path)
    monkeypatch.setattr(pq, "read_table", lambda *a, **k: (_ for _ in ()).throw(AssertionError("decoded rows")))
    assert proof.finmind_snapshot_ready(tmp_path, MASTER_ID, NOW) is True


@pytest.mark.parametrize("changes", [
    {"rows": 3}, {"rows": True}, {"parquet_size_bytes": 0}, {"status": "failed"},
    {"query_scope": "start_date_limited"}, {"dataset": "Other"},
    {"sha256": "0" * 64}, {"sha256": None},
    {"snapshot_date_taipei": "2026-09-27"}, {"parquet_path": "../outside.parquet"},
    {"fetched_at_utc": (NOW + timedelta(seconds=1)).isoformat()},
])
def test_master_rejects_broken_identity_values_and_hash(tmp_path, changes):
    _master(tmp_path, **changes)
    assert proof.finmind_snapshot_ready(tmp_path, MASTER_ID, NOW) is False


def test_master_follows_daily_boundary_not_calendar_twenty_hour_timer(tmp_path):
    observed = datetime(2026, 9, 26, 6, 0, tzinfo=UTC)  # yesterday 14:00
    _master(tmp_path, observed=observed)
    before = datetime(2026, 9, 27, 5, 59, tzinfo=UTC)  # today 13:59
    assert proof.finmind_snapshot_ready(tmp_path, MASTER_ID, before) is True
    assert proof.finmind_snapshot_ready(tmp_path, MASTER_ID, before + timedelta(minutes=1)) is False


def test_master_cannot_reuse_two_days_old_snapshot(tmp_path):
    _master(tmp_path, observed=NOW - timedelta(days=2))
    assert proof.finmind_snapshot_ready(tmp_path, MASTER_ID, NOW) is False


def test_master_cache_is_invalidated_by_same_size_corruption(tmp_path, monkeypatch):
    monkeypatch.setattr(proof.time, "monotonic", lambda: 100.0)
    _, source = _master(tmp_path)
    assert proof.finmind_snapshot_ready(tmp_path, MASTER_ID, NOW) is True
    original = proof.sha256_file
    calls = []
    def count(path):
        calls.append(path)
        return original(path)
    monkeypatch.setattr(proof, "sha256_file", count)
    assert proof.finmind_snapshot_ready(tmp_path, MASTER_ID, NOW) is True
    assert not calls
    raw = source.read_bytes()
    source.write_bytes(bytes([raw[0] ^ 1]) + raw[1:])
    assert proof.finmind_snapshot_ready(tmp_path, MASTER_ID, NOW) is False
    assert len(calls) == 1


def test_master_cache_is_invalidated_by_receipt_change(tmp_path):
    receipt, _ = _master(tmp_path)
    assert proof.finmind_snapshot_ready(tmp_path, MASTER_ID, NOW) is True
    data = json.loads(receipt.read_text())
    data["rows"] = 3
    receipt.write_text(json.dumps(data))
    assert proof.finmind_snapshot_ready(tmp_path, MASTER_ID, NOW) is False


def test_master_does_not_hide_failed_newer_receipt(tmp_path):
    _master(tmp_path)
    _master(tmp_path, observed=NOW - timedelta(minutes=20), status="failed")
    assert proof.finmind_snapshot_ready(tmp_path, MASTER_ID, NOW) is False


def test_master_rejects_canonical_path_symlink_escape(tmp_path):
    _, source = _master(tmp_path)
    outside = tmp_path.parent / f"{tmp_path.name}-outside.parquet"
    outside.write_bytes(source.read_bytes())
    source.unlink()
    source.symlink_to(outside)
    assert proof.finmind_snapshot_ready(tmp_path, MASTER_ID, NOW) is False


def test_master_rejects_file_replacement_during_hash(tmp_path, monkeypatch):
    _, source = _master(tmp_path)
    original = proof.sha256_file
    def changing(path):
        value = original(path)
        source.touch()
        return value
    monkeypatch.setattr(proof, "sha256_file", changing)
    assert proof.finmind_snapshot_ready(tmp_path, MASTER_ID, NOW) is False

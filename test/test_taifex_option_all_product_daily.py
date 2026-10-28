from datetime import date
import fcntl
import json

import pytest

from scripts import download_taifex_option_daily_history as producer
from scripts import taifex_daily_download_common as common


class _Sampler:
    def __init__(self, _root):
        pass

    def initial_sample(self, timeout_seconds):
        return {"state": "admitted"}


def _run(root):
    return producer._download_all_products_raw(
        root, start_date=date(2026, 10, 1), end_date=date(2026, 10, 27),
        attempts=1, request_interval=0,
    )


def test_all_products_daily_uses_original_source_and_verifies_reuse(tmp_path, monkeypatch):
    monkeypatch.setattr(producer, "AcquisitionCapacitySampler", _Sampler)
    calls = []

    def download(payload, target, **kwargs):
        calls.append(payload)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("trade_date,product,expiry,strike,right,opening,closing,volume\n"
                          "2026-10-27,TXO,202610,100,Call,1,2,3\n", encoding="utf-8")
        return target

    monkeypatch.setattr(producer, "_download", download)
    assert _run(tmp_path) == 0
    assert _run(tmp_path) == 0
    assert len(calls) == 1
    assert calls[0]["commodity_id"] == "all"
    target = next(tmp_path.glob("raw/all_products/ranges/*.csv"))
    receipt = json.loads(target.with_suffix(".receipt.json").read_text(encoding="utf-8"))
    assert receipt["point_in_time_snapshot"] is False
    assert receipt["sha256"] == producer.sha256_path(target)
    target.write_bytes(target.read_bytes() + b"corruption")
    with pytest.raises(ValueError, match="digest mismatch"):
        _run(tmp_path)
    assert len(calls) == 1


def test_all_products_defers_without_capacity_or_shared_lock(tmp_path, monkeypatch):
    class Deferred(_Sampler):
        def initial_sample(self, timeout_seconds):
            return {"state": "deferred", "reason": "physical_backing_reserve"}

    monkeypatch.setattr(producer, "AcquisitionCapacitySampler", Deferred)
    monkeypatch.setattr(producer, "_download", lambda *a, **k: pytest.fail("unexpected request"))
    assert _run(tmp_path) == 75
    assert not list(tmp_path.glob("raw/all_products/ranges/*"))
    state = tmp_path / "state" / "registered_auxiliary_daily.lock"
    with state.open("a") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert _run(tmp_path) == 75


def test_all_products_rejects_unprovenanced_existing_file(tmp_path, monkeypatch):
    monkeypatch.setattr(producer, "AcquisitionCapacitySampler", _Sampler)
    target = tmp_path / "raw/all_products/ranges/2026-10-01_2026-10-27_all.csv"
    target.parent.mkdir(parents=True)
    target.write_text("anything", encoding="utf-8")
    with pytest.raises(ValueError, match="without request provenance"):
        _run(tmp_path)


def test_finite_priority_rejects_a_range_and_unverified_capacity(tmp_path, monkeypatch):
    with pytest.raises(ValueError, match="one exact calendar day"):
        producer._download_all_products_raw(
            tmp_path, start_date=date(2026, 10, 1), end_date=date(2026, 10, 27),
            attempts=1, request_interval=0, finite_priority=True,
        )

    class UnknownSampler:
        def __init__(self, root, *, probe):
            assert root == tmp_path
            assert probe.keywords["additional_headroom_bytes"] == (
                producer.FINITE_PRIORITY_HEADROOM_BYTES + producer.FINITE_PRIORITY_RESPONSE_BYTES
            )

        def initial_sample(self, timeout_seconds):
            return {"state": "not_enrolled"}

    monkeypatch.setattr(producer, "AcquisitionCapacitySampler", UnknownSampler)
    monkeypatch.setattr(producer, "_download", lambda *a, **kw: pytest.fail("unverified capacity"))
    assert producer._download_all_products_raw(
        tmp_path, start_date=date(2026, 10, 27), end_date=date(2026, 10, 27),
        attempts=1, request_interval=0, finite_priority=True,
    ) == 75
    assert not list(tmp_path.glob("raw/all_products/ranges/*"))


def test_finite_priority_reuses_same_lock_receipt_and_bounded_request(tmp_path, monkeypatch):
    class AdmittedSampler:
        def __init__(self, root, *, probe):
            assert probe.func == producer.priority_dispatch_status

        def initial_sample(self, timeout_seconds):
            assert timeout_seconds == 29
            return {"state": "admitted"}

    calls = []

    def download(payload, target, **kwargs):
        calls.append((payload, kwargs))
        target.write_text("trade_date,product,expiry,strike,right,opening,closing,volume\n"
                          "2026-10-27,TXO,202610,100,Call,1,2,3\n", encoding="utf-8")
        return target

    monkeypatch.setattr(producer, "AcquisitionCapacitySampler", AdmittedSampler)
    monkeypatch.setattr(producer, "_download", download)
    kwargs = dict(start_date=date(2026, 10, 27), end_date=date(2026, 10, 27),
                  attempts=1, request_interval=0, finite_priority=True)
    assert producer._download_all_products_raw(tmp_path, **kwargs) == 0
    assert producer._download_all_products_raw(tmp_path, **kwargs) == 0
    assert len(calls) == 1 and calls[0][0]["commodity_id"] == "all"
    assert calls[0][1]["max_response_bytes"] == producer.FINITE_PRIORITY_RESPONSE_BYTES
    target = next(tmp_path.glob("raw/all_products/ranges/*.csv"))
    receipt = json.loads(target.with_suffix(".receipt.json").read_text(encoding="utf-8"))
    assert receipt["sha256"] == producer.sha256_path(target)


def test_finite_priority_rejects_oversized_response_before_writing(tmp_path, monkeypatch):
    sizes = []

    class Response:
        headers = {"Content-Disposition": 'attachment; filename="data.csv"'}

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def read(self, size):
            sizes.append(size)
            return b"x" * size

    monkeypatch.setattr(common.request, "urlopen", lambda *a, **kw: Response())
    target = tmp_path / "oversize.csv"
    with pytest.raises(RuntimeError) as exc:
        common.download_taifex_attachment(
            "https://example.invalid/", {"day": "1"}, target, attempts=1,
            request_interval=0, user_agent="test", max_response_bytes=100,
        )
    assert isinstance(exc.value.__cause__, ValueError)
    assert sizes == [101]
    assert not target.exists()
    assert list(tmp_path.iterdir()) == []


def test_finite_priority_year_is_split_into_bounded_monthly_requests(tmp_path, monkeypatch):
    class AdmittedSampler:
        def __init__(self, root, *, probe):
            assert probe.func == producer.priority_dispatch_status

        def initial_sample(self, timeout_seconds):
            assert timeout_seconds == 29
            return {"state": "admitted"}

    calls = []

    def download(payload, target, **kwargs):
        calls.append((payload.copy(), kwargs["max_response_bytes"]))
        target.write_text("trade_date,product,expiry,strike,right,opening,closing,volume\n"
                          "2026-01-02,TXO,202601,100,Call,1,2,3\n", encoding="utf-8")
        return target

    monkeypatch.setattr(producer, "AcquisitionCapacitySampler", AdmittedSampler)
    monkeypatch.setattr(producer, "_download", download)
    assert producer._download_all_products_raw(
        tmp_path, start_date=date(2026, 1, 1), end_date=date(2026, 2, 10),
        attempts=1, request_interval=0, finite_priority=True, finite_priority_monthly=True,
    ) == 0
    assert [(x[0]['queryStartDate'], x[0]['queryEndDate']) for x in calls] == [
        ('2026/01/01', '2026/01/31'), ('2026/02/01', '2026/02/10')]
    assert all(x[1] == producer.FINITE_PRIORITY_RESPONSE_BYTES for x in calls)
    assert len(list(tmp_path.glob('raw/all_products/ranges/*.receipt.json'))) == 2


def test_monthly_priority_cannot_be_enabled_without_priority_guard(tmp_path):
    with pytest.raises(ValueError, match="requires finite priority admission"):
        producer._download_all_products_raw(
            tmp_path, start_date=date(2026, 1, 1), end_date=date(2026, 2, 1),
            attempts=1, request_interval=0, finite_priority_monthly=True,
        )

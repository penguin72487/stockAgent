"""Head-query failures must not fabricate FX prices or erase existing history."""

from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import datetime
import json
from urllib.parse import parse_qs, urlparse
from types import SimpleNamespace

import pytest
import requests

from downloader import download_forex_frankfurter as fx


START = "1999-01-04"
THROUGH = "2000-01-12"
FIRST = "2000-01-13"


def pivot_payload():
    # Explicit test fixture, not a reconstructed market observation.
    return {"base": "EUR", "amount": 1, "start_date": START, "end_date": THROUGH,
            "rates": {START: {"USD": 1.1789}, THROUGH: {"USD": 1.0319}}}


def http_error(status=404):
    response = requests.Response()
    response.status_code = status
    return requests.HTTPError(f"fixture HTTP {status}", response=response)


def existing_pair(tmp_path, base="BRL", quote="USD"):
    record = fx.SymbolRecord(base + quote, base + quote, "forex", base, quote)
    path = tmp_path / f"{record.code}_features.parquet"
    frame = fx._normalize_rate_rows([
        {"date": date, "open": rate, "close": rate, "max": rate, "min": rate,
         "adjclose": rate, "Trading_Volume": None}
        for date, rate in [(FIRST, 0.55), ("2026-09-25", 0.19)]
    ], START, "2026-09-30")
    fx._write_parquet(frame, path)
    return record, path


@pytest.fixture(autouse=True)
def clear_singleflight():
    fx._BASE_RESPONSES.clear()
    yield
    fx._BASE_RESPONSES.clear()


def test_verified_absence_preserves_every_price_byte_and_shares_one_source(monkeypatch, tmp_path):
    records = [existing_pair(tmp_path, base, quote)
               for base in ["BRL", "CNY", "ILS", "INR"]
               for quote in ["USD", "AUD", "JPY", "CAD"]]
    before = {path: fx.sha256_file(path) for _, path in records}
    calls = []

    def get_json(url, timeout):
        calls.append(url)
        if parse_qs(urlparse(url).query)["from"] == ["EUR"]:
            return pivot_payload()
        raise http_error()

    monkeypatch.setattr(fx, "_get_json", get_json)
    with ThreadPoolExecutor(8) as workers:
        list(workers.map(lambda item: fx._repair_history_head(item[0], START, "2026-09-30", item[1], 30), records))
    assert len(calls) == 5  # Four original base queries; one shared native EUR response.
    assert len(list((tmp_path / ".head_sources").glob("*.json"))) == 1
    for record, path in records:
        assert fx.sha256_file(path) == before[path]
        proof = json.loads(path.with_suffix(".head.json").read_text())
        assert proof["head_status"] == "verified_base_unpublished"
        assert proof["unpublished_base"] == record.base
        assert proof["first_observed_date"] == FIRST
        assert proof["head_coverage_version"] == fx.HEAD_COVERAGE_VERSION
    fx._BASE_RESPONSES.clear()
    monkeypatch.setattr(fx, "_get_json", lambda *a: pytest.fail("validated prefix must not be fetched again"))
    for record, path in records:
        fx._repair_history_head(record, START, "2026-10-01", path, 30)


def test_verified_unpublished_head_allows_real_tail_and_keeps_history(monkeypatch, tmp_path):
    record, path = existing_pair(tmp_path)
    original = fx._read_parquet(path)
    calls = []

    def rates(base, start, end, timeout):
        calls.append((base, start, end))
        if start == START:
            if base == "EUR":
                return pivot_payload()
            raise http_error()
        return {"rates": {"2026-09-28": {"USD": 0.188}}}

    monkeypatch.setattr(fx, "_base_rates", rates)
    result = fx._download_pair(record, START, "2026-09-30", tmp_path, 30, False, True)
    assert result.status == "updated_incremental" and result.rows == 3
    frame = fx._read_parquet(path)
    assert frame.filter(fx.pl.col("date") <= datetime(2026, 9, 25)).equals(original)
    assert frame["date"].min() == datetime(2000, 1, 13)
    assert frame["close"].to_list() == [0.55, 0.19, 0.188]
    assert frame["Trading_Volume"].null_count() == 3
    assert calls[-1] == ("BRL", "2026-09-26", "2026-09-30")
    proof = json.loads(path.with_suffix(".head.json").read_text())
    assert proof["head_status"] == "verified_base_unpublished"
    assert proof["source_sha256"] == fx.sha256_file(path)
    fx._repair_history_head(record, START, "2026-10-01", path, 30)
    assert len(calls) == 3


@pytest.mark.parametrize("defect", ["has_base", "empty", "wrong_base", "wrong_amount", "bool_amount",
                                  "wrong_start", "wrong_end", "bad_date", "bad_row", "bad_rate",
                                  "bool_rate", "bad_currency", "missing_boundary", "mixed_keys"])
def test_a_404_cannot_be_hidden_by_untrustworthy_pivot_evidence(monkeypatch, tmp_path, defect):
    record, path = existing_pair(tmp_path)
    digest = fx.sha256_file(path)
    payload = deepcopy(pivot_payload())
    if defect == "has_base":
        payload["rates"][START]["BRL"] = 1.8
    elif defect == "empty":
        payload["rates"] = {}
    elif defect == "wrong_base":
        payload["base"] = "USD"
    elif defect == "wrong_amount":
        payload["amount"] = 2
    elif defect == "bool_amount":
        payload["amount"] = True
    elif defect == "wrong_start":
        payload["start_date"] = "1998-01-04"
    elif defect == "wrong_end":
        payload["end_date"] = "2000-01-11"
    elif defect == "bad_date":
        payload["rates"]["1999-02-30"] = {"USD": 1.1}
    elif defect == "bad_row":
        payload["rates"][START] = []
    elif defect == "bad_rate":
        payload["rates"][START]["USD"] = float("nan")
    elif defect == "bool_rate":
        payload["rates"][START]["USD"] = True
    elif defect == "bad_currency":
        payload["rates"][START]["usd"] = 1.1
    elif defect == "missing_boundary":
        del payload["rates"][THROUGH]
    elif defect == "mixed_keys":
        payload["rates"][None] = {"USD": 1.1}

    def rates(base, *args):
        if base == "EUR":
            return payload
        raise http_error()

    monkeypatch.setattr(fx, "_base_rates", rates)
    result = fx._download_pair(record, START, "2026-09-30", tmp_path, 30, False, True)
    assert result.status == "failed_head_source" and result.rows == 2
    assert fx.sha256_file(path) == digest
    assert not path.with_suffix(".head.json").exists()


@pytest.mark.parametrize("status", [401, 403, 429, 500, 503])
def test_other_source_errors_never_trigger_absence_fallback(monkeypatch, tmp_path, status):
    record, path = existing_pair(tmp_path)
    digest = fx.sha256_file(path)
    calls = []

    def rates(base, *args):
        calls.append(base)
        raise http_error(status)

    monkeypatch.setattr(fx, "_base_rates", rates)
    result = fx._download_pair(record, START, "2026-09-30", tmp_path, 30, False, True)
    assert result.status == "failed_head_source" and result.rows == 2
    assert calls == ["BRL"] and fx.sha256_file(path) == digest


def test_a_short_checked_window_does_not_cover_a_later_end(monkeypatch, tmp_path):
    record, path = existing_pair(tmp_path, "EUR")
    calls = []
    monkeypatch.setattr(fx, "_base_rates", lambda base, start, end, timeout:
                        calls.append(end) or {"rates": {start: {"CAD": 1.4}}})
    fx._repair_history_head(record, START, "1999-06-01", path, 30)
    fx._repair_history_head(record, START, "2026-09-30", path, 30)
    assert calls == ["1999-06-01", THROUGH]


def test_changed_or_missing_pivot_invalidates_the_head_proof(monkeypatch, tmp_path):
    record, path = existing_pair(tmp_path)

    def rates(base, *args):
        if base == "EUR":
            return pivot_payload()
        raise http_error()

    monkeypatch.setattr(fx, "_base_rates", rates)
    fx._repair_history_head(record, START, "2026-09-30", path, 30)
    source_path = fx._pivot_source_path(tmp_path, START, THROUGH)
    damaged = json.loads(source_path.read_text())
    damaged["response"]["rates"][START]["BRL"] = 1.8
    fx.atomic_write_json(source_path, damaged)
    with pytest.raises(fx.HistoricalSourceError, match="base observations"):
        fx._repair_history_head(record, START, "2026-09-30", path, 30)


def test_corrupt_source_evidence_is_preserved_and_not_mislabeled_as_bad_prices(monkeypatch, tmp_path):
    record, path = existing_pair(tmp_path)
    digest = fx.sha256_file(path)

    def rates(base, *args):
        if base == "EUR":
            return pivot_payload()
        raise http_error()

    monkeypatch.setattr(fx, "_base_rates", rates)
    fx._repair_history_head(record, START, "2026-09-30", path, 30)
    source_path = fx._pivot_source_path(tmp_path, START, THROUGH)
    fx.atomic_write_text(source_path, "{corrupt fixture")
    result = fx._download_pair(record, START, "2026-09-30", tmp_path, 30, False, True)
    assert result.status == "failed_head_source" and result.rows == 2
    assert source_path.read_text() == "{corrupt fixture"
    assert fx.sha256_file(path) == digest


def test_native_eur_404_cannot_be_declared_unpublished(monkeypatch, tmp_path):
    record, path = existing_pair(tmp_path, "EUR")
    monkeypatch.setattr(fx, "_base_rates", lambda *a: (_ for _ in ()).throw(http_error()))
    result = fx._download_pair(record, START, "2026-09-30", tmp_path, 30, False, True)
    assert result.status == "failed_head_source" and result.rows == 2
    assert not (tmp_path / ".head_sources").exists()


def test_successful_but_empty_head_response_does_not_establish_coverage(monkeypatch, tmp_path):
    record, path = existing_pair(tmp_path)
    monkeypatch.setattr(fx, "_base_rates", lambda *a: {"rates": {}})
    result = fx._download_pair(record, START, "2026-09-30", tmp_path, 30, False, True)
    assert result.status == "failed_head_source" and result.rows == 2
    assert not path.with_suffix(".head.json").exists()


def test_contract_pins_v1_ecb_not_blended_v2():
    contract = fx.acquisition_contract()
    assert contract["api_base"] == "https://api.frankfurter.dev/v1"
    assert contract["provider"] == "ECB" and contract["price_schema_version"] == 1
    assert len(contract["fingerprint_sha256"]) == 64


def test_main_holds_canonical_writer_lock_through_failure(monkeypatch, tmp_path):
    fcntl = pytest.importorskip("fcntl")
    monkeypatch.setattr(fx, "parse_args", lambda: SimpleNamespace(output_dir=tmp_path, lock_timeout_seconds=0))

    def failing_download(args):
        with (tmp_path / ".download.lock").open("a+") as competing:
            with pytest.raises(BlockingIOError):
                fcntl.flock(competing.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        raise RuntimeError("fixture source failure")

    monkeypatch.setattr(fx, "_run_download", failing_download)
    with pytest.raises(RuntimeError, match="fixture source failure"):
        fx.main()
    with (tmp_path / ".download.lock").open("a+") as next_writer:
        fcntl.flock(next_writer.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(next_writer.fileno(), fcntl.LOCK_UN)


@pytest.mark.parametrize("value", ["-1", "nan", "inf"])
def test_lock_timeout_uses_shared_finite_nonnegative_contract(monkeypatch, value):
    monkeypatch.setattr("sys.argv", ["collector", "--lock-timeout-seconds", value])
    with pytest.raises(SystemExit):
        fx.parse_args()

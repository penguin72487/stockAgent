from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts import finlab_arrow_history as arrow
from scripts import download_finlab_history as history


def ipc_file(path, table, *, batch_size=2):
    with pa.OSFile(str(path), "wb") as handle:
        with pa.ipc.new_file(handle, table.schema) as writer:
            writer.write_table(table, max_chunksize=batch_size)
    return path


def test_wide_metadata_projects_columns_retains_raw_labels_and_null_semantics(tmp_path):
    raw = ipc_file(tmp_path / "wide.feather", pa.table({
        "date": [datetime(2020, 1, 2), datetime(2020, 1, 3), datetime(2020, 1, 6)],
        "2330 OLD": ["上市", None, ""], "2330 NEW": [None, "上市", None],
        "9999 EMPTY": pa.array([None] * 3, type=pa.string()),
    }))
    target = tmp_path / "out.parquet"
    with patch("pandas.read_feather", side_effect=AssertionError("full pandas read forbidden")):
        stats = arrow.convert_raw("after_market_fixed_price:市場別", raw, target, column_batch_size=1)
    actual = pq.read_table(target).to_pydict()
    assert actual["value"] == ["上市", "", "上市"]
    assert actual["symbol"] == ["2330"] * 3
    assert actual["source_column"] == ["2330 OLD", "2330 OLD", "2330 NEW"]
    assert stats["source_rows"] == stats["source_rows_with_values"] == stats["rows"] == 3
    assert stats["field_columns"] == 3 and stats["storage_layout"] == "sparse_long_raw_columns"
    assert stats["first_event_at"] == "2020-01-02T00:00:00"
    assert stats["last_event_at"] == "2020-01-06T00:00:00"


def test_all_null_metadata_does_not_become_usable_observations(tmp_path):
    raw = ipc_file(tmp_path / "empty.feather", pa.table({
        "date": [datetime(2020, 1, 2)], "2330": pa.array([None], type=pa.string()),
    }))
    stats = arrow.convert_raw("after_market_fixed_price:資料來源", raw, tmp_path / "out.parquet")
    assert stats["rows"] == stats["rows_with_values"] == stats["source_rows_with_values"] == 0
    assert stats["source_rows"] == 1 and stats["last_event_at"] is None


def test_broker_batches_keep_raw_values_types_and_global_row_index(tmp_path):
    raw = ipc_file(tmp_path / "broker.feather", pa.table({
        "date": [datetime(2018, 1, 2)] * 3, "stock_id": ["2330", "2303", "2330"],
        "broker": ["A", "B", "C"], "buy": [0.0, None, 1.25], "sell": [2.0, 0.5, None],
    }))
    path = tmp_path / "out.parquet"
    stats = arrow.convert_raw("broker_transactions", raw, path)
    result = pq.read_table(path).to_pydict()
    assert result["source_index"] == ["0", "1", "2"]
    assert result["buy"] == [0.0, None, 1.25]
    assert result["symbol"] == result["stock_id"]
    assert stats["rows"] == 3 and stats["first_event_at"] == "2018-01-02T00:00:00"


def test_schema_drift_fails_closed(tmp_path):
    raw = ipc_file(tmp_path / "wrong.feather", pa.table({"date": [datetime(2020, 1, 2)], "renamed": [1]}))
    for key in arrow.STREAMING_KEYS:
        with pytest.raises(ValueError):
            arrow.convert_raw(key, raw, tmp_path / "out.parquet")


class Response:
    status_code = 200

    def __init__(self, chunks, length):
        self.chunks, self.headers = chunks, {"Content-Length": str(length)}

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def iter_content(self, **kwargs):
        yield from self.chunks


@pytest.mark.parametrize("chunks,length,maximum", [([b"123"], 4, 10), ([b"123", b"456"], 0, 5), ([b"123"], 100, 5)])
def test_raw_transfer_rejects_truncation_or_oversize_and_hides_url(tmp_path, chunks, length, maximum):
    secret_url = "https://storage.googleapis.com/bucket/table?secret=NEVER-PRINT"
    with pytest.raises((ValueError, arrow.FinlabResourceDeferred)) as exc:
        arrow.stream_download(secret_url, tmp_path / "data.tmp", maximum_bytes=maximum,
                              get=lambda *a, **k: Response(chunks, length))
    assert "NEVER-PRINT" not in str(exc.value)


def test_raw_transfer_is_streamed_without_redirects(tmp_path):
    calls = []

    def get(url, **kwargs):
        calls.append(kwargs)
        return Response([b"123", b"456"], 6)

    path = tmp_path / "out"
    assert arrow.stream_download("https://storage.googleapis.com/bucket/file", path, maximum_bytes=10, get=get) == 6
    assert path.read_bytes() == b"123456"
    assert calls[0]["stream"] and not calls[0]["allow_redirects"]
    with pytest.raises(ValueError):
        arrow.stream_download("https://example.net/?token=x", path, maximum_bytes=10, get=get)
    assert len(calls) == 1


def test_streaming_adapter_reuses_main_receipts_preserves_old_version(tmp_path, monkeypatch):
    raw = ipc_file(tmp_path / "raw.feather", pa.table({
        "date": [datetime(2020, 1, 2)], "2330 NAME": ["上市"],
    }))
    key = "after_market_fixed_price:市場別"

    def prepare(key, root, stem, destination, **kwargs):
        return arrow.convert_raw(key, raw, destination)

    monkeypatch.setattr(history, "prepare_arrow", prepare)
    first = history.fetch_one(key, tmp_path)
    second = history.fetch_one(key, tmp_path)
    assert first["last_check_result"] == "downloaded"
    assert second["last_check_result"] == "unchanged"
    assert history.has_local_download(key, tmp_path)
    assert (tmp_path / first["parquet_path"]).is_file()
    assert history.audit_local(tmp_path) == (1, 0)


def test_resource_deferral_is_not_permission_or_provider_error():
    assert history.classify_provider_error(arrow.FinlabResourceDeferred("quota")) == "resource_deferred"
    assert "broker_transactions" not in history.AUTOMATICALLY_DEFERRED_KEYS


def test_inventory_batches_match_native_sdk_normalization_and_nullable_values(tmp_path):
    import pandas as pd
    from finlab.data.transform import process_data

    raw_table = pa.table({"stock_id": ["0050", "2330", "2330"],
        "date": pa.array([datetime(2020, 1, 2), datetime(2020, 1, 3), None], type=pa.timestamp("ns")),
        "持股分級": ["1", "10", None], "人數": [0, 5, 10], "持有股數": [0, 1000, 1],
        "占集保庫存數比例": [0.0, float("nan"), None],
        "key_date": pa.array([datetime(2020, 1, 4)]*3, type=pa.timestamp("ns"))})
    raw = ipc_file(tmp_path / "inventory.feather", raw_table, batch_size=2)
    expected, old_stats = history.serialize_provider_frame(process_data("inventory", raw_table.to_pandas()))
    target = tmp_path / "inventory.parquet"
    with patch("pandas.read_feather", side_effect=AssertionError("full pandas forbidden")):
        stats = arrow.convert_raw("inventory", raw, target)
    pd.testing.assert_frame_equal(pq.read_table(target).to_pandas(), expected.to_pandas())
    for field in ("rows", "rows_with_values", "field_columns", "non_null_values", "source_index_dtype",
                  "first_non_null_source_index", "last_non_null_source_index", "first_event_at", "last_event_at"):
        assert stats[field] == old_stats[field]
    assert stats["normalization_contract_version"] == 2


def test_inventory_dictionary_encoded_cloud_payload_keeps_real_labels_not_codes(tmp_path):
    table = pa.table({"stock_id": pa.array(["0050", "2330"]).dictionary_encode(),
        "date": pa.array([datetime(2020, 1, 2)]*2, type=pa.timestamp("ns")),
        "持股分級": pa.array(["1", "10"]).dictionary_encode(), "人數": [1, 10],
        "持有股數": [1000, 10000], "占集保庫存數比例": [0.5, 5.0],
        "key_date": pa.array([datetime(2020, 1, 3)]*2, type=pa.timestamp("ns"))})
    raw = ipc_file(tmp_path / "cloud.feather", table)
    target = tmp_path / "out.parquet"
    arrow.convert_raw("inventory", raw, target)
    result = pq.read_table(target)
    assert result["symbol"].to_pylist() == ["0050", "2330"]
    assert result["持股分級"].to_pylist() == ["1", "10"] and pa.types.is_string(result["持股分級"].type)


def test_same_raw_generation_skips_reconversion_only_with_a_verified_normalized_object(tmp_path, monkeypatch):
    key = "after_market_fixed_price:市場別"
    raw = ipc_file(tmp_path / "raw.feather", pa.table({"date": [datetime(2020, 1, 2)], "2330 NAME": ["上市"]}))
    (tmp_path / "raw").mkdir()
    evidence = arrow._commit_raw(raw, tmp_path, history.safe_stem(key), key, "upstream_forced", "test")
    monkeypatch.setattr(arrow, "acquire_raw", lambda *a, **kw: (tmp_path/evidence["raw_path"], evidence))
    first = history.fetch_one(key, tmp_path, refresh=True)
    with patch.object(arrow, "convert_raw", side_effect=AssertionError("unchanged raw must not decode again")):
        second = history.fetch_one(key, tmp_path, refresh=True)
    assert second["last_check_result"] == "unchanged" and second["sha256"] == first["sha256"]
    (tmp_path/first["parquet_path"]).write_bytes(b"invalid normalized object")
    with patch.object(arrow, "convert_raw", side_effect=RuntimeError("must rebuild corrupt object")):
        with pytest.raises(RuntimeError, match="must rebuild"):
            history.fetch_one(key, tmp_path, refresh=True)


def test_raw_recent_metadata_reuses_exact_acquired_clock_without_decoding_or_charge(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from finlab.data import auth
    import finlab.auth

    raw = ipc_file(tmp_path / "source.feather", pa.table({"date": [datetime(2020, 1, 2)], "2330": ["上市"]}))
    (tmp_path / "raw").mkdir()
    key = "after_market_fixed_price:市場別"
    stem = history.safe_stem(key)
    old = datetime.now(UTC)-timedelta(days=1)
    evidence = arrow._commit_raw(raw, tmp_path, stem, key, "upstream_forced", "test")
    evidence.update(source_checked_at_utc=old.isoformat(), provider_expiry_at_utc=old.isoformat(),
                    raw_acquired_at_utc=(old-timedelta(hours=1)).isoformat())
    arrow._atomic_json(tmp_path / "raw_receipts" / (stem+".json"), evidence)
    requested = []

    def metadata(request):
        requested.append(request)
        return {key+".recent": SimpleNamespace(error=None, data=None, url=None,
                                               expiry=datetime.now(UTC)+timedelta(hours=2))}

    monkeypatch.setattr(auth, "fetch_metadata_batch", metadata)
    monkeypatch.setattr(finlab.auth, "get_data_status", lambda: pytest.fail("unchanged raw is not a full allocation"))
    path, checked = arrow.acquire_raw(key, tmp_path, stem, refresh=True)
    assert path.is_file() and checked["source_check_mode"] == "upstream_incremental"
    assert checked["raw_acquired_at_utc"] == evidence["raw_acquired_at_utc"]
    assert requested[0][key+".recent"].isoformat() == evidence["raw_acquired_at_utc"]
    assert checked["source_checked_at_utc"] != evidence["source_checked_at_utc"]
    arrow.acquire_raw(key, tmp_path, stem, refresh=True)  # Future expiry, not another metadata request.
    assert len(requested) == 1


@pytest.mark.parametrize("value,status", [(None, "provider_empty"), (float("nan"), "provider_empty"),
                                          (0.0, "normalization_error"), (1.25, "normalization_error")])
def test_empty_sdk_result_compared_with_raw_values_without_inventing_data(tmp_path, value, status):
    raw = ipc_file(tmp_path / "source.feather", pa.table({
        "date": [datetime(2020, 1, 2)], "2330": pa.array([value], type=pa.float64()),
    }))
    key = "dividend_otc:權息"
    evidence = arrow.capture_empty_evidence(key, tmp_path, history.safe_stem(key), cache_path=raw)
    assert (tmp_path / evidence["empty_evidence_path"]).is_file()
    assert not (tmp_path / "receipts").exists()
    exc = history.EmptyProviderFrame(rows=1, fields=1, evidence=evidence)
    assert history.classify_provider_error(exc) == status
    assert evidence["raw_non_null_values"] == (1 if status == "normalization_error" else 0)


def test_raw_object_reuse_checks_bytes_not_filename(tmp_path):
    raw = ipc_file(tmp_path / "temporary.feather", pa.table({
        "date": [datetime(2020, 1, 2)], "2330": ["上市"],
    }))
    (tmp_path / "raw").mkdir()
    key = "after_market_fixed_price:資料來源"
    receipt = arrow._commit_raw(raw, tmp_path, history.safe_stem(key), key,
                                "sdk_cache_allowed", "test")
    assert arrow._verified_raw(tmp_path, receipt, key) is not None
    path = tmp_path / receipt["raw_path"]
    data = path.read_bytes()
    path.write_bytes(b"X" + data[1:])
    assert arrow._verified_raw(tmp_path, receipt, key) is None


def test_raw_reuse_is_expiry_bound_not_whole_quota_day(tmp_path, monkeypatch):
    import finlab.auth
    from finlab.data import auth
    raw = ipc_file(tmp_path / "temporary.feather", pa.table({
        "date": [datetime(2020, 1, 2)], "2330": ["上市"],
    }))
    (tmp_path / "raw").mkdir()
    key = "after_market_fixed_price:資料來源"
    stem = history.safe_stem(key)
    evidence = arrow._commit_raw(raw, tmp_path, stem, key, "upstream_forced", "test")
    evidence["provider_expiry_at_utc"] = (datetime.now(UTC)+timedelta(hours=1)).isoformat()
    arrow._atomic_json(tmp_path / "raw_receipts" / f"{stem}.json", evidence)
    monkeypatch.setattr(finlab.auth, "get_data_status", lambda: {"limit_size": 5000, "quota": 0})
    def source_reached(*args, **kwargs):
        raise RuntimeError("source reached after expiry")
    monkeypatch.setattr(auth, "fetch_metadata_batch", source_reached)
    _, reused = arrow.acquire_raw(key, tmp_path, stem, refresh=True)
    assert reused["raw_sha256"] == evidence["raw_sha256"]
    evidence["source_checked_at_utc"] = (datetime.now(UTC)-timedelta(hours=2)).isoformat()
    evidence["provider_expiry_at_utc"] = (datetime.now(UTC)-timedelta(hours=1)).isoformat()
    arrow._atomic_json(tmp_path / "raw_receipts" / f"{stem}.json", evidence)
    with pytest.raises(RuntimeError, match="source reached after expiry"):
        arrow.acquire_raw(key, tmp_path, stem, refresh=True)


@pytest.mark.parametrize("quota", [4900, float("nan"), float("inf")])
def test_quota_guard_precedes_signing_or_charging_a_broker_object(tmp_path, monkeypatch, quota):
    import finlab.auth
    from finlab.data import auth
    monkeypatch.setattr(finlab.auth, "get_data_status", lambda: {"limit_size": 5000, "quota": quota})

    def forbidden(*args, **kwargs):
        raise AssertionError("must not sign or spend insufficient/unknown quota")

    monkeypatch.setattr(auth, "fetch_metadata_batch", forbidden)
    with pytest.raises(arrow.FinlabResourceDeferred):
        arrow.acquire_raw("broker_transactions", tmp_path, "broker", refresh=True)

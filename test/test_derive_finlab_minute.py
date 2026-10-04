from datetime import UTC, date, datetime
import hashlib
import json

import pandas as pd
import polars as pl
import pytest

from scripts.derive_finlab_minute import (
    derive_partition, derived_receipt_path, stored_derived_receipt, ticks_to_minutes,
)
from scripts.download_finlab_intraday import fetch_partition, reconcile_stored_tick_unit


DAY = date(2026, 6, 1)


def ticks():
    return pd.DataFrame({
        "stock_id": ["2330"] * 6,
        "trade_date": [DAY.isoformat()] * 6,
        "timestamp": pd.to_datetime([
            "2026-06-01 09:00:01+08:00", "2026-06-01 09:00:01+08:00",
            "2026-06-01 09:00:59+08:00", "2026-06-01 13:30:00+08:00",
            "2026-06-01 14:30:00+08:00", "2026-06-01 09:01:01+08:00",
        ]),
        "sequence": [0, 1, 2, 3, 4, 5],
        "close": [100., 102., 101., 99., 98., 105.],
        "volume": [1, 2, 3, 4, 5, 0],
        "session": ["regular"] * 4 + ["after_hours_fixed", "regular"],
    })


def test_derived_bars_keep_sequence_auction_and_only_traded_minutes():
    bars = ticks_to_minutes(ticks(), "2330", DAY)
    assert bars["timestamp"].dt.strftime("%H:%M").tolist() == ["09:00", "13:30"]
    first = bars.iloc[0]
    assert (first["open"], first["high"], first["low"], first["close"]) == (100, 102, 100, 101)
    assert (first["volume"], first["tick_count"]) == (6, 3)
    assert first["vwap"] == pytest.approx((100 + 204 + 303) / 6)


def test_derivation_is_local_idempotent_and_provenance_pinned(tmp_path, monkeypatch):
    from finlab import data
    monkeypatch.setattr(data, "get", lambda *args, **kwargs: ticks())
    source = fetch_partition(tmp_path, "tw_tick:2330", DAY,
                             now=datetime(2026, 9, 25, tzinfo=UTC))
    receipt = derive_partition(tmp_path, "2330", DAY)
    assert receipt["source_tick_sha256"] == source["sha256"]
    assert receipt["source_kind"] == "derived_from_tw_tick"
    assert receipt["status"] == "derived_unverified_for_pit"
    assert receipt["rows"] == 2
    assert receipt["canonical_volume_unit"] == "unresolved"
    assert pd.read_parquet(tmp_path / receipt["parquet_path"])["volume_shares"].isna().all()
    assert stored_derived_receipt(tmp_path, "2330", DAY, source["sha256"]) == receipt
    assert derive_partition(tmp_path, "2330", DAY) == receipt
    assert json.loads(derived_receipt_path(tmp_path, "2330", DAY).read_text()) == receipt


def test_invalid_regular_timestamp_fails_closed():
    frame = ticks()
    frame.loc[0, "timestamp"] = pd.Timestamp("2026-06-01 08:59:00+08:00")
    with pytest.raises(ValueError, match="outside 09:00"):
        ticks_to_minutes(frame, "2330", DAY)


def test_finlab_tick_volume_is_shares_only_after_exact_shioaji_reconciliation(
    tmp_path, monkeypatch,
):
    from finlab import data
    from scripts.finlab_volume_units import with_verified_volume_shares

    reference_root = tmp_path / "shioaji"
    folder = reference_root / "minute_chunks/2330"
    folder.mkdir(parents=True)
    parquet = folder / "2026-06-01_2026-06-01.parquet"
    pl.DataFrame({
        "ts": [datetime(2026, 6, 1, 9, 1), datetime(2026, 6, 1, 13, 30)],
        "date": [DAY, DAY], "Open": [100., 99.], "High": [102., 99.],
        "Low": [100., 99.], "Close": [101., 99.], "Volume": [6., 4.],
        "Amount": [607_000., 396_000.],
    }).write_parquet(parquet)
    digest = hashlib.sha256(parquet.read_bytes()).hexdigest()
    parquet.with_suffix(".receipt.json").write_text(json.dumps({
        "status": "ok", "symbol": "2330", "security_type": "stock",
        "market": "twse", "contract_unit": 1000,
        "output_receipt": {"sha256": digest, "size": parquet.stat().st_size},
    }))
    normalized, evidence = with_verified_volume_shares(
        ticks(), "2330", DAY, reference_root=reference_root,
    )
    assert evidence["canonical_volume_unit"] == "shares"
    assert evidence["volume_multiplier"] == 1000
    assert normalized["volume_shares"].iloc[:4].tolist() == [1000, 2000, 3000, 4000]
    assert pd.isna(normalized["volume_shares"].iloc[4])
    assert normalized["volume_shares"].iloc[5] == 0
    monkeypatch.setattr(data, "get", lambda *args, **kwargs: ticks())
    first_receipt = fetch_partition(
        tmp_path / "finlab", "tw_tick:2330", DAY,
        now=datetime(2026, 9, 25, tzinfo=UTC), reference_root=tmp_path / "missing",
    )
    assert first_receipt["canonical_volume_unit"] == "unresolved"
    receipt = reconcile_stored_tick_unit(
        tmp_path / "finlab", "tw_tick:2330", DAY, reference_root=reference_root,
    )
    assert receipt["canonical_volume_unit"] == "shares"
    assert receipt["raw_object_sha256"] == first_receipt["sha256"]
    assert (tmp_path / "finlab" / first_receipt["parquet_path"]).is_file()
    assert reconcile_stored_tick_unit(
        tmp_path / "finlab", "tw_tick:2330", DAY, reference_root=reference_root,
    ) == receipt
    derived = derive_partition(tmp_path / "finlab", "2330", DAY, reference_root=reference_root)
    bars = pd.read_parquet(tmp_path / "finlab" / derived["parquet_path"])
    assert bars["volume"].tolist() == [6, 4]
    assert bars["volume_shares"].tolist() == [6000, 4000]
    # Provider-minute partitions get the same share contract. A legacy raw
    # minute object remains recoverable after local-only reconciliation.
    provider_minute = ticks_to_minutes(ticks(), "2330", DAY).drop(columns="volume_shares")
    monkeypatch.setattr(data, "get", lambda *args, **kwargs: provider_minute)
    raw_minute = fetch_partition(
        tmp_path / "finlab", "tw_minute:2330", DAY,
        now=datetime(2026, 9, 25, tzinfo=UTC), reference_root=tmp_path / "missing",
    )
    assert raw_minute["canonical_volume_unit"] == "unresolved"
    minute_receipt = reconcile_stored_tick_unit(
        tmp_path / "finlab", "tw_minute:2330", DAY, reference_root=reference_root,
    )
    assert minute_receipt["canonical_volume_unit"] == "shares"
    assert pd.read_parquet(tmp_path / "finlab" / minute_receipt["parquet_path"])["volume_shares"].tolist() == [6000, 4000]
    assert (tmp_path / "finlab" / raw_minute["parquet_path"]).is_file()

    # A tick-derived Amount is circular evidence. Losing the direct reference
    # must remove stale shares from the object as well as from its receipt.
    reference_path = parquet.with_suffix(".receipt.json")
    reference_text = reference_path.read_text()
    reference_payload = json.loads(reference_text)
    reference_payload["underlying_data_method"] = "observed_ticks_aggregated_to_right_labelled_1m"
    reference_path.write_text(json.dumps(reference_payload))
    rejected = reconcile_stored_tick_unit(
        tmp_path / "finlab", "tw_tick:2330", DAY, reference_root=reference_root,
    )
    assert rejected["canonical_volume_unit"] == "unresolved"
    assert rejected["volume_multiplier"] is None
    assert rejected["volume_unit_reference"] is None
    rejected_frame = pd.read_parquet(tmp_path / "finlab" / rejected["parquet_path"])
    assert rejected_frame["volume_shares"].isna().all()
    assert rejected_frame["volume"].tolist() == ticks()["volume"].tolist()

    # Restoring direct broker evidence triggers a local retry, even though no
    # new FinLab source data was fetched and the old receipt is already v2.
    reference_path.write_text(reference_text)
    refreshed = derive_partition(
        tmp_path / "finlab", "2330", DAY, reference_root=reference_root,
    )
    assert refreshed["canonical_volume_unit"] == "shares"
    assert pd.read_parquet(tmp_path / "finlab" / refreshed["parquet_path"])["volume_shares"].tolist() == [6000, 4000]


def test_finlab_unit_mismatch_does_not_multiply_raw_volume(tmp_path):
    from scripts.finlab_volume_units import with_verified_volume_shares

    normalized, evidence = with_verified_volume_shares(
        ticks(), "2330", DAY, reference_root=tmp_path,
    )
    assert evidence["canonical_volume_unit"] == "unresolved"
    assert normalized["volume"].tolist() == ticks()["volume"].tolist()
    assert normalized["volume_shares"].isna().all()


def test_partial_share_proof_does_not_create_partial_minute_capacity():
    frame = ticks()
    frame["volume_shares"] = pd.Series([1000, pd.NA, 3000, 4000, pd.NA, 0], dtype="Int64")
    bars = ticks_to_minutes(frame, "2330", DAY)
    assert pd.isna(bars.iloc[0]["volume_shares"])
    assert bars.iloc[1]["volume_shares"] == 4000

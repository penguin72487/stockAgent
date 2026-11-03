"""Synthetic fixed records exercise mechanics, not historical source admission."""
from dataclasses import replace
from datetime import date
import json

import numpy as np
import polars as pl
import pytest

from downloader.artifact_io import sha256_file
from stockagent.data.tw_futures_account_attachment import _reject_unreviewed_holiday_valuations
from stockagent.data.tw_futures_holiday_c11 import (
    HOLIDAY_C11_ABI, decode_c11, decode_p08_futures, load_holiday_c11,
    require_complete_holiday_c11, apply_holiday_c11_marks,
)


def _c11(*, code="C11", day="20260212", identifier="TXFC6", encoded=32105, oi=0):
    n = 10 if code == "C11" else 20
    return (f"{identifier:<{n}}{encoded:09d}{oi:08d}{day}N" + " " * 28).encode()


def _p08(*, code="P08", identifier="TXFC6", decimals=0):
    n = 10 if code == "P08" else 20
    return (f"{identifier:<{n}}202603000000000 2025120120260318"
            f"000030000IY{decimals}0000101202603180001" + " " * 87).encode()


def _source(tmp_path, *, price=32105., encoded=32105, decimals=0):
    physical = pl.DataFrame({"date": [date(2026, 2, 12)], "product": ["TX"], "contract": ["202603"],
                             "physical_instance": ["TX:202603@generation=0"], "valuation_price": [price],
                             "accounting_only_date": [True], "cash_settlement": [False]})
    daily = tmp_path / "daily.parquet"
    physical.write_parquet(daily)
    folder = tmp_path / "data_holiday"
    folder.mkdir()

    def original(name, body, code, day):
        path = folder / name
        path.write_bytes(body)
        receipt = path.with_suffix(".json")
        receipt.write_text(json.dumps({"source_url": "https://example.test/synthetic-source",
                                       "retrieved_at_utc": "2026-10-31T01:00:00+00:00",
                                       "data_date": day, "content_sha256": sha256_file(path)}))
        return {"path": str(path.relative_to(tmp_path)), "sha256": sha256_file(path),
                "receipt_path": str(receipt.relative_to(tmp_path)), "receipt_sha256": sha256_file(receipt),
                "file_code": code}

    marks = original("marks.raw", _c11(encoded=encoded), "C11", "2026-02-12")
    catalog = original("catalog.raw", _p08(decimals=decimals), "P08", "2026-02-11")
    manifest = {"contract": HOLIDAY_C11_ABI, "layout": "taifex_file_transfer_1_14_1",
                "financial_daily_sha256": sha256_file(daily), "batches": [
                    {"account_date": "2026-02-12", "catalog_date": "2026-02-11",
                     "marks": marks, "catalog": catalog}]}
    (folder / "manifest.json").write_text(json.dumps(manifest))
    return load_holiday_c11(folder, workspace=tmp_path), physical, daily, folder


@pytest.mark.parametrize("code,catalog_code", [("C11", "P08"), ("CB1", "PA8")])
def test_short_and_long_layouts_keep_raw_price_and_dated_decimals(code, catalog_code):
    raw = _c11(code=code, encoded=123456)
    assert len(raw) == (64 if code == "C11" else 74)
    records = decode_c11(raw + b"\r\n", file_code=code)
    assert records[0]["encoded_price"] == 123456
    assert records[0]["date"] == date(2026, 2, 12)
    catalog = _p08(code=catalog_code, decimals=3)
    assert len(catalog) == (160 if catalog_code == "P08" else 170)
    definition = decode_p08_futures(catalog, file_code=catalog_code)["TXFC6"]
    assert definition == {"product": "TX", "contract": "202603", "begin": date(2025, 12, 1),
                           "end": date(2026, 3, 18), "decimals": 3}


@pytest.mark.parametrize("mutate", [
    lambda raw: raw[:-1],
    lambda raw: raw[:10] + b"-00000001" + raw[19:],
    lambda raw: raw[:27] + b"20260230" + raw[35:],
    lambda raw: raw[:35] + b" " + raw[36:],  # option underlying appendix
    lambda raw: raw + b"\n\n",
])
def test_c11_rejects_truncation_signed_price_bad_date_and_option_appendix(mutate):
    with pytest.raises(ValueError):
        decode_c11(mutate(_c11()), file_code="C11")


def test_c11_rejects_duplicate_identity_in_packed_or_line_delimited_file():
    for separator in (b"", b"\n", b"\r\n"):
        with pytest.raises(ValueError, match="duplicate"):
            decode_c11(_c11() + separator + _c11(), file_code="C11")


def test_retired_zero_record_cannot_value_a_held_contract_or_block_valid_other_records(tmp_path):
    _, physical, daily, folder = _source(tmp_path)
    path = folder / "manifest.json"
    manifest = json.loads(path.read_text())
    retired = _c11(identifier="TEFC6", encoded=0)
    retired = retired[:35] + b"Z" + retired[36:]
    raw = folder / "marks.raw"
    raw.write_bytes(raw.read_bytes() + retired)
    item = manifest["batches"][0]["marks"]
    item["sha256"] = sha256_file(raw)
    receipt = tmp_path / item["receipt_path"]
    doc = json.loads(receipt.read_text())
    doc["content_sha256"] = item["sha256"]
    receipt.write_text(json.dumps(doc))
    item["receipt_sha256"] = sha256_file(receipt)
    path.write_text(json.dumps(manifest))
    source = load_holiday_c11(folder, workspace=tmp_path)
    require_complete_holiday_c11(daily, physical, source)
    with pytest.raises(ValueError, match="incomplete"):
        require_complete_holiday_c11(daily, physical.with_columns(pl.lit("TE").alias("product")), source)


def test_source_admission_reconstructs_price_and_preserves_other_gates(tmp_path):
    source, physical, daily, _ = _source(tmp_path, price=321.05, encoded=32105, decimals=2)
    assert source.rows["valuation_price"].to_list() == [321.05]
    require_complete_holiday_c11(daily, physical, source)
    dates = np.array(["2026-02-11", "2026-02-23"], dtype="datetime64[D]")
    with pytest.raises(ValueError, match="complete source-bound"):
        _reject_unreviewed_holiday_valuations(daily, dates)
    _reject_unreviewed_holiday_valuations(daily, dates, source=source)
    # Admitting the existing source does not silently change a single mark.
    assert pl.read_parquet(daily).equals(physical)
    from stockagent.data.tw_futures_account_attachment import _reject_missing_holiday_account_dates
    with pytest.raises(ValueError, match="2026-02-13"):
        _reject_missing_holiday_account_dates(physical, dates)


@pytest.mark.parametrize("change", ["missing", "different_mark", "new_month", "ambiguous_generation"])
def test_coverage_checks_every_physical_row_and_never_fills_missing_marks(tmp_path, change):
    source, physical, daily, _ = _source(tmp_path)
    if change == "different_mark":
        physical = physical.with_columns(pl.lit(32000.).alias("valuation_price"))
    elif change == "missing":
        physical = physical.with_columns(pl.lit("TE").alias("product"))
    elif change == "new_month":
        physical = physical.with_columns(pl.lit("202604").alias("contract"))
    else:
        physical = pl.concat([physical, physical.with_columns(pl.lit("another-generation").alias("physical_instance"))])
    with pytest.raises(ValueError, match="incomplete|unambiguous"):
        require_complete_holiday_c11(daily, physical, source)


def test_rejects_source_drift_wrong_parent_and_mutated_decoded_rows(tmp_path):
    source, physical, daily, folder = _source(tmp_path)
    with pytest.raises(ValueError, match="another financial"):
        require_complete_holiday_c11(daily, physical, replace(source, financial_daily_sha256="f" * 64))
    source.rows.replace_column(source.rows.get_column_index("valuation_price"), pl.Series("valuation_price", [1.]))
    with pytest.raises(ValueError, match="decoded rows changed"):
        source.verify()
    (folder / "marks.raw").write_bytes(_c11(encoded=2))
    with pytest.raises(ValueError, match="hash/path"):
        load_holiday_c11(folder, workspace=tmp_path)


def test_manifest_cannot_backdate_an_original_or_substitute_a_future_catalog(tmp_path):
    _, _, _, folder = _source(tmp_path)
    path = folder / "manifest.json"
    manifest = json.loads(path.read_text())
    manifest["batches"][0]["account_date"] = "2026-02-13"
    path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="date|provenance"):
        load_holiday_c11(folder, workspace=tmp_path)
    manifest["batches"][0]["catalog_date"] = "2026-02-14"
    path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="future product catalog"):
        load_holiday_c11(folder, workspace=tmp_path)


def test_canonical_preparation_changes_account_marks_only_and_keeps_expiry_cash(tmp_path):
    source, account, _, _ = _source(tmp_path)
    prior = account.with_columns(pl.lit(date(2026, 2, 11)).alias("date"),
                                 pl.lit(False).alias("accounting_only_date"),
                                 pl.lit(30000.).alias("valuation_price"))
    terminal = account.with_columns(pl.lit("TE").alias("product"), pl.lit(True).alias("cash_settlement"),
                                    pl.lit(9876.).alias("valuation_price"))
    physical = pl.concat([prior, account.with_columns(pl.lit(30000.).alias("valuation_price")), terminal])
    updated = apply_holiday_c11_marks(physical, source)
    assert updated["valuation_price"].to_list() == [30000., 32105., 9876.]
    assert updated["holiday_valuation_contract"].to_list() == [None, HOLIDAY_C11_ABI, None]
    assert updated.drop("valuation_price", "holiday_valuation_contract", "holiday_valuation_source_fingerprint").equals(
        physical.drop("valuation_price"))
    incomplete = physical.with_columns(pl.lit(False).alias("cash_settlement"))
    with pytest.raises(ValueError, match="incomplete"):
        apply_holiday_c11_marks(incomplete, source)
    with pytest.raises(ValueError, match="conflicts with a market"):
        apply_holiday_c11_marks(account.with_columns(pl.lit(False).alias("accounting_only_date")), source)


def test_unbound_originals_can_prepare_a_new_release_without_future_file_hash(tmp_path):
    _, physical, daily, folder = _source(tmp_path)
    path = folder / "manifest.json"
    manifest = json.loads(path.read_text())
    manifest.pop("financial_daily_sha256")
    path.write_text(json.dumps(manifest))
    source = load_holiday_c11(folder, workspace=tmp_path)
    assert source.financial_daily_sha256 is None
    result = apply_holiday_c11_marks(physical, source)
    require_complete_holiday_c11(daily, result, source)

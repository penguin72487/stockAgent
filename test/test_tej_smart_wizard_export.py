from copy import deepcopy
from datetime import date
import json

import pytest

from scripts.verify_tej_smart_wizard_export import HEADERS, main, verify_export


@pytest.fixture
def sample():
    payload = {
        "contract_version": 1, "provider": "tej_smart_wizard",
        "extraction_method": "excel_com_value2", "date_system": "excel_1900",
        "observed_at_utc": "2026-10-01T10:00:00Z",
        "cells": [list(HEADERS), ["2330 TSMC", 41641, 105, 105.5, 103.5, 104.5, 15133, 1578860]],
    }
    preview = {"extraction_method": "msaa_preview_grid", "cells": deepcopy(payload["cells"])}
    preview["cells"][1][1] = "2014/01/02"
    return payload, preview


def verify(sample, dates=None):
    payload, preview = sample
    return verify_export(payload, preview, symbol="2330", expected_dates=dates or ["2014-01-02"])


def test_raw_prices_and_explicit_share_money_units(sample):
    report, rows = verify(sample)
    assert rows[0]["open_twd"] == "105"
    assert rows[0]["volume_shares"] == 15_133_000
    assert rows[0]["amount_twd"] == "1578860000"
    assert report["numeric_values"] == 6
    assert not any(report["limits"].values())


@pytest.mark.parametrize("value", [None, "", True, "NaN", "Infinity", -1])
def test_missing_invalid_numeric_cells_fail(sample, value):
    sample[0]["cells"][1][2] = value
    with pytest.raises(ValueError):
        verify(sample)


def test_wrong_unit_header_rejected(sample):
    sample[0]["cells"][0][6] = "Volume(shares)"
    with pytest.raises(ValueError, match="units"):
        verify(sample)


def test_log_field_not_accepted_as_raw_price(sample):
    sample[0]["cells"][0][2] = "ROI%-Ln"
    with pytest.raises(ValueError, match="schema"):
        verify(sample)


def test_duplicate_key_rejected(sample):
    sample[0]["cells"].append(deepcopy(sample[0]["cells"][1]))
    with pytest.raises(ValueError, match="duplicate export"):
        verify(sample)


def test_stale_or_partial_export_rejected(sample):
    with pytest.raises(ValueError, match="planned date"):
        verify(sample, ["2014-01-02", "2014-01-03"])


def test_preview_mismatch_rejected(sample):
    sample[1]["cells"][1][5] = 104
    with pytest.raises(ValueError, match="mismatch"):
        verify(sample)


@pytest.mark.parametrize("symbol", ["2317 TSMC", None, "", True])
def test_wrong_symbol_rejected(sample, symbol):
    sample[0]["cells"][1][0] = symbol
    with pytest.raises(ValueError):
        verify(sample)


def test_ohlc_inconsistent_rejected(sample):
    sample[0]["cells"][1][3] = 100
    with pytest.raises(ValueError, match="OHLC"):
        verify(sample)


@pytest.mark.parametrize("value", [41641.5, 60, True, "Infinity"])
def test_bad_excel_serial_rejected(sample, value):
    sample[0]["cells"][1][1] = value
    with pytest.raises(ValueError):
        verify(sample)


def test_1904_date_system(sample):
    sample[0]["date_system"] = "excel_1904"
    sample[0]["cells"][1][1] = 41641 - (date(1904, 1, 1) - date(1899, 12, 30)).days
    _, rows = verify(sample)
    assert rows[0]["date"] == "2014-01-02"


def test_fractional_thousand_shares_are_not_rounded(sample):
    for obj in sample:
        obj["cells"][1][6] = "0.001"
        obj["cells"][1][7] = "0.1045"
    _, rows = verify(sample)
    assert rows[0]["volume_shares"] == 1
    assert rows[0]["amount_twd"] == "104.5000"


def test_fractional_share_after_conversion_rejected(sample):
    sample[0]["cells"][1][6] = "0.0001"
    with pytest.raises(ValueError, match="volume"):
        verify(sample)


def test_cli_does_not_replace_prior_evidence(sample, tmp_path):
    payload, preview = sample
    inp, pre, out = tmp_path / "input.json", tmp_path / "preview.json", tmp_path / "verified"
    inp.write_text(json.dumps(payload))
    pre.write_text(json.dumps(preview))
    argv = ["--input", str(inp), "--preview", str(pre), "--symbol", "2330",
            "--dates", "2014-01-02", "--output-dir", str(out)]
    assert main(argv) == 0
    before = (out / "verification.json").read_bytes()
    assert main(argv) == 1
    assert (out / "verification.json").read_bytes() == before


def test_failed_verification_does_not_create_output(sample, tmp_path):
    payload, preview = sample
    payload["cells"][1][2] = None
    inp, pre, out = tmp_path / "input.json", tmp_path / "preview.json", tmp_path / "verified"
    inp.write_text(json.dumps(payload))
    pre.write_text(json.dumps(preview))
    assert main(["--input", str(inp), "--preview", str(pre), "--symbol", "2330",
                 "--dates", "2014-01-02", "--output-dir", str(out)]) == 1
    assert not out.exists()

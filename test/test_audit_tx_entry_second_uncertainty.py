from copy import deepcopy

import pytest

from scripts.audit_tx_entry_second_uncertainty import summarize


def parent():
    return {"contract": "tx_retained_finmind_contract_day_clock_audit_v2",
            "benchmark_admitted": False, "training_admitted": False,
            "counts": {"target_contract_days": 1, "clock_probes": 2,
                       "same_second_multi_price_probes": 1},
            "rows": [{"date": "2011-01-03", "product": "TX", "contract_month": "201101",
                      "role": "year_entry", "valid_tick_values": True, "full_book_matches": True,
                      "entry_probes": [
                          {"after": "08:45:00", "observed": True, "distinct_prices": [100., 102.],
                           "same_second_price_unambiguous": False},
                          {"after": "09:00:00", "observed": True, "distinct_prices": [105.],
                           "same_second_price_unambiguous": True}]}]}


def test_observed_range_is_not_a_fill_or_training_admission():
    out = summarize(parent())
    assert out["probe_count"] == 2 and out["multi_price_probe_count"] == 1
    assert out["groups"][0]["max_observed_range_twd"] == 400
    assert not out["benchmark_admitted"] and not out["training_admitted"]
    assert any("not a slippage" in text for text in out["limitations"])


@pytest.mark.parametrize("mutation", ["duplicate", "bad_price", "bad_count", "unverified"])
def test_incomplete_or_inconsistent_parent_is_rejected(mutation):
    doc = deepcopy(parent())
    if mutation == "duplicate":
        doc["rows"][0]["entry_probes"][1]["after"] = "08:45:00"
    elif mutation == "bad_price":
        doc["rows"][0]["entry_probes"][0]["distinct_prices"] = [100., float("nan")]
    elif mutation == "bad_count":
        doc["counts"]["same_second_multi_price_probes"] = 0
    else:
        doc["rows"][0]["full_book_matches"] = False
    with pytest.raises(ValueError):
        summarize(doc)

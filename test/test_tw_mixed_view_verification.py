from dataclasses import asdict
from datetime import date
import json

import polars as pl
import pytest

from scripts.prepare_tw_day_trade_feature_catalog import sha256, write_csv
from scripts.verify_tw_day_trade_mixed_view import verify, channel_checks
from stockagent.data.tw_day_trade_mixed_frequency import CONTRACT
from stockagent.data.tw_public_release_schedule import RULES


def fixture_view(tmp_path, *, duplicate=False, invalid=False, count=1):
    view=tmp_path/"view"
    view.mkdir()
    days=[date(2014,1,3), date(2014,1,6)]
    if duplicate:
        days[0]=days[1]
    frame=pl.DataFrame({"date":days, "symbol":["2330"]*2,
        "daily":[0., None], "daily__available":[0. if invalid else 1., None],
        "daily__age_days":[0., None], "daily__updated":[1., None],
        "_twpub_rule":[1., 1.]}).with_columns(
            pl.col("daily", "daily__available", "daily__age_days", "daily__updated").cast(pl.Float32))
    frame.write_parquet(view/"model_inputs.parquet")
    (view/"training.yaml").write_text("test-only-config: true\n")
    write_csv(view/"feature_dictionary.csv", [{"feature":"daily", "rule":json.dumps(asdict(RULES["daily"]))}])
    proof={"contract":CONTRACT,"feature_view_ready":True, "rows":2,
        "matrix_sha256":sha256(view/"model_inputs.parquet"), "config_sha256":sha256(view/"training.yaml"),
        "value_features":1, "feature_available_cells":{"daily":count}, "model_channels":10,
        "source_snapshot_id":"pinned-test", "source_manifest_sha256":"test-proof",
        "last_decision_session":"2014-01-06"}
    (view/"dataset_manifest.json").write_text(json.dumps(proof))
    return view


def test_zero_and_null_barriers_have_distinct_valid_state_channels():
    frame=pl.DataFrame({"x":[0.,None,None,1.,None],"x__available":[1.,0.,None,0.,1.],
        "x__age_days":[0.,None,None,0.,0.],"x__updated":[1.,1.,None,1.,1.]})
    result=frame.select(channel_checks("x",0)).row(0,named=True)
    assert result["x__invalid_cells"] == 2
    assert result["x__available_sum"] == 2


def test_columnar_acceptance_is_not_training_or_pit_acceptance(tmp_path):
    view=fixture_view(tmp_path)
    result=verify(view,tmp_path/"acceptance.json")
    assert result["economic_keys_unique"] and result["feature_available_cells_verified"]
    assert result["status"] == "columnar_feature_invariants_accepted"
    assert not result["training_ready"] and not result["gpu_ddp_verified"]
    assert not result["historical_point_in_time"]
    with pytest.raises(FileExistsError):
        verify(view,tmp_path/"acceptance.json")


@pytest.mark.parametrize("case,message",[("duplicate","duplicate matrix"),("invalid","channel invariant"),("count","availability mismatch")])
def test_reject_duplicate_keys_invalid_state_and_wrong_counts(tmp_path,case,message):
    view=fixture_view(tmp_path,duplicate=case=="duplicate",invalid=case=="invalid",count=0 if case=="count" else 1)
    with pytest.raises(ValueError,match=message):
        verify(view,tmp_path/"failed.json")
    assert not (tmp_path/"failed.json").exists()

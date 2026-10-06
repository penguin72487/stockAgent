from dataclasses import asdict
from datetime import date
import json

import numpy as np
import polars as pl

from scripts.prepare_tw_day_trade_factorized_panel import causal_state_blocks
from scripts.repair_tw_day_trade_factorized_common import repair
from stockagent.data.factorized_panel import CONTRACT,file_sha256,verify_factorized_members,write_array_block
from stockagent.data.tw_day_trade_mixed_frequency import public_spec
from stockagent.data.tw_public_release_schedule import RULES


def test_incremental_common_repair_preserves_raw_and_individual_bytes(tmp_path):
    parent=tmp_path/"parent";parent.mkdir();(parent/"annual_events").mkdir();(parent/"observations").mkdir()
    dates=[date(2025,1,d) for d in (6,7,8,9,10)]
    low=public_spec("twpub_cbc_fx_reserves_usd_billion_raw")
    native={"feature":"native_weekly","source":"fred_events","rule":{**asdict(RULES["weekly"]),"scope":"market"}}
    own={"feature":"own","source":"FinLab","rule":asdict(RULES["quarter"])}
    raw=pl.DataFrame({"date":[dates[1],dates[2],dates[3],dates[4],dates[1]],
        "symbol":["*"]*5,"feature":[low["feature"]]*4+["native_weekly"],
        "value":[10.,None,None,None,4.],"period":[str(d) for d in (dates[1],dates[2],dates[3],dates[4],dates[1])]})
    observation=parent/"observations/0.parquet";raw.write_parquet(observation)
    annual=parent/"annual_events/2025.parquet"
    raw.with_columns(pl.lit(0,dtype=pl.Int32).alias("_source_part")).write_parquet(annual)
    old=np.concatenate([v for _,v,_ in causal_state_blocks(dates,["*"],[low,native],raw,None)])[:,0,:]
    np.save(parent/"common.npy",old,allow_pickle=False)
    block=write_array_block(parent/"stock.npy.zst",np.zeros((5,1,4),dtype=np.float32))
    dictionary=parent/"feature_dictionary.json"
    dictionary.write_text(json.dumps({"features":[own,low,native],"aliases":[],"excluded":[]}))
    coverage=parent/"feature_coverage.csv"
    coverage.write_text("feature,scope,source,available_panel_cells\nown,stock,FinLab,0\n"+
        low["feature"]+",market,tw-public,1\nnative_weekly,market,fred_events,4\n")
    channels=lambda name:[name,name+"__available",name+"__age_days",name+"__updated"]
    manifest={"contract":CONTRACT,"status":"complete","source_snapshot_id":"exact-fixture",
        "dates":[str(d) for d in dates],"symbols":["2330"],"base_feature_names":[],
        "individual_quantities":1,"shared_quantities":2,"logical_model_channels":12,
        "individual_channels":channels("own"),"common_channels":channels(low["feature"])+channels("native_weekly"),
        "common":{"path":"common.npy","sha256":file_sha256(parent/"common.npy")},
        "blocks":[{**block,"start":0}],"feature_dictionary_sha256":file_sha256(dictionary),
        "observations":[{"path":"observations/0.parquet","sha256":file_sha256(observation)}],
        "annual_events":[{"path":"annual_events/2025.parquet","sha256":file_sha256(annual)}]}
    path=parent/"factorized_manifest.json";path.write_text(json.dumps(manifest));before=file_sha256(path)
    out=tmp_path/"repaired"
    receipt=repair(path,out)
    assert file_sha256(path)==before and file_sha256(out/"observations/0.parquet")==file_sha256(observation)
    assert (out/"stock.npy.zst").stat().st_ino==(parent/"stock.npy.zst").stat().st_ino
    new=np.load(out/"common.npy")
    np.testing.assert_array_equal(new[:4,1],np.ones(4))
    np.testing.assert_array_equal(new[:4,2],[0,1,2,3])
    np.testing.assert_array_equal(new[:,4:].view(np.uint32),old[:,4:].view(np.uint32))
    assert pl.read_parquet(out/"observations/0.parquet")["value"].null_count()==3
    assert receipt["changed_common"][0]["available_dates_before"]==1
    assert receipt["changed_common"][0]["available_dates_after"]==4
    verify_factorized_members(out,json.loads((out/"factorized_manifest.json").read_text()))

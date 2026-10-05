from dataclasses import asdict
from datetime import date

import numpy as np
import polars as pl
import pytest

from scripts.prepare_tw_day_trade_factorized_panel import causal_state_blocks, quarter_clock, economic_primary, global_rule, first_ready_session
from stockagent.data.tw_public_release_schedule import RULES, rule_for, schedule_lookup
from stockagent.data.tw_native_panel_features import collapse_conflicting_observations, taxonomy_key


def spec(name,rule="quarter",group=None):
    return {"feature":name,"rule":asdict(RULES[rule]),"report_group":group}


def test_nullable_source_and_causal_age_zero_lifecycle_and_report_reset():
    dates=[date(2025,1,d) for d in [6,7,8,9,10]]
    definitions=[spec("a",group="report"),spec("b",group="report"),spec("daily","daily")]
    raw=pl.DataFrame({"date":[date(2024,12,31),date(2024,12,31),dates[1],dates[2],dates[2]],
        "symbol":["2330"]*5,"feature":["a","b","daily","a","daily"],"value":[0.,9.,7.,None,8.]})
    before=raw.clone()
    resets=pl.DataFrame({"date":[dates[2]],"symbol":["2330"],"report_group":["report"]})
    lifecycle=np.full((5,1),date(2024,1,1).toordinal(),dtype=np.int32)
    # On day 9 the code denotes a new lifecycle: no predecessor state.
    lifecycle[3:]=dates[3].toordinal()
    slabs=[v for _,v,_ in causal_state_blocks(dates,["2330"],definitions,raw,lifecycle,resets=resets,max_block_bytes=48)]
    x=np.concatenate(slabs,axis=0).reshape(5,1,3,4)
    assert raw.equals(before) and raw["value"].null_count()==1
    assert x[0,0,0].tolist()==[0.,1.,7.,0.] # reported zero is not missing
    assert x[0,0,1].tolist()==[9.,1.,7.,0.]
    assert x[0,0,2].tolist()==[7.,1.,0.,1.]
    assert x[1,0,0].tolist()==[0.,0.,0.,1.] # invalid new observation is barrier
    assert x[1,0,1].tolist()==[0.,0.,0.,0.] # omitted new-report account cleared
    assert x[2,0,2].tolist()==[0.,0.,0.,0.] # daily has no gap carry
    assert not x[-1].any() # no hypothetical future session


def test_weekly_policy_change_and_daily_are_not_blanket_forward_fill():
    definitions=[spec("weekly","weekly"),spec("daily","daily")]
    dates=[date(2015,4,29),date(2015,4,30),date(2015,5,4),date(2015,5,5)]
    raw=pl.DataFrame({"date":[date(2015,4,1)]*2,"symbol":["2330"]*2,"feature":["weekly","daily"],"value":[5.,5.]})
    x=np.concatenate([v for _,v,_ in causal_state_blocks(dates,["2330"],definitions,raw,None)]).reshape(4,1,2,4)
    assert x[0,0,0,1]==1 and x[0,0,0,2]==29
    assert x[1,0,0,1]==0 # weekly transition stops old 62d carry
    assert not x[:,:,1].any()


def test_compact_state_blocks_reconstruct_identical_bits_and_cumulative_counts(tmp_path):
    from datetime import timedelta
    from stockagent.data.factorized_panel import write_array_block
    import io
    import pyarrow as pa

    dates=[date(2025,1,1)+timedelta(days=i) for i in range(43)]
    symbols=[str(i) for i in range(5)]
    definitions=[spec("f"+str(i),"daily" if i%3==0 else "quarter",group="r" if i%2 else None) for i in range(17)]
    raw=pl.DataFrame({"date":[dates[1],dates[1],dates[3],dates[9],dates[21]],
        "symbol":["0","4","0","3","0"],"feature":["f1","f2","f1","f16","f1"],
        "value":[-0.,100000001.,None,7.,9.]})
    resets=pl.DataFrame({"date":[dates[9]],"symbol":["0"],"report_group":["r"]})
    lifecycle=np.full((len(dates),len(symbols)),dates[0].toordinal(),dtype=np.int32)
    lifecycle[30:,3]=dates[30].toordinal()
    options=dict(resets=resets,max_block_bytes=5000)
    ordinary=list(causal_state_blocks(dates,symbols,definitions,raw,lifecycle,**options))
    compact=list(causal_state_blocks(dates,symbols,definitions,raw,lifecycle,**options,
        column_blocks=True,symbol_block_size=2))
    expected=np.concatenate([v for _,v,_ in ordinary])
    actual=np.zeros_like(expected)
    for start,block,_ in compact:
        for stock_start,columns,values in block.symbol_blocks:
            path=tmp_path/f"{start}_{stock_start}.npy.zst"
            proof=write_array_block(path,values,omit_zero_columns=True,logical_columns=columns,
                logical_shape=(block.shape[0],values.shape[1],block.shape[2]))
            encoded=pa.Codec("zstd").decompress(path.read_bytes(),proof["serialized_bytes"])
            data=np.load(io.BytesIO(encoded),allow_pickle=False)
            actual[start:start+block.shape[0],stock_start:stock_start+values.shape[1],data["columns"]]=data["values"]
            assert proof["uncompressed_bytes"]==block.shape[0]*values.shape[1]*block.shape[2]*4
    np.testing.assert_array_equal(actual.view(np.uint32),expected.view(np.uint32))
    np.testing.assert_array_equal(compact[-1][2],ordinary[-1][2])
    assert len(compact)<len(ordinary)


def test_known_upload_after_calendar_never_falls_back_to_guessed_deadline():
    sessions=[date(2025,5,d) for d in [14,15,16,19]]
    raw=pl.DataFrame({"period":["2025Q1"]*2,"symbol":["2330","2317"],"value":[1.,2.]})
    uploads=pl.DataFrame({"source_index":["2025-Q1"]*2,"symbol":["2330","2317"],"known_upload_on":[date(2025,5,14),date(2025,5,20)]})
    result=quarter_clock(raw,sessions,uploads)
    assert result.select("symbol","date").rows()==[("2330",date(2025,5,15))]


def test_taxonomy_alias_requires_definition_proof_and_keeps_unit_grain():
    concept="{http://www.xbrl.org/tifrs/bsci/basi/2014-03-31}Loans"
    assert taxonomy_key(concept)==concept
    dictionary={"aliases":{concept:{"semantic_key":"verified_same_schema_label"}}}
    assert taxonomy_key(concept,dictionary)=="verified_same_schema_label"
    conflicting=pl.DataFrame({"date":[date(2025,1,1)]*3,"symbol":["2330"]*3,"feature":["a","a","b"],"value":[1.,2.,0.]})
    result,count=collapse_conflicting_observations(conflicting,["date","symbol","feature"])
    assert count==1
    assert result.filter(pl.col("feature")=="a")["value"][0] is None
    assert result.filter(pl.col("feature")=="b")["value"][0]==0.


def test_native_finlab_quarter_and_inventory_rules_are_explicit():
    assert rule_for("fundamental_features:負債比率").kind=="quarter"
    rule=rule_for("financial_statement_revised:總資產")
    assert rule.kind=="quarter_date"
    sessions=[date(2025,11,d) for d in [14,17,18]]
    lookup=schedule_lookup(["2025-11-15 00:00:00"],rule,sessions)
    assert lookup["date"][0]==date(2025,11,18)
    assert rule_for("etl:inventory:小於一千張股數").kind=="weekly"


def test_late_old_report_cannot_reset_or_overwrite_newer_current_state():
    dates=[date(2025,8,d) for d in [14,15,18,19]]
    definitions=[spec("a",group="report"),spec("b",group="report")]
    raw=pl.DataFrame({"date":[dates[1],dates[1],dates[2]],"symbol":["2330"]*3,
        "feature":["a","b","a"],"period":["2025Q2","2025Q2","2025Q1"],"value":[10.,20.,1.]})
    resets=raw.select("date","symbol","period").unique().with_columns(pl.lit("report").alias("report_group"))
    x=np.concatenate([v for _,v,_ in causal_state_blocks(dates,["2330"],definitions,raw,None,resets=resets)]).reshape(4,1,2,4)
    assert x[1,0,0,0]==10. and x[1,0,1,0]==20.
    assert x[1,0,0,2]==3. # age of Q2 release, not the late Q1 revision


def test_missing_only_primary_union_and_fred_weekly_cadence():
    import pytest
    frame=pl.DataFrame({"period":["2024Q1"]*2,"symbol":["2330"]*2,"value":[None,0.]})
    assert economic_primary(frame).rows()==[("2024Q1","2330",0.)]
    with pytest.raises(ValueError,match="conflicting primary"):
        economic_primary(frame.with_columns(pl.Series("value",[1.,2.])))
    assert global_rule("fred_events",{"series_id":"WALCL"}).carry_days==14
    assert global_rule("fred_events",{"series_id":"NFCI"}).cadence_change_on==""
    assert global_rule("fred_events",{"series_id":"DGS10"}).carry_days==0


def test_native_availability_does_not_receive_a_second_safety_day():
    sessions=[date(2025,1,d) for d in [6,7,8]]
    assert first_ready_session("2025-01-06T00:00:00Z",sessions)==sessions[0]
    assert first_ready_session("2025-01-06T01:00:00Z",sessions)==sessions[1]
    assert first_ready_session("2025-01-04T00:00:00Z",sessions)==sessions[0]
    assert first_ready_session("2025-01-08T05:00:00Z",sessions) is None


def test_native_macro_uses_publication_boundary_not_stale_same_close_session():
    from scripts.prepare_tw_day_trade_factorized_panel import macro_preopen_sessions
    sessions = [date(2026, 10, 1), date(2026, 10, 2)]
    raw = pl.DataFrame({'published_at_taipei': ['2026-10-01T17:00:00', '2026-10-02T08:59:59',
                                              '2026-10-02T09:00:00', '2026-10-02T18:00:00'],
        'effective_session': [sessions[0], sessions[1], sessions[1], sessions[1]]})
    result = macro_preopen_sessions(raw, sessions)
    assert result['_preopen_session'].to_list() == [sessions[1], sessions[1], None, None]
    assert result.select(raw.columns).equals(raw)  # Raw clocks and NULLs are not rewritten.


def test_empty_new_report_clears_old_state_and_later_old_revision_stays_masked():
    dates=[date(2025,8,d) for d in [14,15,18,19]]
    definitions=[spec("a",group="report")]
    raw=pl.DataFrame({"date":[dates[0],dates[2]],"symbol":["2330"]*2,
        "feature":["a"]*2,"period":["2025Q1"]*2,"value":[10.,99.]})
    resets=pl.DataFrame({"date":[dates[1]],"symbol":["2330"],"report_group":["report"],"period":["2025Q2"]})
    x=np.concatenate([v for _,v,_ in causal_state_blocks(dates,["2330"],definitions,raw,None,resets=resets)]).reshape(4,1,1,4)
    assert not x.any()


def test_public_calendar_nulls_are_not_releases_but_native_nulls_still_clear_state():
    import json
    from stockagent.data.tw_day_trade_mixed_frequency import public_coordinate_null_spec,public_spec
    days=[date(2025,1,d) for d in (6,7,8,9,10)]
    public=public_coordinate_null_spec(json.loads(json.dumps(public_spec("twpub_cbc_fx_reserves_usd_billion_raw"))))
    native=spec("native",group="release")
    raw=pl.DataFrame({"date":[days[1],days[2],days[3],days[4],days[1],days[3]],
        "symbol":["*"]*6,"feature":[public["feature"]]*4+["native"]*2,
        "value":[0.,None,None,None,5.,None],"period":[str(d) for d in (days[1],days[2],days[3],days[4],days[1],days[3])]})
    original=raw.clone()
    x=np.concatenate([v for _,v,_ in causal_state_blocks(days,["*"],[public,native],raw,None)]).reshape(5,1,2,4)
    np.testing.assert_array_equal(x[:4,0,0,1],[1,1,1,1])
    np.testing.assert_array_equal(x[:4,0,0,2],[0,1,2,3])
    np.testing.assert_array_equal(x[:4,0,0,3],[1,0,0,0])
    assert not x[2:,0,1,1].any() # a genuine new native NULL remains a barrier
    assert raw.equals(original) and raw["value"].null_count()==4
    assert not x[-1].any() # no imagined next decision


def test_coordinate_null_policy_cannot_escape_native_or_changed_clock_scope():
    from stockagent.data.tw_day_trade_mixed_frequency import PUBLIC_COORDINATE_NULL_POLICY,public_coordinate_null_spec,public_spec
    native={**spec("native"),"source":"FinLab","null_event_policy":PUBLIC_COORDINATE_NULL_POLICY}
    with pytest.raises(ValueError,match="escaped"):
        public_coordinate_null_spec(native)
    shared=public_spec("twpub_cbc_fx_reserves_usd_billion_raw")
    shared["clock"]="completed_session"
    with pytest.raises(ValueError,match="exact registered"):
        public_coordinate_null_spec(shared)


@pytest.mark.parametrize("native", [False, True])
@pytest.mark.parametrize("channel_policy", ["value_available_age_updated", "value_only"])
def test_complete_synthetic_prepare_raw_null_shared_axis_and_markdown(tmp_path,monkeypatch,native,channel_policy):
    import json
    from dataclasses import replace
    from pathlib import Path
    import scripts.prepare_tw_day_trade_factorized_panel as builder
    from stockagent.data.factorized_panel import attach_factorized_features,file_sha256
    from stockagent.data.panel import PanelData
    from stockagent.data.tw_day_trade_mixed_frequency import CONTRACT,PRIVATE_USE
    source=tmp_path/"source";source.mkdir();(source/"stocks").mkdir();(source/"features").mkdir()
    dates=[date(y,1,6) for y in range(2014,2027)];symbols=["2330","2317"]
    for symbol in symbols:
        pl.DataFrame({"date":dates,"lifecycle_episode_id":["issuer-1"]*len(dates)}).write_parquet(source/"stocks"/f"{symbol}_features.parquet")
    pl.DataFrame({"date":dates}).write_parquet(source/"twse_taiex_ohlc.parquet")
    public=pl.DataFrame([{"date":day,"symbol":symbol,"_twpub_test_rule":True,
        "twpub_individual_raw":None if i==2 else float(i),"twpub_market_raw":float(i)}
        for i,day in enumerate(dates) for symbol in symbols])
    public.write_parquet(source/"features/tw_public_stock_daily.parquet")
    specs=[{"feature":"twcad_"+name,"source_column":"twpub_"+name,"source":"tw-public",
        "clock":"available_session","rule":asdict(replace(RULES["daily"],scope=scope))}
        for name,scope in [("individual_raw","stock"),("market_raw","market")]]
    adapters=[]
    if native:
        (source/"native").mkdir()
        pl.DataFrame({"date":["2014-03-31","2014-06-30"],"stock_id":["2330"]*2,
            "type":["OperatingExpense"]*2,"origin_name":["營業費用"]*2,
            "value":[0.,None]}).write_parquet(source/"native/financial.parquet")
        pl.DataFrame({"series_id":["WALCL"],"observation_date":["2015-01-01"],
            "available_at_utc":["2015-01-05T23:00:00Z"],"value":[2.]}).write_parquet(source/"native/fred.parquet")
        adapters=[{"kind":"finmind_financial_facts","members":[{
            "dataset":"TaiwanStockFinancialStatements","path":"native/financial.parquet"}]},
            {"kind":"fred_events","path":"native/fred.parquet"}]
    manifest={"contract":CONTRACT,"source_only":True,"private_delivery_authorized":True,
        "use_restriction":PRIVATE_USE,"end_date":"2026-10-02","feature_specs":specs,
        "native_adapters":adapters,
        "files":{str(p.relative_to(source)):{"sha256":file_sha256(p)} for p in source.rglob("*.parquet")}}
    (source/"source_manifest.json").write_text(json.dumps(manifest))
    n=len(dates);mask=np.ones((n,2),dtype=bool);base=np.ones((n,2,6),dtype=np.float32)
    panel=PanelData(np.array(dates,dtype="datetime64[D]"),symbols,builder.BASE_FEATURES,base,
        np.zeros((n,2),dtype=np.float32),mask,mask,np.zeros(n,dtype=np.float32),np.ones((n,2),dtype=np.float32))
    monkeypatch.setattr(builder,"build_panel",lambda *a,**kw:panel)
    monkeypatch.setattr(builder,"_build_panel_kwargs",lambda config:{})
    config=Path("configs/deployments/tw_day_trade_last_last_only_training_vastai1t_v10_research_143_no_bottleneck.yaml")
    out=tmp_path/"prepared"
    result=builder.prepare(source,out,config,tmp_path/"minutes","pinned-synthetic-release",max_block_bytes=128,
        model_channel_policy=channel_policy)
    assert result["individual_quantities"]==1+int(native) and result["shared_quantities"]==1+int(native)
    channels = 1 if channel_policy == "value_only" else 4
    expected_width = 6 + channels * (2 + 2 * int(native))
    assert result["logical_model_channels"]==expected_width and result["training_ready"] is False
    raw=pl.concat([pl.read_parquet(p) for p in (out/"observations").glob("*.parquet")])
    assert raw["value"].null_count()==2+int(native) # source NULLs preserved, not imputed observations
    assert (out/"feature_report/index.md").is_file()
    attached=attach_factorized_features(replace(panel),out/"factorized_manifest.json")
    assert attached.features.shape==(n,2,expected_width)
    values=attached.features[1:3]
    market=attached.feature_names.index("twcad_market_raw")
    if channel_policy == "value_only":
        assert not any(name.endswith(("__available", "__age_days", "__updated")) for name in attached.feature_names)
        assert "不輸入可用性" in (out/"feature_report/stock_0001.md").read_text()
    else:
        missing=attached.feature_names.index("twcad_individual_raw__available")
        assert values[0,0,missing]==0. # invalid release is unavailable
        assert values[0,0,market+1]==1.
    assert values[0,1,market]==values[0,0,market]

import json

import pytest

from scripts.prepare_tw_day_trade_feature_catalog import sha256
from scripts.stage_tw_day_trade_physical_companions import stage
from scripts.stage_tw_public_research_release import FORMAL_MEMBERS
from stockagent.data.tw_day_trade_carry_source import PHYSICAL_PUBLIC_RELATIVE_MEMBERS
from stockagent.data.tw_day_trade_mixed_frequency import CONTRACT,PRIVATE_USE


def fixture(tmp_path):
    pinned,public=tmp_path/"pinned",tmp_path/"public"
    pinned.mkdir();public.mkdir()
    members=(*FORMAL_MEMBERS,"raw/exact.jsonl")
    for root,names in [(pinned,members),(public,PHYSICAL_PUBLIC_RELATIVE_MEMBERS)]:
        for name in names:
            path=root/name;path.parent.mkdir(parents=True,exist_ok=True)
            path.write_bytes(("exact-"+name).encode())
    (pinned/"tw_corporate_action_entitlements.summary.json").write_text(json.dumps({
        "raw_receipt_manifest":{"relative_path":"raw/exact.jsonl"}}))
    files={name:{"sha256":sha256(pinned/name)} for name in members}
    (pinned/"source_manifest.json").write_text(json.dumps({"contract":CONTRACT,
        "source_only":True,"private_delivery_authorized":True,"use_restriction":PRIVATE_USE,
        "files":files,"end_date":"2025-01-02"}))
    grant=tmp_path/"grant.json";grant.write_text(json.dumps({"private_vastai1T_delivery_authorized":True}))
    return pinned,public,grant


def test_supplement_binds_same_features_and_all_consumer_dependencies(tmp_path):
    pinned,public,grant=fixture(tmp_path);out=tmp_path/"new"
    original=sha256(pinned/"features/tw_public_stock_daily.parquet")
    result=stage(pinned,public,out,grant)
    assert result["model_observations_changed"] is False
    assert sha256(out/"features/tw_public_stock_daily.parquet")==original
    assert all((out/name).is_file() for name in PHYSICAL_PUBLIC_RELATIVE_MEMBERS)
    manifest=json.loads((out/"source_manifest.json").read_text())
    assert manifest["files"]["twse_daily_ohlcv.parquet"]["pinned_original_member"] is False
    with pytest.raises(FileExistsError):stage(pinned,public,out,grant)


def test_missing_physical_dependency_fails_before_partial_source(tmp_path):
    pinned,public,grant=fixture(tmp_path)
    (public/"tpex_daily_ohlcv.parquet").unlink()
    with pytest.raises(FileNotFoundError):stage(pinned,public,tmp_path/"new",grant)
    assert not (tmp_path/"new").exists()

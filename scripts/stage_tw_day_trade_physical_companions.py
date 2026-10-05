#!/usr/bin/env python3
"""Incrementally stage omitted physical inputs without rebuilding model data."""
from __future__ import annotations

import argparse
import fcntl
import json
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from downloader.artifact_io import atomic_write_json
from scripts.prepare_tw_day_trade_feature_catalog import sha256
from scripts.prepare_tw_day_trade_mixed_frequency import verify_sources
from scripts.stage_tw_public_research_release import _required_formal_members, _stage_copy, LOCK
from stockagent.data.tw_day_trade_carry_source import PHYSICAL_PUBLIC_RELATIVE_MEMBERS, _public_root
from stockagent.data.tw_day_trade_mixed_frequency import CONTRACT, PRIVATE_USE


def stage(pinned_source: Path,public_root: Path,output_root: Path,authorization: Path):
    grant=json.loads(authorization.read_text())
    if grant.get("private_vastai1T_delivery_authorized") is not True:
        raise ValueError("explicit private delivery authorization required")
    pinned=json.loads((pinned_source/"source_manifest.json").read_text())
    pinned_sha=sha256(pinned_source/"source_manifest.json")
    _public_root(public_root/"features/tw_public_stock_daily.parquet")
    if output_root.exists():raise FileExistsError("physical supplement requires a fresh source root")
    output_root.mkdir(parents=True,mode=0o700)
    entitlement=json.loads((pinned_source/"tw_corporate_action_entitlements.summary.json").read_text())
    members=sorted(set(PHYSICAL_PUBLIC_RELATIVE_MEMBERS)|set(_required_formal_members(entitlement,pinned_source)))
    files={}
    for relative in members:
        proof=pinned["files"].get(relative)
        original=pinned_source/relative if proof else public_root/relative
        digest=sha256(original)
        if proof and digest!=proof["sha256"]:raise ValueError("pinned physical companion changed")
        _stage_copy(original,output_root/relative,digest)
        if sha256(original)!=digest:raise ValueError("physical source changed during staging")
        files[relative]={"sha256":digest,"bytes":(output_root/relative).stat().st_size,
            "original_source":str(original),"pinned_original_member":bool(proof)}
    _public_root(output_root/"features/tw_public_stock_daily.parquet")
    result={"contract":CONTRACT,"status":"complete","source_only":True,
        "private_personal_research":True,"private_delivery_authorized":True,
        "research_only":True,"historical_point_in_time":False,"live_eligible":False,
        "training_ready":False,"use_restriction":PRIVATE_USE,"end_date":pinned["end_date"],
        "files":files,"authorization_sha256":sha256(authorization),
        "pinned_model_source_manifest_sha256":pinned_sha,
        "model_observations_changed":False,"source_bytes":sum(p["bytes"] for p in files.values()),
        "purpose":"canonical physical FIFO dependencies; no new model input or fitted cache"}
    if sha256(pinned_source/"source_manifest.json")!=pinned_sha:raise ValueError("pinned source changed")
    atomic_write_json(output_root/"source_manifest.json",result)
    verify_sources(output_root)
    return {k:v for k,v in result.items() if k!="files"}


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--pinned-source",type=Path,required=True)
    p.add_argument("--public-root",type=Path,required=True)
    p.add_argument("--output-root",type=Path,required=True)
    p.add_argument("--authorization",type=Path,required=True)
    a=p.parse_args()
    LOCK.parent.mkdir(parents=True,exist_ok=True)
    with LOCK.open("a+") as lock:
        fcntl.flock(lock.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
        print(json.dumps(stage(a.pinned_source,a.public_root,a.output_root,a.authorization),ensure_ascii=False))


if __name__=="__main__":main()

#!/usr/bin/env python3
"""Repair proven public calendar NULLs without rebuilding stock feature blocks.

Raw NULL coordinates stay immutable. Verified members are hard-linked into a
fresh root, only shared training state and its report/fingerprint are rebuilt.
"""
from __future__ import annotations

import argparse
import copy
from datetime import date
import json
import os
from pathlib import Path
import sys
import time

import numpy as np
import polars as pl

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from downloader.artifact_io import atomic_write_json
from scripts.prepare_tw_day_trade_factorized_panel import AnnualObservationEvents, causal_state_blocks
from scripts.prepare_tw_day_trade_feature_catalog import write_csv
from stockagent.data.factorized_panel import CONTRACT, factorized_member_proofs, file_sha256, verify_factorized_members
from stockagent.data.tw_day_trade_mixed_frequency import PUBLIC_COORDINATE_NULL_POLICY, public_coordinate_null_spec, rule_from_spec
from stockagent.data.tw_feature_semantic_report import write_factorized_feature_report

REPAIR_CONTRACT="tw_factorized_public_common_coordinate_null_repair_v2"


def repair(manifest_path: Path, output: Path, *, report: bool=True) -> dict:
    started=time.perf_counter()
    manifest_path=manifest_path.resolve(strict=True);parent=manifest_path.parent
    before=file_sha256(manifest_path)
    manifest=json.loads(manifest_path.read_text())
    if manifest.get("contract")!=CONTRACT or manifest.get("status")!="complete":
        raise ValueError("complete canonical factorized parent required")
    verify_factorized_members(parent,manifest)
    dictionary=json.loads((parent/"feature_dictionary.json").read_text())
    revised=copy.deepcopy(dictionary);affected=[]
    for i,definition in enumerate(dictionary["features"]):
        revised["features"][i]=public_coordinate_null_spec(definition)
        if revised["features"][i]!=definition:
            affected.append(definition["feature"])
    if not affected:raise ValueError("no uncorrected registered public coordinate NULLs")
    common=[d for d in revised["features"] if rule_from_spec(d).scope=="market"]
    channels=[n for d in common for n in (d["feature"],d["feature"]+"__available",d["feature"]+"__age_days",d["feature"]+"__updated")]
    if channels!=manifest["common_channels"]:raise ValueError("repair must retain common channel identity/order")
    if output.exists():raise FileExistsError("incremental repair requires a fresh root")
    output.mkdir(parents=True)
    replaced={"common.npy","feature_dictionary.json"}
    linked=[]
    def link(relative: str):
        original=(parent/relative).resolve(strict=True)
        if not original.is_relative_to(parent) or not original.is_file():
            raise ValueError("repair dependency escaped immutable parent")
        destination=output/relative;destination.parent.mkdir(parents=True,exist_ok=True)
        if not destination.exists():os.link(original,destination)
        linked.append(relative)
    for proof in factorized_member_proofs(manifest):
        if proof["path"] not in replaced:link(proof["path"])
    for name in ("execution_rules.parquet","formal_companion_receipt.json","quality_masks.json",
        "tw_corporate_action_reference.parquet","tw_corporate_action_reference.summary.json",
        "tw_corporate_action_entitlements.parquet","tw_corporate_action_entitlements.summary.json"):
        if (parent/name).exists():link(name)
    # Native financial rows have symbol != '*': pushed-down predicates avoid
    # reloading the several hundred million stock facts just to fix macros.
    event_root=output/"common_repair_events";event_root.mkdir()
    paths=[]
    for proof in manifest["annual_events"]:
        path=parent/proof["path"]
        table=pl.scan_parquet(path).filter(pl.col("symbol")=="*").collect(engine="streaming")
        if table.is_empty():continue
        target=event_root/path.name;table.write_parquet(target,compression="zstd");paths.append(target)
    events=AnnualObservationEvents(paths)
    dates=[date.fromisoformat(d) for d in manifest["dates"]]
    parts=[];counts=None
    for _,values,total in causal_state_blocks(dates,["*"],common,events,None):
        parts.append(values[:,0,:]);counts=total
    values=np.concatenate(parts)
    old=np.load(parent/manifest["common"]["path"],mmap_mode="r",allow_pickle=False)
    changed=[]
    for i,definition in enumerate(common):
        if definition["feature"] not in affected:
            np.testing.assert_array_equal(values[:,4*i:4*i+4].view(np.uint32),old[:,4*i:4*i+4].view(np.uint32))
        else:
            changed.append({"feature":definition["feature"],"null_event_policy":PUBLIC_COORDINATE_NULL_POLICY,
                "available_dates_before":int(old[:,4*i+1].sum()),"available_dates_after":int(values[:,4*i+1].sum())})
    np.save(output/"common.npy",values,allow_pickle=False)
    atomic_write_json(output/"feature_dictionary.json",revised)
    with (parent/"feature_coverage.csv").open(encoding="utf-8",newline="") as stream:
        import csv
        coverage=list(csv.DictReader(stream))
    updates={d["feature"]:int(n) for d,n in zip(common,counts,strict=True)}
    for row in coverage:
        if row["scope"]=="market":row["available_panel_cells"]=updates[row["feature"]]
    write_csv(output/"feature_coverage.csv",coverage)
    result={**manifest,"common":{**manifest["common"],"sha256":file_sha256(output/"common.npy"),
        "shape":list(values.shape),"bytes":(output/"common.npy").stat().st_size},
        "feature_dictionary_sha256":file_sha256(output/"feature_dictionary.json"),
        "semantic_revision":REPAIR_CONTRACT,"parent_manifest_sha256":before,
        "parent_manifest_path":str(manifest_path),"affected_common_features":affected,
        "training_missingness":"last released finite value + availability + age; public calendar absence is not a NULL release; native NULL/report/TTL/lifecycle barriers preserved",
        "feature_view_ready":True,"training_ready":False,"gpu_training_verified":False,"execution_preflight_passed":False}
    atomic_write_json(output/"factorized_manifest.json",result)
    if report:write_factorized_feature_report(output/"factorized_manifest.json",output/"feature_report")
    if file_sha256(manifest_path)!=before:raise ValueError("immutable parent changed during incremental repair")
    proof={"contract":REPAIR_CONTRACT,"parent_manifest_sha256":before,
        "manifest_sha256":file_sha256(output/"factorized_manifest.json"),
        "dictionary_sha256":result["feature_dictionary_sha256"],"changed_common":changed,
        "reused_individual_block_bytes":sum(b["bytes"] for b in manifest["blocks"]),
        "raw_members_unchanged":True,"hardlinked_members":len(set(linked)),
        "unaffected_common_bitwise_identical":True,"wall_s":time.perf_counter()-started,
        "script_sha256":file_sha256(Path(__file__)),"training_ready":False}
    atomic_write_json(output/"common_repair_receipt.json",proof)
    return proof


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--manifest",type=Path,required=True)
    p.add_argument("--output-root",type=Path,required=True)
    a=p.parse_args()
    print(json.dumps(repair(a.manifest,a.output_root),ensure_ascii=False))
    return 0


if __name__=="__main__":raise SystemExit(main())

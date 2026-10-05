#!/usr/bin/env python3
"""Extend a verified private release with native financial/source observations.

Penguin owns cleaned originals and provenance. No panel, lag alignment,
normalization, labels, training fit or broker call is performed by this script.
"""
from __future__ import annotations

import argparse
import csv
from datetime import UTC, datetime
import json
from pathlib import Path
import sys
from dataclasses import asdict

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from downloader.artifact_io import atomic_write_json
from scripts.prepare_tw_day_trade_feature_catalog import sha256, write_csv
from scripts.prepare_tw_day_trade_mixed_frequency import verify_sources
from scripts.stage_tw_public_research_release import _stage_copy
from stockagent.data.tw_public_release_schedule import rule_for, feature_name, feature_category
from stockagent.data.finlab_acquisition_contract import read_receipt_bound_source
from stockagent.data.tw_public_cross_source_fill import verify_staged_source_repairs

FINANCIAL = {"TaiwanStockFinancialStatements", "TaiwanStockBalanceSheet", "TaiwanStockCashFlowsStatement"}


def stage(base: Path, out: Path, catalog: Path, authorization: Path,
          taxonomy_dictionary: Path | None = None,
          refresh_finlab: bool = False, retain_prior_source: Path | None = None) -> dict:
    grant = json.loads(authorization.read_text())
    if grant.get("private_vastai1T_delivery_authorized") is not True:
        raise ValueError("private owner delivery authorization required")
    before = verify_sources(base)
    base_sha = sha256(base/"source_manifest.json")
    if refresh_finlab and not before.get("native_adapters"):
        raise ValueError("incremental FinLab refresh requires an accepted native source release")
    if refresh_finlab and retain_prior_source is not None:
        raise ValueError('capture latest raw source and verified history retention are separate stages')
    if out.exists():
        raise FileExistsError("use a new scoped private source release")
    out.mkdir(parents=True, mode=0o700)
    manifest = {**before, "files": dict(before["files"]), "native_adapters": list(before.get("native_adapters",[]))}
    for relative, proof in before["files"].items():
        _stage_copy(base/relative, out/relative, proof["sha256"])
    for name in ("finlab_admission.csv", "publication_rules.json"):
        if (base/name).is_file():
            digest = sha256(base/name)
            _stage_copy(base/name, out/name, digest)
            manifest["files"][name] = {"sha256":digest,"bytes":(out/name).stat().st_size}

    def copy(source: Path, relative: str, *, original_receipt=None):
        digest=sha256(source)
        _stage_copy(source,out/relative,digest)
        manifest["files"][relative]={"sha256":digest,"bytes":source.stat().st_size,
            "original_source":str(source.resolve()),"source_receipt":original_receipt}

    if retain_prior_source is not None:
        from stockagent.data.tw_public_cross_source_fill import (retain_verified_wide_snapshot,
            verify_staged_snapshot_retention,SNAPSHOT_RETENTION_CONTRACT)
        prior=verify_sources(retain_prior_source)
        for key in ('contract','use_restriction','authorization_sha256','end_date'):
            if prior.get(key)!=before.get(key):raise ValueError('prior snapshot scope/private authorization differs')
        prior_specs={s['dataset']:s for s in prior['feature_specs'] if s.get('source')=='FinLab'}
        audits=[];retained=[]
        manifest['feature_specs']=[dict(s) for s in before['feature_specs']]
        for spec in manifest['feature_specs']:
            if spec.get('source')!='FinLab' or spec['dataset'] not in prior_specs:continue
            old_spec=prior_specs[spec['dataset']]
            if any(old_spec.get(k)!=spec.get(k) for k in ('feature','rule','category')):
                raise ValueError('provider snapshot economic identity changed')
            old_proof=prior['files'][old_spec['path']];current_proof=before['files'][spec['path']]
            if old_proof['sha256']==current_proof['sha256']:continue
            table,fills,qa,conflicts=retain_verified_wide_snapshot(pl.read_parquet(base/spec['path']),
                pl.read_parquet(retain_prior_source/old_spec['path']))
            audits.append({'dataset':spec['dataset'],**qa})
            if not fills.height:continue
            if current_proof.get('source_repair'):
                raise ValueError('a repaired primary needs a new combined cross-source evidence bundle')
            prefix='source_retention/'+spec['feature']
            latest_rel,prior_rel,fills_rel=prefix+'.latest.parquet',prefix+'.prior.parquet',prefix+'.fills.parquet'
            copy(base/spec['path'],latest_rel);copy(retain_prior_source/old_spec['path'],prior_rel)
            fills.write_parquet(out/fills_rel,compression='zstd')
            manifest['files'][fills_rel]={'sha256':sha256(out/fills_rel),'bytes':(out/fills_rel).stat().st_size}
            table.write_parquet(out/spec['path'],compression='zstd')
            manifest['files'][spec['path']]={**current_proof,'sha256':sha256(out/spec['path']),
                'bytes':(out/spec['path']).stat().st_size,
                'projection':'verified_same_provider_missing_only_history_union; latest_finite_unchanged'}
            spec['value_vintage']='latest_received_finite_with_verified_prior_snapshot_missing_only'
            retained.append({'dataset':spec['dataset'],'latest':latest_rel,'prior':prior_rel,'fills':fills_rel,
                'filled_observations':fills.height,'agreement':qa['agreement']})
        copy(base/'source_manifest.json','source_retention/latest_source_manifest.json')
        copy(retain_prior_source/'source_manifest.json','source_retention/prior_source_manifest.json')
        write_csv(out/'source_retention/agreement.csv',audits)
        manifest['files']['source_retention/agreement.csv']={'sha256':sha256(out/'source_retention/agreement.csv'),
            'bytes':(out/'source_retention/agreement.csv').stat().st_size}
        manifest.update(created_at_utc=datetime.now(UTC).isoformat(),base_source_manifest_sha256=base_sha,
            source_bytes=sum(x['bytes'] for x in manifest['files'].values()),implementation_sha256=sha256(Path(__file__)),
            source_snapshot_retention={'contract':SNAPSHOT_RETENTION_CONTRACT,'datasets':retained,
                'filled_observations':sum(r['filled_observations'] for r in retained),
                'prior_source_manifest_sha256':sha256(retain_prior_source/'source_manifest.json'),
                'latest_source_manifest_sha256':base_sha,'new_provider_requests':0})
        verify_staged_source_repairs(out,manifest);verify_staged_snapshot_retention(out,manifest)
        atomic_write_json(out/'source_manifest.json',manifest);verify_sources(out)
        return {k:v for k,v in manifest.items() if k not in {'files','feature_specs','native_adapters'}}

    if before.get("native_adapters"):
        # Versioned enrichment of an already verified native release: never
        # reread mutable MOPS/FinMind archives in place of its pinned members.
        if taxonomy_dictionary:
            copy(taxonomy_dictionary,"taxonomy/dictionary.json")
            manifest["taxonomy_dictionary"]="taxonomy/dictionary.json"
        additional=[]; refreshed=[]
        by_dataset={spec.get('dataset'):spec for spec in manifest['feature_specs'] if spec.get('source')=='FinLab'}
        symbols={p.name.removesuffix("_features.parquet") for p in (base/"stocks").glob("*_features.parquet")}
        with (catalog/"finlab_keys.csv").open(encoding="utf-8-sig",newline="") as stream:
            for row in csv.DictReader(stream):
                key=row["dataset_id"]
                existing=by_dataset.get(key)
                if existing and not refresh_finlab: continue
                if refresh_finlab and not existing: continue # No implicit feature/universe expansion.
                rule=rule_for(key)
                if rule is None:continue
                receipt_path=Path(row['receipt_path'])
                original,receipt=read_receipt_bound_source(receipt_path.parent.parent,key,receipt_path)
                if sha256(original)!=receipt.get("sha256"):raise ValueError("FinLab receipt changed")
                if existing:
                    old=manifest['files'][existing['path']]
                    if old.get('original_source_sha256')==receipt['sha256']:continue
                    if old.get('source_repair'):
                        raise ValueError('changed repaired primary requires a newly validated missing-only bundle')
                import pyarrow.parquet as pq
                columns=[n for n in pq.read_schema(original).names if n=="source_index" or rule.scope=="market" or n in symbols]
                if "source_index" not in columns or len(columns)<2:
                    if existing:raise ValueError('updated admitted FinLab quantity lost its axis or universe')
                    continue
                table=pl.read_parquet(original,columns=columns)
                schema=table.schema
                if any(not(schema[n].is_numeric() or schema[n]==pl.Null) for n in columns if n!="source_index"):
                    if existing:raise ValueError('updated admitted FinLab quantity has nonnumeric measures')
                    continue
                if table['source_index'].null_count() or table['source_index'].n_unique()!=table.height:
                    raise ValueError('FinLab observation axis is NULL or duplicated')
                name=existing['feature'] if existing else feature_name(key)
                relative=existing['path'] if existing else f"finlab/observations/{name}.parquet"
                target=out/relative;target.parent.mkdir(parents=True,exist_ok=True)
                table.write_parquet(target,compression="zstd")
                if sha256(original)!=receipt["sha256"]:raise ValueError("FinLab source changed")
                manifest["files"][relative]={"sha256":sha256(target),"bytes":target.stat().st_size,
                    "source_receipt":receipt,"original_source_sha256":receipt["sha256"],
                    "projection":"unchanged_original_source_index_and_values; official_historical_universe_only"}
                if existing:
                    refreshed.append({'dataset':key,'before_original_source_sha256':old.get('original_source_sha256'),
                        'after_original_source_sha256':receipt['sha256'],
                        'before_projection_sha256':old['sha256'],
                        'after_projection_sha256':manifest['files'][relative]['sha256']})
                else:
                    additional.append({"feature":name,"source":"FinLab","dataset":key,"path":relative,
                        "rule":asdict(rule),"clock":"estimated_publication","category":feature_category(key),
                        "publication_time_estimated":True,"value_vintage":"current_provider_revision"})
        manifest["feature_specs"].extend(additional)
        if refresh_finlab:
            # Only current, admitted FinLab objects change. Preserve the pinned
            # macro/MOPS/FinMind source members and every verified real fill.
            manifest.update(created_at_utc=datetime.now(UTC).isoformat(),base_source_manifest_sha256=base_sha,
                source_bytes=sum(x['bytes'] for x in manifest['files'].values()),
                implementation_sha256=sha256(Path(__file__)),
                incremental_refresh={'scope':'existing_finlab_receipt_bound_projections_only',
                    'refreshed_datasets':refreshed,'new_feature_specs':0})
            if sha256(base/'source_manifest.json')!=base_sha:
                raise ValueError('accepted base release changed during incremental refresh')
            verify_staged_source_repairs(out,manifest)
            atomic_write_json(out/'source_manifest.json',manifest);verify_sources(out)
            return {k:v for k,v in manifest.items() if k not in {'files','feature_specs','native_adapters'}}
        global_members=[]
        # These source rows carry native entity/unit/publication axes. Unknown
        # historical clocks stay NULL in the model; they are not backdated.
        for kind,relative,original in [
            ("fred_events","global/fred.parquet",ROOT/"data_fred_crypto_macro/observations.parquet"),
            ("free_public_events","global/free_public.parquet",ROOT/"data_free_public/observations.parquet"),
            ("etf_events","global/etf.parquet",ROOT/"data_crypto_etf/normalized/issuer_daily_fund_metrics.parquet")]:
            if original.is_file():
                copy(original,relative);global_members.append({"kind":kind,"path":relative})
        manifest["native_adapters"].extend(global_members)
        # Public US economic observations remain source-only until a native
        # program/release-time adapter qualifies their historical clocks.
        economic=[]
        with (catalog/"economic_source_files.csv").open(encoding="utf-8-sig",newline="") as stream:
            for i,row in enumerate(csv.DictReader(stream)):
                if row["provider"] not in {"bea","census"}:continue # no housing
                original=Path(row["source_path"])
                if sha256(original)!=row["declared_sha256"]:raise ValueError("US macro receipt changed")
                relative=f"global/economic/{i:05d}.parquet";copy(original,relative)
                economic.append({"path":relative,"provider":row["provider"],"dataset":row["dataset"],
                    "admission":"native_publication_program_adapter_required"})
        manifest["native_adapters"].append({"kind":"us_economic_programs","members":economic})
        manifest.update(created_at_utc=datetime.now(UTC).isoformat(),base_source_manifest_sha256=base_sha,
            source_bytes=sum(x["bytes"] for x in manifest["files"].values()),
            selected_value_features=len(manifest["feature_specs"]),implementation_sha256=sha256(Path(__file__)))
        verify_staged_source_repairs(out,manifest)
        atomic_write_json(out/"source_manifest.json",manifest);verify_sources(out)
        return {k:v for k,v in manifest.items() if k not in {"files","feature_specs","native_adapters"}}

    # Every currently inventoried IFRS archive is retained unchanged; the
    # remote adapter owns issuer, context, fiscal-grain and clock admission.
    mops_root = ROOT/"data_tw_public/mops_xbrl"
    fact_paths = sorted((mops_root/"normalized/ifrs").glob("*/*/facts.parquet"))
    relative_paths=[]
    for path in fact_paths:
        relative="mops_xbrl/"+path.relative_to(mops_root).as_posix()
        copy(path,relative); relative_paths.append(relative)
    copy(mops_root/"publication_candidates.parquet","mops_xbrl/publication_candidates.parquet")
    manifest["native_adapters"].append({"kind":"mops_financial_facts", "paths":relative_paths,
        "publication_candidates":"mops_xbrl/publication_candidates.parquet",
        "native_contexts_preserved":True,"only_dimensionless_measures_initially_trainable":True})
    symbols={p.name.removesuffix("_features.parquet") for p in (base/"stocks").glob("*_features.parquet")}
    selected=[]
    with (catalog/"finmind_source_files.csv").open(encoding="utf-8-sig",newline="") as stream:
        for row in csv.DictReader(stream):
            if row["dataset"] in FINANCIAL and row["last"] >= "2013-01-01":
                selected.append(row)
    # Native period/type/origin_name are all kept. Sponsor and complement
    # collisions are resolved only by exact value agreement on the remote.
    proofs=[]
    for i,row in enumerate(selected):
        original=Path(row["source_path"])
        receipt=json.loads(Path(row["receipt_path"]).read_text())
        digest=sha256(original)
        if digest!=row["declared_sha256"] or receipt.get("sha256")!=digest:
            raise ValueError(f"FinMind receipt changed: {original}")
        table=pl.read_parquet(original).filter(pl.col("stock_id").is_in(symbols))
        if table.is_empty(): continue
        relative=f"finmind/{row['dataset']}/{i:05d}.parquet"
        target=out/relative; target.parent.mkdir(parents=True,exist_ok=True)
        table.select("date","stock_id","type","origin_name","value").write_parquet(target,compression="zstd")
        if sha256(original)!=digest or json.loads(Path(row["receipt_path"]).read_text()).get("sha256")!=digest:
            raise ValueError("FinMind source changed while projecting")
        manifest["files"][relative]={"sha256":sha256(target),"bytes":target.stat().st_size,
            "original_source_sha256":digest,"source_receipt":receipt,
            "projection":"unchanged_native_period_code_label_value; official_security_columns"}
        proofs.append({"path":relative,"dataset":row["dataset"],"lane":row["lane"]})
    manifest["native_adapters"].append({"kind":"finmind_financial_facts","members":proofs})
    # Actual macro-event source values include zero/negative exports/balances
    # lost by older positive-only log views. Keep its native release evidence.
    macro=ROOT/"artifacts/data_quality/tw_public_provisional_macro/events.parquet"
    if macro.is_file():
        copy(macro,"native_macro/events.parquet")
        manifest["native_adapters"].append({"kind":"tw_macro_release_events","path":"native_macro/events.parquet"})
    # Other source families stay visible in the full semantic inventory. Do
    # not ship TEJ previews as dated observations or use license metadata as X.
    manifest.update(created_at_utc=datetime.now(UTC).isoformat(),
        base_source_manifest_sha256=base_sha, authorization_sha256=sha256(authorization),
        source_bytes=sum(x["bytes"] for x in manifest["files"].values()),
        source_only=True,training_ready=False,implementation_sha256=sha256(Path(__file__)))
    if sha256(base/"source_manifest.json")!=base_sha:
        raise ValueError("base release changed during source staging")
    verify_staged_source_repairs(out,manifest)
    atomic_write_json(out/"source_manifest.json",manifest)
    verify_sources(out)
    return {k:v for k,v in manifest.items() if k not in {"files","feature_specs","native_adapters"}}


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--base-source",type=Path,required=True)
    p.add_argument("--output-root",type=Path,required=True)
    p.add_argument("--catalog",type=Path,required=True)
    p.add_argument("--authorization",type=Path,required=True)
    p.add_argument("--taxonomy-dictionary",type=Path)
    p.add_argument("--refresh-finlab",action='store_true',help='refresh only changed admitted FinLab objects; keep pinned native sources')
    p.add_argument('--retain-prior-source',type=Path,help='missing-only real-observation union from one verified same-scope prior source')
    a=p.parse_args()
    print(json.dumps(stage(a.base_source,a.output_root,a.catalog,a.authorization,a.taxonomy_dictionary,a.refresh_finlab,a.retain_prior_source),ensure_ascii=False))


if __name__=="__main__":main()

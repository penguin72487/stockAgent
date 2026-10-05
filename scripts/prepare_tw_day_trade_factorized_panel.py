#!/usr/bin/env python3
"""Build the nullable, common/individual panel on its training node.

Observations are immutable coordinate tables (unobserved coordinates = NULL).
The causal training view carries only already-released states, with availability,
age and update channels. Executable prices/rules use the ordinary TW loader.
"""
from __future__ import annotations

import argparse
from bisect import bisect_left, bisect_right
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, replace
from datetime import UTC, date, datetime
import json
from pathlib import Path
from zoneinfo import ZoneInfo
import sys
import time

import numpy as np
import polars as pl
import pyarrow.parquet as pq
import yaml

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from downloader.artifact_io import atomic_write_json
from scripts.prepare_tw_day_trade_feature_catalog import sha256, write_csv
from scripts.prepare_tw_day_trade_mixed_frequency import verify_sources, attach_formal_companions
from scripts.build_tw_release_schedule_dataset import normalize_source
from stockagent.config import load_config
from stockagent.data.factorized_panel import CONTRACT, VALUE_ONLY_CONTRACT, write_array_block
from stockagent.data.panel import build_panel
from stockagent.data.tw_day_trade_mixed_frequency import source_clock_lookup, rule_from_spec, PRIVATE_USE, public_coordinate_null_spec, PUBLIC_COORDINATE_NULL_POLICY
from stockagent.data.tw_native_panel_features import mops_native, mops_admission, native_feature_name, collapse_conflicting_observations
from stockagent.data.tw_public_release_schedule import RULES, schedule_lookup, next_session, max_carry_days
from stockagent.data.tw_public_cross_source_fill import MAPPINGS, normalize_finmind, missing_only
from stockagent.data.tw_feature_semantic_report import semantic_role, write_factorized_feature_report
from stockagent.data.walkforward import build_expanding_year_folds
from train import _build_panel_kwargs

BASE_FEATURES=["open_raw","high_raw","low_raw","close_raw","trading_volume_raw","next_session_open_gap_logret"]
OBS_SCHEMA={"date":pl.Date,"symbol":pl.String,"feature":pl.String,"value":pl.Float64,"period":pl.String}


def quarter_clock(table, sessions, uploads=None):
    """Actual upload bound when known; otherwise the explicit quarterly proxy."""
    lookup=schedule_lookup(table["period"].unique().to_list(),RULES["quarter"],sessions).select(
        pl.col("source_index").alias("period"),pl.col("date").alias("_proxy"))
    table=table.join(lookup,on="period",how="left",validate="m:1")
    if uploads is not None:
        known=uploads.with_columns(pl.col("source_index").str.replace_all("-","").alias("period"))
        table=table.join(known.select("period","symbol","known_upload_on"),on=["period","symbol"],how="left",validate="m:1")
        dates={d:next_session(d,sessions) for d in table["known_upload_on"].drop_nulls().unique()}
        table=table.with_columns(pl.when(pl.col("known_upload_on").is_not_null()).then(
            pl.col("known_upload_on").replace_strict(dates,default=None,return_dtype=pl.Date))
            .otherwise(pl.col("_proxy")).alias("date")).drop("known_upload_on")
    else:table=table.with_columns(pl.col("_proxy").alias("date"))
    return table.drop("_proxy").filter(pl.col("date").is_not_null())


def economic_primary(table):
    """Approved missing-only unions replace NULL slots, never existing values."""
    primary=table.select("period","symbol","value").group_by("period","symbol").agg(
        pl.col("value").drop_nulls().n_unique().alias("variants"),
        pl.col("value").drop_nulls().first().alias("value"))
    if primary.filter(pl.col("variants")>1).height:
        raise ValueError("conflicting primary economic values; not a missing-only repair")
    return primary.drop("variants")


def causal_report_events(events,definitions,*,period_order=None,current_periods=None):
    """Retain old revisions in raw tables, but never replace a newer report state."""
    ignored=[]
    for definition in definitions:
        policy=definition.get("null_event_policy", "released_null_barrier")
        if policy==PUBLIC_COORDINATE_NULL_POLICY:
            if public_coordinate_null_spec(definition).get("null_event_policy")!=policy:
                raise ValueError("coordinate NULL policy escaped its public market scope")
            ignored.append(definition["feature"])
        elif policy!="released_null_barrier":
            raise ValueError("unknown NULL event policy")
    if ignored:
        # Only the training event stream omits proven outer-join padding.
        # Raw nullable coordinates remain untouched and native NULL releases
        # still clear state; no changed-value test infers a publication.
        events=events.filter(~pl.col("feature").is_in(ignored) | pl.col("value").is_finite())
    if events.is_empty() or "period" not in events.columns:return events
    mapping=pl.DataFrame({"feature":[d["feature"] for d in definitions],
        "_state_key":[d.get("report_group") or d["feature"] for d in definitions]})
    table=events.join(mapping,on="feature",validate="m:1").with_columns(
        pl.col("period").fill_null(pl.col("date").cast(pl.String)).alias("_subject"))
    subjects=sorted(table["_subject"].unique().to_list()) if period_order is None else None
    ranks=dict(zip(subjects,range(len(subjects)))) if subjects is not None else period_order
    table=table.with_columns(pl.col("_subject").replace_strict(ranks,return_dtype=pl.Int32).alias("_rank"))
    table=table.sort("date","symbol","_state_key","_rank").with_columns(
        pl.col("_rank").max().over("date","symbol","_state_key").alias("_daily_rank"))
    table=table.with_columns(pl.col("_daily_rank").cum_max().over("symbol","_state_key").alias("_latest"))
    if current_periods:
        prior=pl.DataFrame([{"symbol":s,"_state_key":key,"_prior":rank} for (s,key),rank in current_periods.items()],
            schema={"symbol":pl.String,"_state_key":pl.String,"_prior":pl.Int32})
        table=table.join(prior,on=["symbol","_state_key"],how="left",validate="m:1").with_columns(
            pl.max_horizontal("_latest",pl.col("_prior").fill_null(-1)).alias("_latest"))
    if current_periods is not None:
        for s,key,rank in table.group_by("symbol","_state_key").agg(pl.col("_latest").max()).iter_rows():
            current_periods[s,key]=rank
    return table.filter(pl.col("_rank")==pl.col("_latest")).select(events.columns)


class AnnualObservationEvents:
    """Bound memory by a single year, not the complete multi-source history."""
    def __init__(self,paths):
        self.paths=sorted(paths)
        periods=pl.scan_parquet(self.paths).select(
            pl.col("period").fill_null(pl.col("date").cast(pl.String)).alias("subject")).unique().collect(engine="streaming")["subject"]
        self.period_order={subject:i for i,subject in enumerate(sorted(periods.to_list()))}

    def iter_date_groups(self,definitions,symbols):
        names=[d["feature"] for d in definitions];current={}
        for path in self.paths:
            table=pl.scan_parquet(path).filter(pl.col("feature").is_in(names)&pl.col("symbol").is_in(symbols)).collect(engine="streaming")
            if table.is_empty():continue
            # Each source part has already masked its conflicting keys. Later
            # parts can only replace an existing NULL under approved alias/fill
            # contracts; they do not vote across incompatible source values.
            table=table.sort("_source_part").unique(["date","symbol","feature","period"],keep="last",maintain_order=True).drop("_source_part")
            table=table.sort("date","symbol","feature","period").unique(["date","symbol","feature"],keep="last",maintain_order=True)
            table=causal_report_events(table,definitions,period_order=self.period_order,current_periods=current)
            for (day,),part in sorted(table.partition_by("date",as_dict=True).items()):yield day,part


def global_rule(kind,identity):
    """Known source cadence, not guessed from a repeated or changing value."""
    if kind=="fred_events" and identity.get("series_id") in {"WALCL","NFCI"}:
        urls=("https://www.federalreserve.gov/releases/h41/default.htm",
              "https://www.chicagofed.org/research/data/nfci/current-data-aws")
        return replace(RULES["weekly"],name="native_fred_weekly_released_state",scope="market",
            cadence_change_on="",older_carry_days=0,urls=urls,
            rationale="Weekly native release availability; 14d bounded carry, no inferred publication date")
    return replace(RULES["daily"],scope="market")


def first_ready_session(timestamp,sessions):
    """Native available_at already includes its source owner's safety policy."""
    stamp=datetime.fromisoformat(str(timestamp).replace("Z","+00:00"))
    if stamp.tzinfo is None:raise ValueError("native availability needs an explicit timezone")
    local=stamp.astimezone(ZoneInfo("Asia/Taipei"));day=local.date()
    index=(bisect_left(sessions,day) if (local.hour,local.minute,local.second,local.microsecond)<(9,0,0,0)
           else bisect_right(sessions,day))
    return sessions[index] if index<len(sessions) else None


def macro_preopen_sessions(table, sessions):
    """Resolve the named publication clock, not a stale precomputed session.

    Raw event clocks may be explicitly estimated/current-revision research
    evidence. This preserves that caveat; it never makes after-close values
    available to the same day's 09:00 decision.
    """
    dates = {}
    for text in table['published_at_taipei'].unique().to_list():
        if text is None:
            raise ValueError('native macro event lacks publication boundary')
        stamp = datetime.fromisoformat(text)
        if stamp.tzinfo is None:
            stamp = stamp.replace(tzinfo=ZoneInfo('Asia/Taipei'))
        dates[text] = first_ready_session(stamp.isoformat(), sessions)
    return table.with_columns(pl.col('published_at_taipei').replace_strict(
        dates, default=None, return_dtype=pl.Date).alias('_preopen_session'))


@dataclass
class CausalColumnBlocks:
    """Exact logical array encoded without allocating its positive-zero cells."""
    shape: tuple[int,int,int]
    symbol_blocks: list[tuple[int,np.ndarray,np.ndarray]]


def combine_column_rows(rows,shape,symbol_block_size):
    blocks=[]
    for group,start in enumerate(range(0,shape[1],symbol_block_size)):
        parts=[row[group] for row in rows]
        columns=np.unique(np.concatenate([p[0] for p in parts])).astype(np.int64)
        values=np.zeros((len(rows),min(symbol_block_size,shape[1]-start),len(columns)),dtype=np.float32)
        for r,(indices,part) in enumerate(parts):
            values[r][:,np.searchsorted(columns,indices)]=part
        blocks.append((start,columns,values))
    return CausalColumnBlocks((len(rows),shape[1],shape[2]),blocks)


def causal_state_blocks(dates, symbols, definitions, events, lifecycle_start, *, resets=None,
                        max_block_bytes=128*1024**2, column_blocks=False, symbol_block_size=128,
                        model_channel_policy="value_available_age_updated"):
    """One chronological sweep, O(events + emitted cells); no per-feature joins.

    A NULL report is a state barrier. A newer issuer report clears omitted
    concepts in its report group. Issuer-code lifecycle reuse never carries a
    predecessor's data. Raw observations are not modified by this sweep.
    """
    if model_channel_policy not in {"value_only", "value_available_age_updated"}:
        raise ValueError("unknown factorized model channel policy")
    channel_count = 1 if model_channel_policy == "value_only" else 4
    names=[d["feature"] for d in definitions];width=len(names);count=len(symbols)
    fmap={n:i for i,n in enumerate(names)};smap={n:i for i,n in enumerate(symbols)}
    rules=[rule_from_spec(d) for d in definitions]
    ttl=np.asarray([r.carry_days for r in rules],dtype=np.int32)
    older=np.asarray([r.older_carry_days for r in rules],dtype=np.int32)
    change=np.asarray([date.fromisoformat(r.cadence_change_on).toordinal() if r.cadence_change_on else -1 for r in rules])
    values=np.full((count,width),np.nan,dtype=np.float64)
    observed=np.full((count,width),-1000000,dtype=np.int32)
    groups=defaultdict(list)
    for i,definition in enumerate(definitions):
        if definition.get("report_group"):groups[definition["report_group"]].append(i)
    if isinstance(events,AnnualObservationEvents):
        event_iterator=iter(events.iter_date_groups(definitions,symbols))
    else:
        events=events.filter(pl.col("feature").is_in(names)&pl.col("symbol").is_in(symbols))
        events=causal_report_events(events,definitions)
        event_iterator=iter(sorted((k[0],v) for k,v in events.partition_by("date",as_dict=True).items()))
    next_event=next(event_iterator,None)
    resets_by_date={} if resets is None else resets.partition_by("date",as_dict=True,maintain_order=False)
    group_mapping=pl.DataFrame({"feature":names,"report_group":[d.get("report_group") or "" for d in definitions]})
    reset_dates=iter(sorted(k[0] for k in resets_by_date));next_reset=next(reset_dates,None)
    report_periods={}
    rows=max(1,min(32,int(max_block_bytes)//max(1,count*width*channel_count*4)))
    block=[];available_count=np.zeros(width,dtype=np.int64)
    column_candidates=[set() for _ in range((count+symbol_block_size-1)//symbol_block_size)]
    block_bytes=0
    for row,day in enumerate(dates):
        # Stored t-1 contains only facts ready by decision t. The final
        # completed day has no next ready decision and therefore no new facts.
        decision=dates[row+1] if row+1<len(dates) else None
        previous_count=available_count.copy()
        matrix=None if column_blocks else np.zeros((count,width,channel_count),dtype=np.float32)
        compact=[]
        if decision is not None:
            while True:
                candidates=[d for d in [next_event[0] if next_event else None,next_reset] if d is not None]
                if not candidates or min(candidates)>decision:break
                released=min(candidates)
                table=next_event[1] if next_event is not None and next_event[0]==released else None
                reset=resets_by_date.get((released,))
                if reset is not None:
                    keys=["symbol","report_group"]+(["period"] if "period" in reset.columns else [])
                    for record in reset.select(keys).unique().sort(keys).iter_rows():
                        symbol,group=record[:2]
                        if len(record)==3:
                            subject=record[2]
                            if subject is None:raise ValueError("report reset needs a subject period")
                            previous=report_periods.get((symbol,group),"")
                            if subject<previous:continue
                            report_periods[symbol,group]=subject
                        if symbol in smap and group in groups:
                            index=np.asarray(groups[group],dtype=np.int64)
                            values[smap[symbol],index]=np.nan;observed[smap[symbol],index]=-1000000
                if table is not None and table.height:
                    if report_periods and "period" in table.columns:
                        latest=pl.DataFrame([{"symbol":s,"report_group":g,"_period_floor":p} for (s,g),p in report_periods.items()])
                        table=table.join(group_mapping,on="feature",validate="m:1").join(latest,on=["symbol","report_group"],how="left",validate="m:1").filter(
                            pl.col("_period_floor").is_null() | (pl.col("period")>=pl.col("_period_floor")))
                    si=np.fromiter((smap[s] for s in table["symbol"]),dtype=np.int64)
                    fi=np.fromiter((fmap[f] for f in table["feature"]),dtype=np.int64)
                    values[si,fi]=table["value"].to_numpy();observed[si,fi]=released.toordinal()
                    if column_blocks:
                        for group in np.unique(si//symbol_block_size):
                            column_candidates[group].update(fi[si//symbol_block_size==group].tolist())
                if next_event is not None and next_event[0]==released:next_event=next(event_iterator,None)
                if next_reset==released:next_reset=next(reset_dates,None)
            limit=np.where((change>=0)&(decision.toordinal()<change),older,ttl)
            if column_blocks:
                for group,start in enumerate(range(0,count,symbol_block_size)):
                    stop=min(count,start+symbol_block_size)
                    columns=np.asarray(sorted(column_candidates[group]),dtype=np.int64)
                    source_values=values[start:stop][:,columns]
                    source_observed=observed[start:stop][:,columns]
                    age=decision.toordinal()-source_observed
                    present=np.isfinite(source_values)&(age>=0)&(age<=limit[None,columns])
                    if lifecycle_start is not None:
                        present &= source_observed>=lifecycle_start[row+1,start:stop,None]
                    part=np.zeros((stop-start,len(columns),channel_count),dtype=np.float32)
                    part[...,0]=np.where(present,source_values,0).astype(np.float32)
                    if channel_count == 4:
                        part[...,1]=present;part[...,2]=np.where(present,age,0);part[...,3]=(age==0)
                    if not np.isfinite(part).all():raise ValueError("source value overflowed Float32; needs explicit value transform")
                    available_count[columns]+=present.sum(axis=0)
                    compact.append(((columns[:,None]*channel_count+np.arange(channel_count)).reshape(-1),part.reshape(stop-start,-1)))
            else:
                age=decision.toordinal()-observed
                present=np.isfinite(values)&(age>=0)&(age<=limit[None,:])
                if lifecycle_start is not None:
                    present &= observed>=lifecycle_start[row+1,:,None]
                matrix[...,0]=np.where(present,values,0).astype(np.float32)
                if channel_count == 4:
                    matrix[...,1]=present
                    matrix[...,2]=np.where(present,age,0)
                    matrix[...,3]=(age==0)
                if not np.isfinite(matrix).all():raise ValueError("source value overflowed Float32; needs explicit value transform")
                available_count+=present.sum(axis=0)
        if column_blocks:
            if decision is None:
                compact=[(np.empty(0,dtype=np.int64),np.empty((min(symbol_block_size,count-start),0),dtype=np.float32))
                    for start in range(0,count,symbol_block_size)]
            new_bytes=sum(p.nbytes+i.nbytes for i,p in compact)
            # Bound the physical buffer, not the often-terabyte logical cube.
            if block and block_bytes+new_bytes>max_block_bytes:
                yield row-len(block),combine_column_rows(block,(0,count,width*channel_count),symbol_block_size),previous_count
                block=[];block_bytes=0
            block.append(compact);block_bytes+=new_bytes
        else:block.append(matrix.reshape(count,width*channel_count))
        if len(block)==(32 if column_blocks else rows) or row+1==len(dates):
            output=combine_column_rows(block,(0,count,width*channel_count),symbol_block_size) if column_blocks else np.stack(block)
            yield row+1-len(block),output,available_count.copy()
            block=[];block_bytes=0


def prepare(source: Path, out: Path, base_config: Path, minute_root: Path, snapshot_id: str,
            *, max_block_bytes=128*1024**2, base_panel_cache_root: Path | None=None,
            model_channel_policy="value_available_age_updated"):
    if model_channel_policy not in {"value_only", "value_available_age_updated"}:
        raise ValueError("unknown factorized model channel policy")
    channel_count = 1 if model_channel_policy == "value_only" else 4
    started=time.perf_counter();manifest=verify_sources(source);source_sha=sha256(source/"source_manifest.json")
    if out.exists():raise FileExistsError("new ABI requires a fresh panel/output root")
    out.mkdir(parents=True);observations=out/"observations";observations.mkdir()
    annual_root=out/"annual_events";annual_root.mkdir();annual_writers={};annual_paths={}
    public=source/"features/tw_public_stock_daily.parquet"
    rules=[n for n in pl.scan_parquet(public).collect_schema() if n.startswith("_twpub_")]
    rule_path=out/"execution_rules.parquet"
    pl.scan_parquet(public).select("date","symbol",*rules).sink_parquet(rule_path,compression="zstd")
    if "tw_corporate_action_entitlements.summary.json" in manifest["files"]:attach_formal_companions(source,out,manifest)
    generated={"base_config":str(base_config.resolve()),"experiment_name":"tw-day-trade-nullable-factorized-panel-20261004-v1",
        "runner":{"output_dir":str((out.parent/"training").resolve()),"resume":False,"post_train_infer":False},
        "data":{"parquet_root":str((source/"stocks").resolve()),"tw_public_feature_path":str(rule_path.resolve()),
            "day_trade_physical_public_feature_path":str(public.resolve()),"day_trade_minute_execution_root":str(minute_root.resolve()),
            "panel_cache_root":str((base_panel_cache_root or out/"base_panel_cache").resolve()),"factorized_feature_manifest":str((out/"factorized_manifest.json").resolve()),
            "feature_include":BASE_FEATURES,"feature_exclude":[],"feature_zero_fill":[],
            "feature_shift_next_session":[],"feature_availability_indicators":[]},
        "training":{"pretrained_initialization_root":None,"cache_train_tensors_on_gpu":False,
            "cache_eval_tensors_on_gpu":False,"curve_plot_async":False,
            "financial_transformer":{"feature_bottleneck_dim":0,"temporal_basis_fp32_contraction":True}}}
    config_path=out/"training.yaml";config_path.write_text(yaml.safe_dump(generated,allow_unicode=True,sort_keys=False))
    config=load_config(config_path)
    if config.trading.execution_mode!="tw_day_trade" or config.runner.resume:raise ValueError("fresh canonical TW lag-one experiment required")
    panel=build_panel(config.data.parquet_root,**_build_panel_kwargs(config))
    dates=[date.fromisoformat(str(d)[:10]) for d in panel.dates];symbols=list(panel.symbols)
    folds=build_expanding_year_folds(panel.dates,min_train_years=config.walk_forward.min_train_years,
        val_years=config.walk_forward.val_years,require_future_test_year=config.walk_forward.require_future_test_year,
        split_start_year=config.walk_forward.split_start_year)
    if not folds:raise ValueError("canonical walk-forward has no training-owned dates")
    training_cutoff=max(dates[int(f.train_indices[-1])] for f in folds)
    sessions=sorted(pl.read_parquet(source/"twse_taiex_ohlc.parquet",columns=["date"])["date"].cast(pl.Date).unique().to_list())
    sessions=[d for d in sessions if date(2013,1,1)<=d<=date.fromisoformat(manifest["end_date"])]
    uploads=None
    if (source/"finlab/known_uploads.parquet").exists():
        uploads=pl.read_parquet(source/"finlab/known_uploads.parquet").unpivot(index="source_index",variable_name="symbol",value_name="known_upload_on")
        uploads=uploads.with_columns(pl.col("known_upload_on").cast(pl.Date,strict=True)).drop_nulls()
    definitions={};parts=[];excluded=[];quality=[];resets=[];aliases=[];primary_parts={}
    bounds_relative = manifest.get('source_repairs', {}).get('publication_lower_bounds')
    publication_bounds = pl.read_parquet(source / bounds_relative) if bounds_relative else None
    def commit(table, specs, label):
        if not table.height:return
        table=table.select(pl.col(c).cast(dtype,strict=True) for c,dtype in OBS_SCHEMA.items())
        table=table.filter(pl.col("date").is_between(sessions[0],sessions[-1]))
        if not table.height:return
        table=table.with_columns(pl.when(pl.col("value").is_finite()).then(pl.col("value")).otherwise(None).alias("value"))
        table,conflicts=collapse_conflicting_observations(table,["date","symbol","feature","period"])
        if conflicts:quality.append({"source":label,"conflicting_values_masked_null":conflicts})
        path=observations/f"{len(parts):05d}.parquet";table.sort("date","symbol","feature").write_parquet(path,compression="zstd")
        parts.append(path)
        staged=table.with_columns(pl.lit(len(parts)-1,dtype=pl.Int32).alias("_source_part"),pl.col("date").dt.year().alias("_year"))
        for (year,),part in staged.partition_by("_year",as_dict=True).items():
            arrow=part.drop("_year").to_arrow()
            if year not in annual_writers:
                annual_paths[year]=annual_root/f"{year}.parquet"
                annual_writers[year]=pq.ParquetWriter(annual_paths[year],arrow.schema,compression="zstd")
            annual_writers[year].write_table(arrow,row_group_size=128_000)
        for spec in specs:
            spec={**spec,"scope":rule_from_spec(spec).scope}
            name=spec["feature"]
            if name in definitions:
                existing=definitions[name]
                if existing.get("identity")!=spec.get("identity"):raise ValueError("feature hash semantic collision")
                existing["original_concepts"]=sorted(set(existing.get("original_concepts",[])+spec.get("original_concepts",[])))
            else:definitions[name]=spec
            if spec.get("dataset","").startswith("financial_statement:"):
                primary_parts.setdefault(spec["dataset"],[]).append(path)
        print(f"[factorized-panel] {label}: {table.height:,} observations; {len(definitions):,} quantities; elapsed={time.perf_counter()-started:.2f}s",flush=True)
    # Native macro events, not forward-filled legacy daily table values.
    native_macro=next((a for a in manifest.get("native_adapters",[]) if a["kind"]=="tw_macro_release_events"),None)
    macro_names=set()
    if native_macro:
        macro=pl.read_parquet(source/native_macro["path"]).filter(pl.col("feature").str.ends_with("_raw")|pl.col("feature").is_in(["twpub_cbc_overnight_rate"]))
        macro_names=set(macro["feature"].unique())
        for name in sorted(macro_names):
            data=macro.filter(pl.col("feature")==name)
            feature="twcad_"+name.removeprefix("twpub_")
            rule=replace(RULES["quarter"] if "gdp" in name else RULES["business"] if name.startswith(("twpub_cbc_m","twpub_dgbas_cpi","twpub_mof_")) else RULES["daily"],scope="market")
            data=macro_preopen_sessions(data,sessions).filter(pl.col("_preopen_session").is_not_null()& (pl.col("published_at_taipei").str.slice(0,10).str.to_date()>=sessions[0]))
            table=data.select(pl.col("_preopen_session").alias("date"),pl.lit("*").alias("symbol"),pl.lit(feature).alias("feature"),
                pl.col("source_value").alias("value"),pl.col("subject_period").alias("period"))
            commit(table,[{"feature":feature,"source":"tw-native-macro","rule":asdict(rule),"clock":"native_published_at_preopen_v2",
                "publication_time_estimated":True,"scope":"market","value_vintage":"current_source_revision"}],name)
    for spec in manifest["feature_specs"]:
        spec=public_coordinate_null_spec(spec)
        name=spec["feature"];rule=rule_from_spec(spec)
        if spec.get("source_column") in macro_names:
            aliases.append({"feature":name,"source":spec.get("source_column"),"reason":"native_observations_replace_already_carried_daily_view"});continue
        if spec["source"]=="FinLab":
            table,_=normalize_source(source/spec["path"],spec["dataset"],sessions,symbols,uploads,
                publication_bounds=publication_bounds,quality_rows=quality)
            period=pl.col("source_index").cast(pl.String) if "source_index" in table.columns else pl.col("date").cast(pl.String)
            table=table.select("date","symbol",pl.lit(name).alias("feature"),pl.col(name).alias("value"),period.alias("period"))
            if rule.kind=="quarter":table=table.with_columns(pl.col("period").str.replace_all("-","").alias("period"))
            if rule.kind=="quarter_date":
                stamp=pl.col("period").str.slice(0,10).str.to_date()
                table=table.with_columns(pl.concat_str(
                    (stamp.dt.year()-(stamp.dt.month()==3).cast(pl.Int32)).cast(pl.String),pl.lit("Q"),
                    stamp.dt.month().replace_strict({3:4,5:1,8:2,11:3}).cast(pl.String)).alias("period"))
                key=spec["dataset"].replace("financial_statement_revised:","financial_statement:",1)
                if key in primary_parts:
                    primary=pl.concat([pl.read_parquet(p) for p in primary_parts[key]])
                    candidate,_=collapse_conflicting_observations(table,["period","symbol","feature"])
                    fills,comparison,_=missing_only(economic_primary(primary),candidate.select("period","symbol","value"))
                    quality.append({"source":"FinLab_revised_to_period","dataset":key,**comparison})
                    if comparison["accepted"]:
                        primary_feature=primary["feature"][0]
                        aliases.append({"source":spec["dataset"],"feature":primary_feature,"reason":"same_provider_financial_field_and_verified_original_period_values; missing_only_union","comparison":comparison})
                        if fills.height:
                            fill=fills.join(table.select("period","symbol","date"),on=["period","symbol"],validate="1:1").with_columns(pl.lit(primary_feature).alias("feature"))
                            commit(fill.select("date","symbol","feature","value","period"),[definitions[primary_feature]],"revised missing-only "+key)
                        continue
        else:
            table=pl.scan_parquet(public).select("date","symbol",pl.col(spec["source_column"]).alias("value")).collect(engine="streaming")
            if rule.scope=="market":
                calendar=table.select("date").unique()
                table,bad=collapse_conflicting_observations(table.filter(pl.col("value").is_finite()),["date"])
                if bad:raise ValueError("declared shared feature differs across stock rows")
                table=calendar.join(table,on="date",how="left",validate="1:1")
                table=table.with_columns(pl.lit("*").alias("symbol"))
            table=table.join(source_clock_lookup(sessions,spec["clock"]),left_on="date",right_on="source_date",how="inner",suffix="_ready",validate="m:1")
            table=table.select(pl.col("date_ready").alias("date"),"symbol",pl.lit(name).alias("feature"),"value",pl.col("date").cast(pl.String).alias("period"))
        commit(table,[{**spec,"scope":rule.scope}],name)
    for adapter in manifest.get("native_adapters",[]):
        kind=adapter["kind"]
        if kind=="mops_financial_facts":
            candidates=pl.read_parquet(source/adapter["publication_candidates"])
            taxonomy=json.loads((source/manifest["taxonomy_dictionary"]).read_text()) if manifest.get("taxonomy_dictionary") else None
            for relative in adapter["paths"]:
                raw,evidence=mops_native(source/relative,candidates,symbols)
                table,specs,work=mops_admission(raw,sessions,taxonomy);excluded.extend(work)
                if table.is_empty():continue
                table=quarter_clock(table.drop("proxy_ready"),sessions,uploads)
                reset=table.select("date","symbol","period",pl.concat_str(pl.lit("MOPS:"),pl.col("report_basis")).alias("report_group")).unique()
                resets.append(reset)
                commit(table.select("date","symbol","feature","value","period"),specs,"MOPS "+evidence["archive_period"])
        elif kind=="finmind_financial_facts":
            for dataset in sorted({p["dataset"] for p in adapter["members"]}):
                paths=[source/p["path"] for p in adapter["members"] if p["dataset"]==dataset]
                chunks=[pl.scan_parquet(paths[i:i+128]).collect(engine="streaming") for i in range(0,len(paths),128)]
                native=pl.concat(chunks);consumed=set()
                for key,mapping in MAPPINGS.items():
                    if mapping.dataset!=dataset or key not in primary_parts:continue
                    primary=pl.concat([pl.read_parquet(p) for p in primary_parts[key]])
                    if primary["period"].str.contains(r"^20\d\dQ[1-4]$").all() is not True:continue
                    candidate,source_quality=normalize_finmind(native,mapping)
                    if candidate.is_empty():continue
                    fills,comparison,_=missing_only(economic_primary(primary),candidate)
                    quality.append({"source":"FinMind_to_FinLab","dataset":key,**source_quality,**comparison})
                    if not comparison["accepted"]:continue
                    feature=primary["feature"][0];consumed.add(mapping.field)
                    aliases.append({"source":dataset+":"+mapping.field,"feature":feature,
                        "reason":"registered_economic_mapping_and_original_period_unit_agreement; missing_only_union",
                        "factor":mapping.factor,"transform":mapping.transform,"comparison":comparison})
                    if fills.height:
                        fill=quarter_clock(fills,sessions,uploads).with_columns(pl.lit(feature).alias("feature"))
                        commit(fill.select("date","symbol","feature","value","period"),[definitions[feature]],"missing-only "+key)
                table=native.filter(~pl.col("type").is_in(sorted(consumed))).with_columns(pl.col("date").str.to_date().alias("_period"))
                if table.is_empty():continue
                table=table.with_columns(pl.concat_str(pl.col("_period").dt.year().cast(pl.String),pl.lit("Q"),pl.col("_period").dt.quarter().cast(pl.String)).alias("period"),pl.col("stock_id").alias("symbol"))
                # Code + native dataset + period basis are the quantity key;
                # duplicate translated origin_name is NOT an alias proof.
                specs=[];mapping=[]
                for code in sorted(table["type"].unique()):
                    identity={"provider":"FinMind","dataset":dataset,"code":code,"unit":"percent" if code.endswith("_per") else "native_provider_unit_not_cross_source_conversion",
                        "fiscal_basis":"instant" if "BalanceSheet" in dataset else "ytd" if "CashFlows" in dataset else "provider_reported_duration"}
                    feature=native_feature_name(identity);mapping.append({"type":code,"feature":feature})
                    specs.append({"feature":feature,"source":"FinMind","identity":identity,"scope":"stock","rule":asdict(RULES["quarter"]),
                        "clock":"known_upload_or_quarter_proxy","publication_time_estimated":True,"report_group":"FinMind:"+dataset,
                        "labels":sorted(table.filter(pl.col("type")==code)["origin_name"].drop_nulls().unique().to_list())})
                table=table.join(pl.DataFrame(mapping),on="type",validate="m:1")
                table=quarter_clock(table,sessions,uploads)
                resets.append(table.select("date","symbol","period",pl.lit("FinMind:"+dataset).alias("report_group")).unique())
                commit(table.select("date","symbol","feature","value","period"),specs,dataset)
        elif kind in {"fred_events","free_public_events","etf_events"}:
            table=pl.read_parquet(source/adapter["path"])
            axes={"fred_events":["series_id"],"free_public_events":["source","dataset","entity","metric","unit"],"etf_events":["provider","ticker","asset","metric","unit"]}[kind]
            value="value_float" if kind=="free_public_events" else "value"
            period="observation_date" if kind=="fred_events" else "event_ts_utc" if kind=="free_public_events" else "event_date"
            if kind=="free_public_events":
                # Filter identifiers/text before creating one feature per entity.
                rejected=[]
                for row in table.select("source","dataset","metric","unit").unique().iter_rows(named=True):
                    role,reason=semantic_role({"field":row["metric"],"expansion_kind":"free_public_metric","source_unit":row["unit"]})
                    if role!="economic_measure":
                        rejected.append(row);excluded.append({**row,"source":kind,"reason":"not_numeric_economic_measure: "+reason})
                if rejected:table=table.join(pl.DataFrame(rejected,schema={c:table.schema[c] for c in rejected[0]}),on=list(rejected[0]),how="anti",nulls_equal=True)
            table=table.filter(pl.col(value).is_finite())
            # There are no training-owned observations in a late-only snapshot;
            # keep its exact source evidence, not thousands of zero-only inputs.
            availability=pl.col("available_at_utc").str.to_datetime(time_zone="UTC",strict=False).dt.convert_time_zone("Asia/Taipei").dt.date()
            late=table.group_by(axes).agg(availability.min().alias("first_available"))
            late=late.filter(pl.col("first_available")>training_cutoff)
            for row in late.iter_rows(named=True):excluded.append({"source":kind,"identity":{a:row[a] for a in axes},"first_available":str(row["first_available"]),"reason":"no_training_owned_history_in_any_canonical_fold; source retained, never backdated"})
            if late.height:table=table.join(late.select(axes),on=axes,how="anti",nulls_equal=True)
            for key,part in table.partition_by(axes,as_dict=True).items():
                identity=dict(zip(axes,key));feature=native_feature_name({"provider":kind,**identity})
                available=part["available_at_utc"].cast(pl.String)
                dates_map={stamp:first_ready_session(stamp,sessions) for stamp in available.drop_nulls().unique()}
                part=part.with_columns(available.replace_strict(dates_map,default=None,return_dtype=pl.Date).alias("_ready"))
                data=part.filter(pl.col("_ready").is_not_null())
                if data.is_empty():
                    excluded.append({"feature":feature,"source":kind,"identity":identity,"reason":"native_availability_outside_historical_training_calendar_not_backdated"});continue
                rule=global_rule(kind,identity)
                commit(data.select(pl.col("_ready").alias("date"),pl.lit("*").alias("symbol"),pl.lit(feature).alias("feature"),
                    pl.col(value).alias("value"),pl.col(period).cast(pl.String).alias("period")),
                    [{"feature":feature,"source":kind,"identity":identity,"rule":asdict(rule),"scope":"market","clock":"first_preopen_after_native_available_at_no_double_safety_delay"}],kind)
        elif kind=="us_economic_programs":
            excluded.extend({**p,"reason":"native_program_release_clock_not_yet_verified_no_period_end_plus1d_guess"} for p in adapter["members"])
    for writer in annual_writers.values():writer.close()
    finite=set(pl.scan_parquet(parts).filter(pl.col("value").is_not_null()&(pl.col("date")<=training_cutoff)).select("feature").unique().collect(engine="streaming")["feature"])
    for name in list(definitions):
        if name not in finite:excluded.append({"feature":name,"reason":"no_non_null_training_owned_observation_in_any_canonical_fold"});del definitions[name]
    individual=sorted((d for d in definitions.values() if rule_from_spec(d).scope=="stock"),key=lambda d:d["feature"])
    common=sorted((d for d in definitions.values() if rule_from_spec(d).scope=="market"),key=lambda d:d["feature"])
    events=AnnualObservationEvents(list(annual_paths.values()))
    channels=lambda defs:[channel for d in defs for channel in ((d["feature"],) if channel_count == 1 else (d["feature"],d["feature"]+"__available",d["feature"]+"__age_days",d["feature"]+"__updated"))]
    # Lifecycle starts follow the current ordinary stock source's explicit
    # episode key, including pre-panel history, not first observation of X.
    lifecycle=np.full((len(dates),len(symbols)),1000000,dtype=np.int32)
    dmap={d:i for i,d in enumerate(dates)}
    for s,symbol in enumerate(symbols):
        raw=pl.read_parquet(source/"stocks"/f"{symbol}_features.parquet",columns=["date","lifecycle_episode_id"])
        raw=raw.with_columns(pl.col("date").cast(pl.Date))
        starts=raw.group_by("lifecycle_episode_id").agg(pl.col("date").min().alias("start"))
        for day,start in raw.join(starts,on="lifecycle_episode_id",validate="m:1").select("date","start").iter_rows():
            if day in dmap:lifecycle[dmap[day],s]=start.toordinal()
    reset_table=pl.concat(resets).unique() if resets else None
    blocks=[];individual_counts=None
    date_budget=min(4*1024**3,max_block_bytes*max(1,len(symbols)//128))
    for begin,values,counts in causal_state_blocks(dates,symbols,individual,events,lifecycle,resets=reset_table,max_block_bytes=date_budget,column_blocks=True,model_channel_policy=model_channel_policy):
        symbol_blocks=[]
        for symbol_start,columns,part in values.symbol_blocks:
            proof=write_array_block(out/f"individual_{begin:05d}_{symbol_start:05d}.npy.zst",part,omit_zero_columns=True,
                logical_columns=columns,logical_shape=(values.shape[0],part.shape[1],values.shape[2]))
            proof["symbol_start"]=symbol_start;symbol_blocks.append(proof)
        proof={"start":begin,"shape":list(values.shape),"symbol_blocks":symbol_blocks,"bytes":sum(p["bytes"] for p in symbol_blocks)}
        blocks.append(proof);individual_counts=counts
        print(f"[factorized-panel] block {begin}:{begin+values.shape[0]} {proof['bytes']:,} compressed bytes",flush=True)
    common_values=[];common_counts=None
    for _,values,counts in causal_state_blocks(dates,["*"],common,events,None,max_block_bytes=max_block_bytes,model_channel_policy=model_channel_policy):
        common_values.append(values[:,0,:]);common_counts=counts
    shared=np.concatenate(common_values,axis=0);np.save(out/"common.npy",shared,allow_pickle=False)
    feature_dictionary=out/"feature_dictionary.json";atomic_write_json(feature_dictionary,{"features":[*individual,*common],"aliases":aliases,"excluded":excluded})
    atomic_write_json(out/"quality_masks.json",quality)
    write_csv(out/"feature_coverage.csv",[{"feature":d["feature"],"source":d["source"],"scope":scope,"available_panel_cells":int(count)}
        for scope,defs,counts in [("stock",individual,individual_counts),("market",common,common_counts)] for d,count in zip(defs,counts)])
    result={"contract":VALUE_ONLY_CONTRACT if channel_count == 1 else CONTRACT,"status":"complete","source_snapshot_id":snapshot_id,"source_manifest_sha256":source_sha,
        "model_channel_policy":model_channel_policy,
        "research_only":True,"historical_point_in_time":False,"live_eligible":False,"use_restriction":PRIVATE_USE,
        "feature_lag":1,"raw_missingness":"NULL; coordinate table absent rows are not fabricated observations",
        "training_missingness":("last already published finite value; missing input encoded as neutral zero; observation metadata excluded from model" if channel_count == 1 else "last already published finite value + availability + age")+"; NULL report barriers, TTL, report and issuer lifecycle reset",
        "symbols":symbols,"dates":[str(d) for d in dates],"base_feature_names":panel.feature_names,
        "individual_channels":channels(individual),"common_channels":channels(common),"blocks":blocks,
        "common":{"path":"common.npy","sha256":sha256(out/"common.npy"),"shape":list(shared.shape),"bytes":(out/"common.npy").stat().st_size},
        "feature_dictionary_sha256":sha256(feature_dictionary),"observations":[{"path":str(p.relative_to(out)),"sha256":sha256(p),"rows":pl.scan_parquet(p).select(pl.len()).collect().item()} for p in parts],
        "annual_events":[{"path":str(p.relative_to(out)),"sha256":sha256(p)} for p in sorted(annual_paths.values())],
        "value_features":len(definitions),"individual_quantities":len(individual),"shared_quantities":len(common),
        "logical_model_channels":len(panel.feature_names)+channel_count*len(definitions),"host_cache_bytes":2*1024**3,"max_slab_bytes":12*1024**3,
        "training_owned_admission_cutoff":str(training_cutoff),"admission_scope":"union of canonical folds; per-fold causal RMS still independently masks unavailable features",
        "storage_codec":"lossless Float32 ZSTD; block-local positive-zero columns elided and restored before ordinary dense model arithmetic",
        "feature_view_ready":True,"training_ready":False,"gpu_training_verified":False,"execution_preflight_passed":False,
        "build_wall_seconds":time.perf_counter()-started,"builder_sha256":sha256(Path(__file__))}
    atomic_write_json(out/"factorized_manifest.json",result)
    write_factorized_feature_report(out/"factorized_manifest.json",out/"feature_report")
    generated["data"]["factorized_feature_manifest"]=str((out/"factorized_manifest.json").resolve())
    config_path.write_text(yaml.safe_dump(generated,allow_unicode=True,sort_keys=False))
    if sha256(source/"source_manifest.json")!=source_sha:raise ValueError("immutable source manifest changed")
    atomic_write_json(out/"preparation_profile.json",{"full_workflow_wall_s":time.perf_counter()-started,
        "manifest_sha256":sha256(out/"factorized_manifest.json"),"report_receipt_sha256":sha256(out/"feature_report/report_receipt.json"),
        "includes_source_verification_execution_panel_observations_nullable_view_reports":True})
    return {k:v for k,v in result.items() if k not in {"symbols","dates","individual_channels","common_channels","blocks","observations"}}


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--output-root",type=Path,required=True)
    for arg in ["source-root","base-config","minute-root"]:p.add_argument("--"+arg,type=Path)
    p.add_argument("--snapshot-id");p.add_argument("--max-block-mib",type=int,default=128)
    p.add_argument("--base-panel-cache-root",type=Path,help="Canonical cache only; original fingerprint gates still apply")
    p.add_argument("--model-channel-policy",choices=("value_available_age_updated","value_only"),default="value_available_age_updated")
    p.add_argument("--project-prepared-manifest",type=Path,help="Incrementally derive value-only blocks from an already verified four-channel panel")
    a=p.parse_args()
    if a.project_prepared_manifest is not None:
        if a.model_channel_policy != "value_only":p.error("prepared projection requires --model-channel-policy value_only")
        if any(v is not None for v in (a.source_root,a.base_config,a.minute_root,a.snapshot_id,a.base_panel_cache_root)):
            p.error("prepared projection does not download/rebuild provider or execution sources")
        from stockagent.data.factorized_panel_projection import project_factorized_values
        result=project_factorized_values(a.project_prepared_manifest,a.output_root)
    else:
        if any(v is None for v in (a.source_root,a.base_config,a.minute_root,a.snapshot_id)):
            p.error("source-root, base-config, minute-root and snapshot-id are required for a new source build")
        result=prepare(a.source_root,a.output_root,a.base_config,a.minute_root,a.snapshot_id,
            max_block_bytes=a.max_block_mib*1024**2,base_panel_cache_root=a.base_panel_cache_root,
            model_channel_policy=a.model_channel_policy)
    print(json.dumps(result,ensure_ascii=False))


if __name__=="__main__":main()

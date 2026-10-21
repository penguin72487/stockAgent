"""Offline, full-dictionary daily-owner acceptance from captured real sources.

Captures exact local sources under the canonical read lease, then releases the
production tree. Runs the original prepare() for prior/new completed sessions
and a fresh full new-session oracle. No provider, inference, broker, deployment
or production publication. Cutoff fixtures are retrospective subsets, NOT PIT
releases or evidence that a historical signal existed.
"""
from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import UTC, date, datetime
import hashlib
import io
import json
import os
from pathlib import Path
import resource
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if __name__ == "__main__":
    from stockagent.data.source_processing_runtime import configure_cpu_source_runtime
    configure_cpu_source_runtime(2)

from downloader.artifact_io import atomic_write_json
from scripts import prepare_tw_day_trade_factorized_live as owner
from scripts.benchmark_factorized_live_owner import shared_identity
from scripts.benchmark_live_base_panel_cutoff import panel_identity, source_members
from stockagent.config import load_config
from stockagent.data.factorized_panel import FactorizedPanelFeatures, file_sha256, verify_factorized_members
from stockagent.data.finlab_acquisition_contract import read_receipt_bound_source, safe_stem
from stockagent.data.tw_public_features import _exchange_session_dates
from stockagent.data_sync.cache_capacity import GenerationWriteBudget, load_capacity_policy, load_prepared_capacity_policy, reserve_cache_write
from stockagent.data_sync.desync_snapshots import _fsync_directory
from stockagent.data_sync.factorized_projection_reuse import seal_new_file
from stockagent.data_sync.immutable_cache_files import payload_info, stable_payload_hash
from stockagent.live.tw_public_source_guard import live_tw_public_source_read_guard


def _copy(source, target, budget):
    before = payload_info(source)
    digest = stable_payload_hash(source, before)
    target.parent.mkdir(parents=True, exist_ok=True)
    with source.open("rb") as src, io.BufferedWriter(budget.open(target), buffer_size=1024**2) as dst:
        while chunk := src.read(1024**2):
            dst.write(chunk)
    if payload_info(source) != before or file_sha256(target) != digest:
        raise ValueError(f"source changed during private capture: {source}")
    seal_new_file(target)
    return {"path": str(target), "source": str(source), "sha256": digest, "bytes": before["size"]}


def _link(source, target):
    """Only a private, already sealed captured inode; never seal a source link."""
    info = payload_info(source)
    if info["mode"] & 0o222 or info["uid"] != os.geteuid():
        raise ValueError("working fixtures require a sealed private capture")
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f".{target.name}.{uuid.uuid4().hex}.tmp")
    try:
        os.link(source, temporary, follow_symlinks=False)
        os.replace(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)


def capture(root, *, public, finlab, parent, native, config):
    import pyarrow.parquet as pq
    public, finlab, parent, native, config = (path.resolve(strict=True) for path in (public, finlab, parent, native, config))
    fixed = json.loads(parent.read_bytes())
    dictionary = parent.parent / "feature_dictionary.json"
    definitions = {v["feature"]: v for v in json.loads(dictionary.read_bytes())["features"]}
    pairs = [(parent, root / "parent/factorized_manifest.json"),
             (dictionary, root / "parent/feature_dictionary.json"),
             (config, root / "config.json")]
    for proof in fixed["annual_events"]:
        rel = Path(proof["path"])
        if rel.is_absolute() or ".." in rel.parts:
            raise ValueError("seed escapes parent")
        source = parent.parent / rel
        if not source.is_file():
            source = parent.parent / rel.name
        if file_sha256(source) != proof["sha256"]:
            raise ValueError("fixed seed changed")
        pairs.append((source, root / "parent" / rel))
    context = json.loads((native / "source_manifest.json").read_bytes())
    for name in ("source_manifest.json", "finlab/known_uploads.parquet",
                 context.get("source_repairs", {}).get("publication_lower_bounds")):
        if name:
            rel = Path(name)
            if rel.is_absolute() or ".." in rel.parts:
                raise ValueError("native context escapes source")
            if (native / rel).is_file():
                pairs.append((native / rel, root / "native" / rel))
    for name in fixed["individual_channels"] + fixed["common_channels"]:
        definition = definitions[name]
        if definition.get("source") == "FinLab":
            key = definition["dataset"]
            receipt_path = finlab / "receipts" / f"{safe_stem(key)}.json"
            raw = receipt_path.read_bytes()
            source, receipt = read_receipt_bound_source(finlab, key, receipt_path)
            if json.loads(raw) != receipt:
                raise ValueError("FinLab head changed while capturing; retry original owner")
            pairs.extend([(receipt_path, root / "finlab/receipts" / receipt_path.name),
                          (source, root / "finlab" / receipt["parquet_path"])])
    config_data = load_config(config).data
    recipe = {"data": {**asdict(config_data), "parquet_root": str(public / "stocks"),
                       "tw_public_feature_path": str(public / "features/tw_public_stock_daily.parquet")}}
    # Reuse the existing complete-owner dependency inventory. Do not copy
    # unrelated source datasets or the 45-GiB panel cache under stocks/.
    with live_tw_public_source_read_guard(public / "stocks"):
        _, base_members = source_members([recipe])
        public_paths = {*base_members,
                        public / "features/tw_public_stock_daily.parquet",
                        public / "features/tw_public_stock_daily.summary.json",
                        *(public / name for name in ("twse_taiex_ohlc.parquet", "twse_taiex_ohlc.summary.json",
                          owner.TW_HOLIDAY_SCHEDULE_NAME))}
        pairs.extend((path, root / "public_complete" / path.relative_to(public)) for path in sorted(public_paths))
        pairs = list(dict.fromkeys(pairs))
        allowance = sum(source.stat().st_size for source, _ in pairs) * 2 + len(pairs) * 262144 + 64 * 1024**2
        with reserve_cache_write(root, key=hashlib.sha256(b"factorized-real-input-capture-v1").hexdigest(),
                maximum_growth_bytes=allowance, family="prepared", policy=load_capacity_policy()), GenerationWriteBudget(allowance) as budget:
            proofs = [_copy(source, target, budget) for source, target in pairs]
        for proof in proofs:
            if file_sha256(Path(proof["path"])) != proof["sha256"]:
                raise ValueError("private captured bytes changed")
        if source_members([recipe])[1] != base_members:
            raise ValueError("complete BASE source membership changed during capture")
        _fsync_directory(root)
    summary = json.loads((root / "public_complete/features/tw_public_stock_daily.summary.json").read_bytes())
    latest = date.fromisoformat(summary["requested_end_date"])
    sessions = [day for day in _exchange_session_dates(root / "public_complete")["_session_date"].to_list()
                if day <= latest]
    if len(sessions) < 2 or sessions[-1] != latest:
        raise ValueError("real source has no two completed sessions")
    return {"contract": "captured_factorized_real_daily_sources_v1", "proofs": proofs,
            "prior_completed": str(sessions[-2]), "latest_completed": str(latest),
            "original_start": fixed["dates"][0], "symbols": len(fixed["symbols"]),
            "individual_channels": len(fixed["individual_channels"]), "common_channels": len(fixed["common_channels"]),
            "public_schema": str(pq.read_schema(root / "public_complete/features/tw_public_stock_daily.parquet")),
            "retrospective_subset_not_historical_pit": True}


def working_public(root, *, cutoff=None, capture_root=None):
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq
    source, target = (capture_root or root) / "public_complete", root / "public_work"
    target.mkdir(parents=True, exist_ok=True)
    paths = [path for path in source.rglob("*") if path.is_file()]
    summary = json.loads((source / "features/tw_public_stock_daily.summary.json").read_bytes())
    counts = {"rows": 0, "stock_rows": 0, "market_rows": 0}
    allowance = sum(path.stat().st_size for path in paths) * 2 + len(paths) * 262144
    with reserve_cache_write(target, key=hashlib.sha256(f"cutoff:{cutoff}".encode()).hexdigest(),
            maximum_growth_bytes=allowance, family="prepared", policy=load_capacity_policy()), GenerationWriteBudget(allowance) as budget:
        for path in paths:
            relative = path.relative_to(source)
            destination = target / relative
            if cutoff is None or not (relative.parts[0] == "stocks" and path.suffix == ".parquet"
                                      or relative == Path("features/tw_public_stock_daily.parquet")):
                _link(path, destination)
                continue
            destination.parent.mkdir(parents=True, exist_ok=True)
            temporary = destination.with_name(f".{destination.name}.{uuid.uuid4().hex}.tmp")
            parquet = pq.ParquetFile(path)
            schema = parquet.schema_arrow
            metadata = dict(schema.metadata or {})
            if relative.parts[0] == "stocks":
                # Individual BASE files are small. Read/filter once so range
                # metadata describes actual observations, not the cutoff or a
                # delisted issuer's fictitious latest trade.
                table = parquet.read(use_threads=False)
                table = table.filter(pc.less_equal(table["date"], pa.scalar(cutoff)))
                metadata[b"stockagent.official_checked_through"] = str(cutoff).encode()
                for key, operation in ((b"stockagent.first_date", pc.min), (b"stockagent.last_date", pc.max)):
                    value = operation(table["date"]).as_py()
                    if value is None:
                        metadata.pop(key, None)
                    else:
                        metadata[key] = value.isoformat().encode()
                tables = (table,)
            else:
                tables = (pa.Table.from_batches([batch]).filter(
                    pc.less_equal(batch.column("date"), pa.scalar(cutoff)))
                    for batch in parquet.iter_batches(batch_size=64000, use_threads=False))
            schema = schema.with_metadata(metadata)
            with io.BufferedWriter(budget.open(temporary), buffer_size=1024**2) as stream:
                with pq.ParquetWriter(stream, schema, compression="snappy") as writer:
                    for table in tables:
                        if table.num_rows:
                            if relative.parts[0] == "features":
                                if table["symbol"].null_count:
                                    raise ValueError("diagnostic public feature source has a null symbol")
                                market_rows = pc.sum(pc.equal(table["symbol"], summary.get("market_symbol", "__MARKET__"))).as_py() or 0
                                counts["rows"] += table.num_rows
                                counts["market_rows"] += market_rows
                                counts["stock_rows"] += table.num_rows - market_rows
                            writer.write_table(table.replace_schema_metadata(schema.metadata))
            seal_new_file(temporary)
            os.replace(temporary, destination)
        if cutoff is not None:
            path = target / "features/tw_public_stock_daily.parquet"
            summary_path = target / "features/tw_public_stock_daily.summary.json"
            if counts["rows"] != pq.ParquetFile(path).metadata.num_rows:
                raise ValueError("diagnostic public feature row reconciliation failed")
            summary.update(requested_end_date=str(cutoff), **counts,
                           output_receipt={"name": path.name, "size": path.stat().st_size, "sha256": file_sha256(path)},
                           diagnostic_fixture={"cutoff": str(cutoff), "retrospective_subset_not_historical_pit": True})
            budget.charge_json(summary)
            atomic_write_json(summary_path, summary)
            seal_new_file(summary_path)
        _fsync_directory(target / "stocks")
        _fsync_directory(target / "features")
        _fsync_directory(target)


def compare_complete(first, second):
    import numpy as np
    manifests = [json.loads(Path(result["manifest"]).read_bytes()) for result in (first, second)]
    for name in ("dates", "symbols", "base_feature_names", "individual_channels", "common_channels", "model_channel_policy"):
        if manifests[0][name] != manifests[1][name]:
            raise ValueError(f"daily/full axes differ: {name}")
    n, s, b = len(manifests[0]["dates"]), len(manifests[0]["symbols"]), len(manifests[0]["base_feature_names"])
    base = np.zeros((n, s, b), dtype=np.float32)
    arrays = []
    for result, manifest in zip((first, second), manifests, strict=True):
        root = Path(result["manifest"]).parent
        verify_factorized_members(root, manifest)
        arrays.append(FactorizedPanelFeatures(root, manifest, base))
    digest = hashlib.sha256()
    for begin in range(0, n, 4):
        a, c = (array[begin:min(begin+4, n)] for array in arrays)
        if not np.array_equal(a.view(np.uint32), c.view(np.uint32)):
            raise ValueError(f"all-cell daily/full bit oracle failed at {begin}")
        digest.update(a.tobytes())
    return {"every_individual_common_value_bit_equal": True, "dates": n, "symbols": s,
            "individual_common_channels": len(manifests[0]["individual_channels"]) + len(manifests[0]["common_channels"]),
            "base_channels_zeroed_for_projection_only_oracle": True, "all_cell_sha256": digest.hexdigest()}


def benchmark(args):
    import requests
    import urllib.request
    import torch
    import pyarrow as pa
    pa.set_cpu_count(2)

    def forbidden(*_args, **_kwargs):
        raise AssertionError("offline daily acceptance cannot call a provider or CUDA")

    if torch.cuda.is_initialized():
        raise ValueError("CPU benchmark already initialized CUDA")
    requests.sessions.Session.request = forbidden
    urllib.request.urlopen = forbidden
    torch.cuda._lazy_init = forbidden
    root = args.output.absolute()
    if root.exists() or root.resolve() != root or not root.is_relative_to(ROOT / "artifacts/benchmarks"):
        raise ValueError("fresh private benchmark output required")
    root.mkdir(parents=True, mode=0o700)
    codes = {**shared_identity(), str(Path(__file__)): file_sha256(Path(__file__)),
             **{str(ROOT / name): file_sha256(ROOT / name) for name in (
                 "stockagent/data/partitioned_parquet.py", "stockagent/data/causal_projection_state.py",
                 "stockagent/data/factorized_normalization_reuse.py", "stockagent/data_sync/cache_capacity.py",
                 "stockagent/data_sync/factorized_projection_reuse.py")}}
    result = {"contract": "complete_factorized_daily_owner_benchmark_v1", "started_at_utc": datetime.now(UTC).isoformat(),
              "provider_calls": 0, "production_writes": False, "service_restarts": 0, "code_sha256": codes, "stages": {}}

    def run_owner(output, options):
        base_proofs = []
        original = owner.subset_panel_symbols

        def record(*args, **kwargs):
            panel = original(*args, **kwargs)
            base_proofs.append(panel_identity(panel))
            return panel

        owner.subset_panel_symbols = record
        try:
            with reserve_cache_write(root, key=hashlib.sha256(str(output).encode()).hexdigest(),
                    maximum_growth_bytes=load_prepared_capacity_policy()["maximum_generation_bytes"],
                    family="prepared", policy=load_capacity_policy()):
                value = owner.prepare(**options, output=output)
            return {**value, "complete_base_oracles": base_proofs, "base_oracle_cost_included": True}
        finally:
            owner.subset_panel_symbols = original

    def measured(name, operation):
        wall, cpu = time.perf_counter(), time.process_time()
        value = operation()
        result["stages"][name] = {"wall_seconds": time.perf_counter()-wall, "cpu_seconds": time.process_time()-cpu, "result": value}
        atomic_write_json(root / "progress.json", result)
        print(json.dumps({"stage": name, "wall_seconds": result["stages"][name]["wall_seconds"]}), flush=True)
        return value

    captured = args.capture_root.resolve(strict=True) if args.capture_root is not None else root
    if args.capture_root is not None:
        def reuse_capture():
            snapshot = json.loads((captured / "progress.json").read_bytes())["stages"]["capture_real_sources"]["result"]
            if snapshot["contract"] != "captured_factorized_real_daily_sources_v1":
                raise ValueError("retained real-source capture contract differs")
            for proof in snapshot["proofs"]:
                path = Path(proof["path"])
                if (not path.resolve().is_relative_to(captured) or payload_info(path)["mode"] & 0o222
                        or file_sha256(path) != proof["sha256"]):
                    raise ValueError("retained real-source capture is incomplete or changed")
            return snapshot
        snapshot = measured("reuse_captured_real_sources", reuse_capture)
    else:
        snapshot = measured("capture_real_sources", lambda: capture(root, public=args.public_root, finlab=args.finlab_root,
            parent=args.parent_manifest, native=args.native_source, config=args.config))
    measured("retrospective_prior_fixture", lambda: working_public(root, capture_root=captured,
        cutoff=date.fromisoformat(snapshot["prior_completed"])))
    kwargs = dict(config_path=captured / "config.json", parent_path=captured / "parent/factorized_manifest.json",
        native_source=captured / "native", public=root / "public_work", finlab=captured / "finlab", start_date=snapshot["original_start"],
        base_cache_root=root / "base_cache", annual_cache_root=root / "annual_cache")
    measured("prior_full_owner", lambda: run_owner(root / "incremental", kwargs))
    measured("restore_latest_real_fixture", lambda: working_public(root, capture_root=captured))
    daily = measured("new_session_complete_owner", lambda: run_owner(root / "incremental", kwargs))
    full = measured("fresh_full_latest_owner", lambda: run_owner(root / "full_oracle", {**kwargs, "annual_cache_root": root / "oracle_annual_cache"}))
    if not daily["complete_base_oracles"] or daily["complete_base_oracles"] != full["complete_base_oracles"]:
        raise ValueError("complete executable BASE arrays/masks differ")
    measured("complete_independent_full_output_oracle", lambda: compare_complete(daily, full))
    measured("unchanged_full_integrity_owner", lambda: run_owner(root / "incremental", kwargs))
    for proof in snapshot["proofs"]:
        if file_sha256(Path(proof["path"])) != proof["sha256"]:
            raise ValueError("retained captured source changed during acceptance")
    if any(file_sha256(Path(path)) != digest for path, digest in codes.items()):
        raise ValueError("daily owner implementation changed during acceptance")
    result.update(status="accepted_real_source_daily_owner_not_pit_live_or_inference_readiness",
                  rss_process_peak_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024)
    atomic_write_json(root / "receipt.json", result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--capture-root", type=Path, help="Reuse only a complete, independently SHA-verified earlier input capture")
    parser.add_argument("--public-root", type=Path, default=Path("/srv/stockagent-live/data_tw_public"))
    parser.add_argument("--finlab-root", type=Path, default=ROOT / "data_finlab")
    parser.add_argument("--native-source", type=Path, default=Path("/srv/stockagent-live/data_tw_daytrade_panel_sources_20261004_gaprepair_v4"))
    parser.add_argument("--parent-manifest", type=Path, default=ROOT / "artifacts/data_preparation/tw_factorized_fold11_live_parent_20261013/factorized_manifest.json")
    parser.add_argument("--config", type=Path, default=ROOT / "configs/deployments/tw_day_trade_factorized_scale_separated_cash_fold11_candidate_v1.yaml")
    args = parser.parse_args()
    benchmark(args)


if __name__ == "__main__":
    main()

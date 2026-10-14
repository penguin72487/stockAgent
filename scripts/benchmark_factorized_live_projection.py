"""Frozen, complete causal-sweep/write/validation/publication CPU benchmark.

Use an already published local projection as the independent all-cell oracle.
Retain the original implementations before testing copies; never patch the live
owner, query a provider, initialize CUDA or publish a production generation.
The denominator excludes upstream FinLab normalization/base-panel preparation
and model inference. Those require their own whole-owner acceptance.
"""
from __future__ import annotations

import argparse
import ast
from datetime import date, timedelta
import hashlib
import json
import os
from pathlib import Path
import shutil
import statistics
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_bytes, atomic_write_json  # noqa: E402
from scripts.benchmark_project_data_processing import load_baseline, measured  # noqa: E402
from stockagent.data.factorized_panel import file_sha256, verify_factorized_members  # noqa: E402
from stockagent.remote_build import observe_node  # noqa: E402

CONTRACT = "complete_factorized_live_causal_projection_benchmark_v1"
SOURCE = ROOT / "scripts/prepare_tw_day_trade_factorized_panel.py"
WRITER = ROOT / "stockagent/data/factorized_panel.py"
SLICE_HELPER = '''\n\ndef chronological_date_groups(table):
    if table.is_empty():
        return
    if table["date"].null_count():
        raise ValueError("observation release date is NULL")
    if not table["date"].is_sorted():
        table = table.sort("date", maintain_order=True)
    offset = 0
    for length, day in table["date"].rle().struct.unnest().iter_rows():
        yield day, table.slice(offset, length)
        offset += length
'''
PREPARED_ITERATOR = '''    def iter_date_groups(self,definitions,symbols):
        from stockagent.data.annual_event_cache import PreparedObservationCache
        if PREPARED_ANNUAL_CACHE_ROOT is None:
            raise ValueError("explicit private prepared cache root required")
        names=[d["feature"] for d in definitions];current={}
        state_keys={d["feature"]:d.get("report_group") or d["feature"] for d in definitions}
        cache=PreparedObservationCache(PREPARED_ANNUAL_CACHE_ROOT)
        PREPARED_ANNUAL_CACHE_INSTANCES.append(cache)
        for path in self.paths:
            for day,table in cache.iter_dates(path,names,symbols,_prepare_annual_cache_coordinates,
                    prepare_identity=PREPARED_ANNUAL_COORDINATES_IDENTITY):
                # A whole release date is indivisible. Only touched state keys
                # need prior floors; untouched entries in current never change.
                touched=table.select("symbol",pl.col("feature").replace_strict(state_keys)
                    .alias("_state_key")).unique()
                prior={key:current[key] for key in touched.iter_rows() if key in current}
                part=causal_report_events(table,definitions,period_order=self.period_order,current_periods=prior)
                current.update(prior)
                if not part.is_empty():yield day,part
'''
PREPARED_CAUSAL_ITERATOR = '''    def iter_date_groups(self,definitions,symbols):
        import hashlib
        from stockagent.data.annual_event_cache import PreparedObservationCache, PreparedAnnualCoordinates, PERIOD_SCHEMA
        if PREPARED_ANNUAL_CACHE_ROOT is None:
            raise ValueError("explicit private prepared cache root required")
        names=[d["feature"] for d in definitions];current={};prefix=[]
        subjects={rank:subject for subject,rank in self.period_order.items()}
        cache=PreparedObservationCache(PREPARED_ANNUAL_CACHE_ROOT)
        PREPARED_ANNUAL_CACHE_INSTANCES.append(cache)

        def prepare(path,names,symbols):
            table=_prepare_annual_cache_coordinates(path,names,symbols)
            if not table.is_empty():
                table=causal_report_events(table,definitions,period_order=self.period_order,current_periods=current)
            floors=pl.DataFrame([(symbol,key,subjects[rank]) for (symbol,key),rank in current.items()],
                schema=PERIOD_SCHEMA,orient="row").sort("symbol","state_key")
            return PreparedAnnualCoordinates(table,floors)

        def restore(generation,metadata):
            floors=cache.read_terminal_periods(generation,metadata)
            restored={(symbol,key):self.period_order[subject] for symbol,key,subject in floors.iter_rows()}
            current.clear();current.update(restored)
            prefix.append(metadata["identity"]["source_sha256"])

        for path in self.paths:
            # Prefix bytes + full definitions uniquely determine the incoming
            # floor. Global dense rank numbers can change when a later delta
            # inserts subjects, but their lexicographic relative order cannot.
            dependency=hashlib.sha256(json.dumps({"definitions":definitions,"prefix":prefix},
                sort_keys=True,separators=(",",":")).encode()).hexdigest()
            identity=PREPARED_ANNUAL_COORDINATES_IDENTITY+":"+PREPARED_POLICY_IDENTITY+":"+dependency
            yield from cache.iter_dates(path,names,symbols,prepare,prepare_identity=identity,on_prepared=restore)
'''


def new_private(path: Path) -> Path:
    path = path.absolute()
    if (path.exists() or path.is_symlink()
            or not path.resolve().is_relative_to((ROOT / "artifacts/benchmarks").resolve())):
        raise ValueError("new private benchmark path required")
    path.mkdir(parents=True, mode=0o700)
    return path


def private_cache_path(path: Path) -> Path:
    path = path.absolute()
    if not path.resolve().is_relative_to((ROOT / "artifacts/benchmarks").resolve()):
        raise ValueError("cache must stay in the private benchmark artifact tree")
    return path


def private_copy(source: Path, target: Path) -> dict:
    target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    before = source.stat()
    shutil.copyfile(source, target)
    digest = file_sha256(target)
    after = source.stat()
    if ((before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns)
            != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns)
            or file_sha256(source) != digest):
        raise ValueError(f"source changed while freezing: {source}")
    target.chmod(0o400)
    return {"path": str(target), "source": str(source), "sha256": digest,
            "bytes": target.stat().st_size}


def freeze(*, fixture: Path, parent_path: Path, projection_path: Path, public: Path) -> dict:
    import numpy as np
    import polars as pl
    from stockagent.data.tw_day_trade_mixed_frequency import rule_from_spec
    from stockagent.live.tw_public_source_guard import live_tw_public_source_read_guard

    parent_path, projection_path = parent_path.resolve(strict=True), projection_path.resolve(strict=True)
    parent, projection = (json.loads(path.read_bytes()) for path in (parent_path, projection_path))
    verify_factorized_members(projection_path.parent, projection)
    if (projection.get("model_channel_policy") != "value_only"
            or projection["individual_channels"] != parent["individual_channels"]
            or projection["common_channels"] != parent["common_channels"]
            or projection["symbols"] != parent["symbols"]
            or projection["paper_inference"]["status"] != "verified"):
        raise ValueError("complete selected original value-only projection required")
    dictionary = parent_path.parent / "feature_dictionary.json"
    if file_sha256(dictionary) != parent["feature_dictionary_sha256"]:
        raise ValueError("original trained dictionary changed")
    by_name = {d["feature"]: d for d in json.loads(dictionary.read_bytes())["features"]}
    definitions = [by_name[name] for name in parent["individual_channels"] + parent["common_channels"]]
    horizon = max(max(rule_from_spec(d).carry_days, rule_from_spec(d).older_carry_days) for d in definitions)
    days = [date.fromisoformat(day) for day in projection["dates"]]
    years = set(range((days[0] - timedelta(days=horizon)).year,
                      min(date.fromisoformat(parent["dates"][-1]),
                          date.fromisoformat(projection["paper_inference"]["decision_session"])).year + 1))
    seeds = [(parent_path.parent / Path(p["path"]).name, p["sha256"])
             for p in parent["annual_events"] if int(Path(p["path"]).stem) in years]
    if {int(path.stem) for path, _ in seeds} != years:
        raise ValueError("full seed horizon absent")
    members = [projection_path, dictionary, projection_path.parent / "delta_events.parquet",
               *(path for path, _ in seeds), projection_path.parent / projection["common"]["path"],
               *(projection_path.parent / p["path"] for b in projection["blocks"] for p in b["symbol_blocks"])]
    size = sum(path.stat().st_size for path in members)
    if shutil.disk_usage(ROOT).free < size + 30 * 1024**3:
        raise ValueError("retain 30 GiB reserve after freezing")
    fixture = new_private(fixture)
    proofs = [private_copy(SOURCE, fixture / "original_sweep.py"),
              private_copy(WRITER, fixture / "original_writer.py"),
              private_copy(ROOT / "scripts/prepare_tw_day_trade_factorized_live.py", fixture / "original_owner.py"),
              private_copy(parent_path, fixture / "parent_manifest.json"),
              private_copy(dictionary, fixture / "feature_dictionary.json")]
    event_paths = []
    for path, digest in seeds:
        proof = private_copy(path, fixture / "events" / path.name)
        if proof["sha256"] != digest:
            raise ValueError("seed differs from fixed parent receipt")
        event_paths.append(proof["path"])
        proofs.append(proof)
    delta = private_copy(projection_path.parent / "delta_events.parquet", fixture / "events/delta.parquet")
    proofs.append(delta)
    event_paths.append(delta["path"])
    for member in members:
        if member.parent == projection_path.parent and member.name != "delta_events.parquet":
            proofs.append(private_copy(member, fixture / "oracle" / member.name))
    lifecycle = np.full((len(days) + 1, len(projection["symbols"])), 1000000, dtype=np.int32)
    date_map = {day: row for row, day in enumerate(days)}
    stock_proofs = []
    with live_tw_public_source_read_guard(public / "stocks"):
        if (file_sha256(public / "features/tw_public_stock_daily.parquet")
                != projection["paper_inference"]["source_identity"]["public_sha"]):
            raise ValueError("published oracle and current public source cohort differ")
        for index, symbol in enumerate(projection["symbols"]):
            path = public / "stocks" / f"{symbol}_features.parquet"
            if not path.is_file():
                stock_proofs.append({"symbol": symbol, "present": False})
                continue
            digest = file_sha256(path)
            raw = pl.read_parquet(path, columns=["date", "lifecycle_episode_id"]).with_columns(pl.col("date").cast(pl.Date))
            starts = raw.group_by("lifecycle_episode_id").agg(pl.col("date").min().alias("start"))
            for day, start in raw.join(starts, on="lifecycle_episode_id", validate="m:1").select("date", "start").iter_rows():
                if day in date_map:
                    lifecycle[date_map[day], index] = start.toordinal()
            if file_sha256(path) != digest:
                raise ValueError("lifecycle source changed under canonical read lease")
            stock_proofs.append({"symbol": symbol, "present": True, "path": str(path), "sha256": digest})
    lifecycle[-1] = lifecycle[-2]
    np.save(fixture / "lifecycle.npy", lifecycle, allow_pickle=False)
    lifecycle_path = fixture / "lifecycle.npy"
    lifecycle_path.chmod(0o400)
    proofs.append({"path": str(lifecycle_path), "sha256": file_sha256(lifecycle_path), "bytes": lifecycle_path.stat().st_size})
    evidence = {"contract": CONTRACT, "status": "frozen", "fixture": str(fixture),
                "event_paths": event_paths, "proofs": proofs, "stock_lifecycle_sources": stock_proofs,
                "dates": projection["dates"], "decision": projection["paper_inference"]["decision_session"],
                "symbols": projection["symbols"], "individual": projection["individual_channels"],
                "common": projection["common_channels"], "frozen_bytes": sum(p["bytes"] for p in proofs),
                "upstream_normalization_included": False, "model_inference_included": False}
    atomic_write_json(fixture / "fixture.json", evidence)
    return {k: evidence[k] for k in ("status", "fixture", "frozen_bytes")}


def candidate_sources(sweep: str, writer: str, variant: str) -> tuple[str, str]:
    if variant not in {"baseline", "take", "slices", "slices_take", "prepared", "prepared_causal"}:
        raise ValueError("unknown exact candidate")
    if variant.startswith("prepared"):
        # Extract the saved canonical scan/deduplication, not a second policy.
        begin = "    def iter_date_groups(self,definitions,symbols):\n"
        end = "\n\ndef global_rule(kind,identity):"
        scan = '            table=pl.scan_parquet(path).filter(pl.col("feature").is_in(names)&pl.col("symbol").is_in(symbols)).collect(engine="streaming")\n'
        causal = "            table=causal_report_events(table,definitions,period_order=self.period_order,current_periods=current)\n"
        if any(sweep.count(marker) != 1 for marker in (begin, end, causal)):
            raise ValueError("saved original coordinate preparation no longer matches")
        block_start, block_end = sweep.index(begin), sweep.index(end)
        if sweep.count(scan) == 1:
            preparation = sweep[sweep.index(scan):sweep.index(causal)]
            preparation = preparation.replace("            if table.is_empty():continue\n",
                                              '            if table.is_empty():return table.drop("_source_part")\n')
            helper = ("\n\ndef _prepare_annual_cache_coordinates(path,names,symbols):\n"
                      + "\n".join(line[8:] for line in preparation.rstrip().splitlines())
                      + "\n    return table\n")
        else:
            # The production refactor extracts the SAME policy into one
            # helper. Keep independent frozen old fixtures usable, while new
            # fixtures extract the actual helper rather than copying policy.
            functions = [node for node in ast.parse(sweep).body if isinstance(node, ast.FunctionDef)
                         and node.name == "prepare_annual_coordinates"]
            if (len(functions) != 1 or functions[0].decorator_list
                    or [arg.arg for arg in functions[0].args.args] != ["path", "names", "symbols"]
                    or sweep.count("table=prepare_annual_coordinates(path,names,symbols)") != 1):
                raise ValueError("saved original coordinate preparation no longer matches")
            helper = "\n\n" + ast.get_source_segment(sweep, functions[0]).replace(
                "def prepare_annual_coordinates(", "def _prepare_annual_cache_coordinates(", 1) + "\n"
        identity = hashlib.sha256(sweep.encode()).hexdigest()
        policy = file_sha256(ROOT / "stockagent/data/tw_day_trade_mixed_frequency.py")
        iterator = PREPARED_CAUSAL_ITERATOR if variant == "prepared_causal" else PREPARED_ITERATOR
        sweep = (sweep[:block_start] + iterator + sweep[block_end:] + helper
                 + f'\nPREPARED_ANNUAL_COORDINATES_IDENTITY = "{identity}"\n'
                 + f'PREPARED_POLICY_IDENTITY = "{policy}"\n'
                 + "PREPARED_ANNUAL_CACHE_ROOT = None\nPREPARED_ANNUAL_CACHE_INSTANCES = []\n")
    if "take" in variant:
        old = "stored = np.ascontiguousarray(values[..., columns])"
        if writer.count(old) != 1:
            raise ValueError("original writer no longer matches frozen experiment")
        writer = writer.replace(old, "stored = np.take(values, columns, axis=-1)")
    if "slices" in variant:
        old = 'for (day,),part in sorted(table.partition_by("date",as_dict=True).items()):yield day,part'
        if sweep.count(old) != 1:
            raise ValueError("original sweep no longer matches frozen experiment")
        sweep = sweep.replace(old, "yield from chronological_date_groups(table)") + SLICE_HELPER
    return sweep, writer


def verify_fixture(fixture: Path) -> dict:
    data = json.loads((fixture / "fixture.json").read_bytes())
    if data["contract"] != CONTRACT:
        raise ValueError("benchmark fixture contract mismatch")
    for proof in data["proofs"]:
        path = Path(proof["path"])
        if not path.resolve().is_relative_to(fixture.resolve()) or file_sha256(path) != proof["sha256"]:
            raise ValueError("frozen input/original/oracle changed")
    return data


def worker(*, fixture: Path, output: Path, variant: str, block_bytes: int,
           cache_root: Path | None = None) -> dict:
    import numpy as np
    import polars as pl
    import pyarrow as pa

    pa.set_cpu_count(int(os.environ.get("POLARS_MAX_THREADS", "2")))
    data = verify_fixture(fixture)
    output = new_private(output)
    sweep, writer = candidate_sources((fixture / "original_sweep.py").read_text(),
                                      (fixture / "original_writer.py").read_text(), variant)
    atomic_write_bytes(output / "candidate_sweep.py", sweep.encode(), mode=0o400)
    atomic_write_bytes(output / "candidate_writer.py", writer.encode(), mode=0o400)
    module = load_baseline(output / "candidate_sweep.py")
    if variant.startswith("prepared"):
        module.PREPARED_ANNUAL_CACHE_ROOT = private_cache_path(cache_root or output / "annual-cache")
    codec = load_baseline(output / "candidate_writer.py")
    by_name = {d["feature"]: d for d in json.loads((fixture / "feature_dictionary.json").read_bytes())["features"]}
    individual, common = ([by_name[name] for name in data[key]] for key in ("individual", "common"))
    days = [date.fromisoformat(day) for day in data["dates"]]
    sweep_days = days + [date.fromisoformat(data["decision"])]
    lifecycle = np.load(fixture / "lifecycle.npy", allow_pickle=False)
    original = json.loads((fixture / "oracle/factorized_manifest.json").read_bytes())

    def execute():
        groups = pl.DataFrame({"feature": [d["feature"] for d in individual + common],
                               "report_group": [d.get("report_group") or "" for d in individual + common]})
        resets = pl.scan_parquet(data["event_paths"]).select("date", "symbol", "feature", "period").join(
            groups.lazy(), on="feature", how="inner").filter(pl.col("report_group") != "").select(
            "date", "symbol", "period", "report_group").unique().collect(engine="streaming")
        attempt = output / ".attempt"
        attempt.mkdir()
        blocks = []
        for begin, values, _ in module.causal_state_blocks(
                sweep_days, data["symbols"], individual, module.AnnualObservationEvents(data["event_paths"]),
                lifecycle, resets=resets, column_blocks=True, max_block_bytes=block_bytes,
                model_channel_policy="value_only"):
            keep = min(values.shape[0], len(days) - begin)
            if keep <= 0:
                continue
            parts = []
            for start, columns, part in values.symbol_blocks:
                proof = codec.write_array_block(attempt / f"individual_{begin:05d}_{start:05d}.npy.zst", part[:keep],
                    omit_zero_columns=True, logical_columns=columns, logical_shape=(keep, part.shape[1], values.shape[2]))
                proof["symbol_start"] = start
                parts.append(proof)
            blocks.append({"start": begin, "shape": [keep, len(data["symbols"]), len(individual)], "symbol_blocks": parts})
            print(f"[{variant}] committed {begin}:{begin+keep}", flush=True)
        shared_parts = [values[:, 0] for _, values, _ in module.causal_state_blocks(
            sweep_days, ["*"], common, module.AnnualObservationEvents(data["event_paths"]), None,
            model_channel_policy="value_only")]
        shared = np.concatenate(shared_parts)[:len(days)]
        np.save(attempt / "common.npy", shared, allow_pickle=False)
        private_copy(fixture / "feature_dictionary.json", attempt / "feature_dictionary.json")
        manifest = {**original, "blocks": blocks,
                    "common": {"path": "common.npy", "shape": list(shared.shape),
                               "sha256": file_sha256(attempt / "common.npy"), "bytes": (attempt / "common.npy").stat().st_size}}
        verify_factorized_members(attempt, manifest)
        atomic_write_json(attempt / "factorized_manifest.json", manifest)
        os.replace(attempt, output / "published")
        link = output / ".current"
        link.symlink_to(output / "published", target_is_directory=True)
        os.replace(link, output / "current")
        return {"manifest": str(output / "published/factorized_manifest.json"), "blocks": len(blocks),
                "physical_bytes": sum(p["bytes"] for b in blocks for p in b["symbol_blocks"])}

    metrics = measured(execute)
    actual = json.loads(Path(metrics["result"]["manifest"]).read_bytes())
    # All output shards, shapes, coordinates and header/compressed bits, not a
    # sampled checksum or model scalar. Partition experiments are not admitted
    # by this strict codec oracle; add independent all-cell comparison first.
    equal = actual["blocks"] == original["blocks"] and actual["common"] == original["common"]
    verify_fixture(fixture)
    evidence = {"contract": CONTRACT, "variant": variant, "block_bytes": block_bytes,
                "full_compressed_member_parity": equal, "all_dates": len(days),
                "all_symbols": len(data["symbols"]), "all_external_channels": len(individual) + len(common),
                "metrics": metrics, "node": observe_node(fixture, output),
                "provider_calls": 0, "cuda_requested": False, "production_publication": False,
                "upstream_normalization_included": False}
    if variant.startswith("prepared"):
        evidence["annual_cache"] = {"root": str(module.PREPARED_ANNUAL_CACHE_ROOT),
            "instances": [cache.stats() for cache in module.PREPARED_ANNUAL_CACHE_INSTANCES],
            "code_sha256": file_sha256(ROOT / "stockagent/data/annual_event_cache.py")}
    atomic_write_json(output / "receipt.json", evidence)
    if not equal:
        raise ValueError("complete published oracle differs; preserve rejected candidate")
    return evidence


def benchmark(*, fixture: Path, output: Path, variant: str, cache_root: Path | None = None) -> dict:
    if variant.startswith("prepared") and (cache_root is None or not cache_root.is_dir()):
        raise ValueError("warm benchmark requires an independently prepared private cache")
    output = new_private(output)
    data = verify_fixture(fixture)
    results = []
    env = {**os.environ, "CUDA_VISIBLE_DEVICES": "", "POLARS_MAX_THREADS": "2",
           "OMP_NUM_THREADS": "2", "OPENBLAS_NUM_THREADS": "2", "MKL_NUM_THREADS": "2"}
    for index, selected in enumerate(("baseline", variant, variant, "baseline")):
        command = [sys.executable, str(Path(__file__).resolve()), "worker", "--fixture", str(fixture),
                   "--output", str(output / f"{index}-{selected}"), "--variant", selected]
        if selected.startswith("prepared"):
            command.extend(["--cache-root", str(cache_root)])
        started = time.perf_counter()
        with (output / f"{index}-{selected}.log").open("x") as stream:
            result = subprocess.run(command, cwd=ROOT, env=env, stdout=stream, stderr=subprocess.STDOUT, timeout=1800)
        if result.returncode:
            raise RuntimeError(f"worker rejected {selected}: {result.returncode}; inspect retained log")
        receipt = json.loads((output / f"{index}-{selected}/receipt.json").read_bytes())
        if selected.startswith("prepared") and any(row["builds"] or row["rejections"] for row in receipt["annual_cache"]["instances"]):
            raise ValueError("cold/repaired cache cannot be averaged into warm timing")
        receipt["child_wall_seconds"] = time.perf_counter() - started
        results.append(receipt)
        atomic_write_json(output / "progress.json", {"finished": len(results), "results": results})
        print(f"finished {index+1}/4 {selected}: {receipt['metrics']['workflow_seconds']:.3f}s", flush=True)
    summary = {}
    for metric in ("workflow_seconds", "cpu_seconds", "peak_rss_bytes"):
        baseline = statistics.mean(row["metrics"][metric] for row in results if row["variant"] == "baseline")
        candidate = statistics.mean(row["metrics"][metric] for row in results if row["variant"] == variant)
        summary[metric] = {"baseline": baseline, "candidate": candidate, "change_fraction": candidate / baseline - 1}
    receipt = {"contract": CONTRACT, "status": "measured_exact_parity", "fixture": str(fixture),
               "frozen_bytes": data["frozen_bytes"], "variant": variant, "results": results, "summary": summary,
               "scope": "complete recorded causal projection including resets/sweep/writer/hash/readback/publication; excludes upstream normalization/base preparation/model"}
    atomic_write_json(output / "receipt.json", receipt)
    return {"status": receipt["status"], "summary": summary}


def main():
    os.nice(19)  # Finite engineering work yields to the existing trading owners.
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    capture = commands.add_parser("freeze")
    capture.add_argument("--fixture", type=Path, required=True)
    capture.add_argument("--parent", type=Path, required=True)
    capture.add_argument("--projection", type=Path, required=True)
    capture.add_argument("--public", type=Path, required=True)
    for name in ("worker", "benchmark"):
        command = commands.add_parser(name)
        command.add_argument("--fixture", type=Path, required=True)
        command.add_argument("--output", type=Path, required=True)
        command.add_argument("--variant", choices=("baseline", "take", "slices", "slices_take", "prepared", "prepared_causal"), required=True)
        command.add_argument("--cache-root", type=Path)
        if name == "worker":
            command.add_argument("--block-bytes", type=int, default=1024**3)
    args = vars(parser.parse_args())
    name = args.pop("command")
    if name == "freeze":
        args["parent_path"], args["projection_path"] = args.pop("parent"), args.pop("projection")
    print(json.dumps({"result": {"freeze": freeze, "worker": worker, "benchmark": benchmark}[name](**args)}, ensure_ascii=False))


if __name__ == "__main__":
    main()

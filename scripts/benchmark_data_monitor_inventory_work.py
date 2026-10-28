#!/usr/bin/env python3
"""Measure canonical inventory discovery and full record/feature cache work.

Discovery traverses the actual namespace, checking its complete ordered
membership on every trial. Cache trials replay retained actual stat/absence
and partition-identity observations, including complete record decode,
signature checks, aggregation, cache/index publication, feature reduction,
JSON and full byte readback. Source payload reads, footer refresh, public
provider labels, network and browser are outside the cache-trial scope.
Production caches, source data, services and scheduling are never modified.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager, nullcontext
import gc
import hashlib
import importlib.util
import json
from pathlib import Path
import statistics
import sys
import tempfile
import time
from types import SimpleNamespace
from unittest.mock import patch
from uuid import UUID

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.benchmark_data_monitor_projection_pipeline import capture
from stockagent import private_json
from stockagent.live import data_monitor_inventory as candidate
from stockagent.live.data_monitor_record_codec import encode_record_cache
from stockagent.live.data_monitor_record_store import cache_file, write_record_delta
from stockagent.live import data_monitor_record_store as candidate_store


def write_new(path, body):
    with path.open("xb") as stream:
        stream.write(body)
    path.chmod(0o600)


def io_counts():
    return {key: int(value) for key, value in
            (line.split(":") for line in Path("/proc/self/io").read_text().splitlines())}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-source", type=Path, required=True)
    parser.add_argument("--baseline-record-store", type=Path,
                        help="pin the baseline storage implementation as well as its inventory owner")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--replay", type=Path, help="reuse a retained result's exact seed and source observations")
    parser.add_argument("--case", choices=("all", "unchanged_index_repair", "source_identity_changed",
                                           "source_footer_refreshed"), default="all")
    parser.add_argument("--incremental-store", action="store_true", help="compare flat baseline with warm transactional store")
    parser.add_argument("--partitioned-metadata", action="store_true",
                        help="candidate uses metadata v2; a supplied baseline store is seeded with its own format")
    parser.add_argument("--factored-stats", action="store_true", help="candidate uses independently stored footer stats")
    parser.add_argument("--interned-schemas", action="store_true", help="candidate shares exact field pairs in private storage v4")
    parser.add_argument("--capture-features", action="store_true", help="candidate verifies each feature cohort immediately after record aggregation")
    parser.add_argument("--baseline-storage-version", type=int, choices=(1, 2, 3, 4), default=1,
                        help="explicit warm baseline format; compare the same selected format when measuring a new implementation")
    parser.add_argument("--baseline-capture-features", action="store_true",
                        help="baseline also verifies feature cohorts during record aggregation")
    parser.add_argument("--refresh-identities", type=int, default=1,
                        help="retained ordinary-file identities changed in footer-refresh case")
    parser.add_argument("--trials", type=int, choices=(4, 8), default=8,
                        help="balanced ABBA or ABBABAAB comparisons")
    args = parser.parse_args()
    if (args.partitioned_metadata or args.factored_stats or args.interned_schemas) and not args.incremental_store:
        parser.error("partitioned metadata and factored stats require --incremental-store")
    if args.output.exists():
        raise ValueError("preserve previous results")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    pins = [Path(__file__), args.baseline_source, Path(candidate.__file__),
            ROOT / "stockagent/live/data_monitor_record_codec.py",
            ROOT / "stockagent/live/data_monitor_record_store.py",
            ROOT / "stockagent/private_json.py",
            ROOT / "scripts/benchmark_data_monitor_projection_pipeline.py"]
    if args.baseline_record_store is not None:
        pins.append(args.baseline_record_store)
    def code_identity():
        return {str(path.resolve()): hashlib.sha256(path.read_bytes()).hexdigest() for path in pins}
    before = code_identity()
    name = "_inventory_work_baseline"
    spec = importlib.util.spec_from_file_location(name, args.baseline_source)
    baseline = importlib.util.module_from_spec(spec)
    sys.modules[name] = baseline
    spec.loader.exec_module(baseline)
    baseline_store = None
    if args.baseline_record_store is not None:
        store_spec = importlib.util.spec_from_file_location("_inventory_store_baseline", args.baseline_record_store)
        baseline_store = importlib.util.module_from_spec(store_spec)
        store_spec.loader.exec_module(baseline_store)
        for helper in ("cache_file", "read_record_cache", "record_store_needs_migration", "write_record_delta"):
            setattr(baseline, helper, getattr(baseline_store, helper))
        # The frozen reader produces its own class identities. Its inventory
        # must recognize those compact schemas, rather than a current import.
        if args.baseline_storage_version >= 4:
            baseline.SharedSchema = baseline_store.SharedSchema
    modules = {"A": baseline, "B": candidate}
    trial_order = "ABBA" if args.trials == 4 else "ABBABAAB"
    discovery = []
    membership = None
    for label in trial_order if args.replay is None else "":
        cpu, started = time.process_time(), time.perf_counter()
        selected = modules[label]._selected_files(ROOT)
        elapsed, cpu_elapsed = time.perf_counter() - started, time.process_time() - cpu
        digest = candidate._membership_fingerprint(selected)
        membership = membership or digest
        discovery.append({"variant": label, "wall_s": elapsed, "cpu_s": cpu_elapsed,
                          "membership": digest, "paths": sum(map(len, selected.values()))})
    if discovery:
        write_new(args.output.with_name(args.output.stem + "_discovery.json"), json.dumps(discovery).encode())
    if any(row["membership"] != membership for row in discovery):
        write_new(args.output, json.dumps({"state": "live_membership_changed", "discovery": discovery}).encode())
        raise ValueError("membership changed; live discovery comparison is inconclusive")

    capture_rejections = []
    prior = json.loads(args.replay.read_bytes()) if args.replay else None
    for attempt in range(3) if prior is None else ():
        try:
            generation = capture(ROOT)
            break
        except ValueError as error:
            capture_rejections.append(str(error))
            print(json.dumps({"capture_rejected": str(error), "attempt": attempt + 1}), flush=True)
            if attempt == 2 or str(error) not in {
                    "cache advanced while capturing", "membership advanced while capturing"}:
                raise
    identities = {}
    with candidate._partition_snapshot_scope() if prior is None else nullcontext():
        if prior is not None:
            seed_path = Path(prior["input_seed"])
            seed = seed_path.read_bytes()
            assert hashlib.sha256(seed).hexdigest() == prior["input_seed_sha256"]
            observations_path = Path(prior["retained_observations"])
            observation = json.loads(observations_path.read_bytes())
            identities = observation["identities"]
            generation = {"payload": json.loads(seed),
                "source_stats": {key: SimpleNamespace(**value) if value is not None else None
                                 for key, value in observation["stats"].items()},
                "selected": {key: list(map(Path, paths)) for key, paths in observation["selected"].items()}}
        for paths in generation["selected"].values():
            for path in paths:
                key = str(path)
                if key not in identities:
                    stat = generation["source_stats"][key]
                    identities[key] = candidate._source_file_identity(path, stat) if stat is not None else None
    attrs = ["st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns", "st_mode", "st_uid", "st_gid", "st_nlink"]
    observation = {"selected": {key: list(map(str, paths)) for key, paths in generation["selected"].items()},
        "stats": {key: {attr: getattr(stat, attr) for attr in attrs} if stat is not None else None
                  for key, stat in generation["source_stats"].items()}, "identities": identities,
        "captured_cache_sha256": generation.get("sha256")}
    if prior is None:
        observations_path = args.output.with_name(args.output.stem + "_observations.json")
        write_new(observations_path, json.dumps(observation, ensure_ascii=True).encode())
    original_stat = Path.stat
    footer_paths = [key for key, identity in identities.items()
                    if identity is not None and len(identity) == 5
                    and isinstance(generation["payload"]["files"].get(key, {}).get("stats"), dict)
                    and generation["payload"]["files"][key].get("file_identity") == identity]
    footer_paths.sort(key=lambda key: len(generation["payload"]["files"][key]["stats"]["non_null"]), reverse=True)
    if not 1 <= args.refresh_identities <= len(footer_paths):
        raise ValueError("refresh identities must fit the retained valid ordinary-file namespace")
    footer_paths = footer_paths[:args.refresh_identities]
    footer_keys = frozenset(footer_paths)

    def retained_footer(path):
        if str(path) not in footer_keys:
            raise AssertionError("unexpected footer outside retained comparison")
        # The owner pops only fields and constructs its own schema pairs. Its
        # count vector and the retained schema are read-only inputs. Deepcopy
        # here adds an artificial Python loop over every retained cell; that
        # is neither source I/O nor work performed by the canonical footer.
        stats = dict(generation["payload"]["files"][str(path)]["stats"])
        stats["fields"] = generation["payload"]["schemas"][stats["schema_id"]]
        return stats

    @contextmanager
    def frozen(module, stats, ids):
        counts = {"stat_calls": 0, "identity_calls": 0, "checked": set()}
        def stat(path, *argv, **kwargs):
            key = str(path)
            if key not in stats:
                return original_stat(path, *argv, **kwargs)
            counts["stat_calls"] += 1
            counts["checked"].add(key)
            if stats[key] is None:
                raise FileNotFoundError(key)
            return stats[key]
        def identity(path, metadata):
            counts["identity_calls"] += 1
            return ids[str(path)]
        with patch.object(Path, "stat", stat), \
             patch.object(module, "_source_file_identity", identity), \
             patch.object(module, "_selected_files", return_value=generation["selected"]), \
             patch.object(module, "uuid4", return_value=UUID(int=1)):
            yield counts

    # Normalize one complete generation under the retained observations.
    # This setup is outside timing; trials all start with these exact bytes.
    with tempfile.TemporaryDirectory(prefix="record-seed-", dir=args.output.parent) as directory:
        root = Path(directory)
        cache = root / "artifacts/live/data_monitor/record_inventory_cache.json"
        cache.parent.mkdir(parents=True)
        if prior is None:
            cache.write_bytes(encode_record_cache(generation["payload"]))
            with frozen(baseline, generation["source_stats"], identities):
                baseline.build_record_inventory(root, refresh=True, max_refresh_files=0)
            seed = cache.read_bytes()
    if prior is None:
        seed_path = args.output.with_name(args.output.stem + "_seed.json")
        write_new(seed_path, seed)
    input_pins = {str(path): hashlib.sha256(path.read_bytes()).hexdigest()
                  for path in (seed_path, observations_path)}
    changed_stats = dict(generation["source_stats"])
    changed_ids = dict(identities)
    changed_path = prior["changed_source_path"] if prior else next(key for key, value in changed_ids.items()
                        if value is not None and len(value) == 5)
    altered = dict(observation["stats"][changed_path])
    altered["st_ctime_ns"] += 1
    changed_stats[changed_path] = SimpleNamespace(**altered)
    changed_ids[changed_path] = [*changed_ids[changed_path][:4], altered["st_ctime_ns"]]
    footer_stats, footer_ids = dict(generation["source_stats"]), dict(identities)
    for key in footer_paths:
        altered = dict(observation["stats"][key])
        altered["st_ctime_ns"] += 1
        footer_stats[key] = SimpleNamespace(**altered)
        footer_ids[key] = [*identities[key][:4], altered["st_ctime_ns"]]
    trials = []
    case_references = {}
    progress_path = args.output.with_name(args.output.stem + "_trials.jsonl")
    write_new(progress_path, b"")
    for case, order, stats, ids, refresh_count in [
            ("unchanged_index_repair", trial_order, generation["source_stats"], identities, 0),
            ("source_identity_changed", trial_order if prior else "ABBA", changed_stats, changed_ids, 0),
            ("source_footer_refreshed", trial_order, footer_stats, footer_ids, args.refresh_identities)]:
        if case == "source_footer_refreshed" and args.case != case:
            continue
        if args.case != "all" and args.case != case:
            continue
        for label in order:
            module = modules[label]
            with tempfile.TemporaryDirectory(prefix="record-work-", dir=args.output.parent) as directory:
                root = Path(directory)
                cache = root / "artifacts/live/data_monitor/record_inventory_cache.json"
                cache.parent.mkdir(parents=True)
                cache.write_bytes(seed)
                setup = None
                if args.incremental_store and (label == "B" or baseline_store is not None):
                    seed_payload = json.loads(seed)
                    setup_started, setup_cpu = time.perf_counter(), time.process_time()
                    store = baseline_store if label == "A" else candidate_store
                    options = {}
                    if label == "A":
                        if args.baseline_storage_version >= 2:
                            options["partitioned_metadata"] = True
                        if args.baseline_storage_version >= 3:
                            options["factored_stats"] = True
                        if args.baseline_storage_version >= 4:
                            options["interned_schemas"] = True
                    else:
                        if args.partitioned_metadata:
                            options["partitioned_metadata"] = True
                        if args.factored_stats:
                            options["factored_stats"] = True
                        if args.interned_schemas:
                            options["interned_schemas"] = True
                    setup = store.write_record_delta(cache, seed_payload, seed_payload,
                        expected_signature=candidate._cache_signature(cache), enable=True, **options)
                    setup = {**setup, "wall_s": time.perf_counter() - setup_started,
                             "cpu_s": time.process_time() - setup_cpu,
                             "physical_bytes": cache_file(cache).stat().st_size}
                    del seed_payload
                prior = candidate._cache_signature(cache)
                frame = module.InventorySnapshot(root)
                gc.collect()
                with frozen(module, stats, ids) as counts, patch.object(module, "parquet_footer_stats", retained_footer):
                    io_before = io_counts()
                    cpu, started = time.process_time(), time.perf_counter()
                    result = module.build_record_inventory(root, refresh=True, max_refresh_files=refresh_count, snapshot=frame,
                        **({"capture_features": True} if (label == "B" and args.capture_features
                            or label == "A" and args.baseline_capture_features) else {}))
                    features = private_json.dumps(module.build_feature_inventory(root, snapshot=frame))
                    if args.incremental_store:
                        body = json.dumps(module._read_json(cache), sort_keys=True, separators=(",", ":")).encode()
                    else:
                        body = cache.read_bytes()
                    index_path = module._quick_index_path(cache)
                    if index_path.exists():
                        index = json.loads(index_path.read_bytes())
                        assert index["cache_signature"] == module._cache_signature(cache)
                        assert index["checksum"] == module._quick_index_checksum(index)
                        index_body = json.dumps({key: value for key, value in index.items()
                            if key not in {"cache_signature", "checksum"}}, sort_keys=True, separators=(",", ":")).encode()
                    else:
                        assert result["source_observation_matches_cache"] is not True
                        index_body = b"null"  # Unverified source cannot publish a proof.
                    digests = [hashlib.sha256(value).hexdigest() for value in (body, features, index_body)]
                    elapsed, cpu_elapsed = time.perf_counter() - started, time.process_time() - cpu
                    io_after = io_counts()
                reused = candidate._cache_signature(cache) == prior
                expected_reuse = result.get("cache_payload_reused", False)
                assert reused is expected_reuse, (case, label, reused)
                assert counts["checked"] == generation["source_stats"].keys()
                if case not in case_references:
                    case_references[case] = digests
                    write_new(args.output.with_name(args.output.stem + "_" + case + "_expected.json"), body)
                assert digests == case_references[case], (case, label, "complete output mismatch")
                trials.append({"case": case, "variant": label, "wall_s": elapsed, "cpu_s": cpu_elapsed,
                    "logical_write_bytes": io_after["wchar"] - io_before["wchar"],
                    "kernel_write_bytes": io_after["write_bytes"] - io_before["write_bytes"],
                    "cache_inode_reused": reused, "source_stat_calls": counts["stat_calls"],
                    "source_identity_calls": counts["identity_calls"], "source_paths_checked": len(counts["checked"]),
                    "cache_sha256": digests[0], "feature_sha256": digests[1], "index_semantic_sha256": digests[2],
                    "features": len(json.loads(features)["rows"]), "output_cache_bytes": len(body),
                    "physical_cache_bytes": cache_file(cache).stat().st_size,
                    "storage_setup_outside_timing": setup,
                    "cache_publication": result.get("cache_encoding_observation"),
                    "refreshed_files": result["refreshed_files"],
                    "timing_ms": result["timing_ms"]})
                with progress_path.open("ab") as stream:
                    stream.write(json.dumps(trials[-1]).encode() + b"\n")
                print(json.dumps({key: trials[-1][key] for key in
                      ["case", "variant", "wall_s", "cpu_s", "logical_write_bytes", "cache_inode_reused"]}), flush=True)
                del frame, result, body, features
    after = code_identity()
    assert before == after, "source changed during comparison"
    assert input_pins == {path: hashlib.sha256(Path(path).read_bytes()).hexdigest()
                          for path in input_pins}, "retained input changed during comparison"
    summaries = {}
    for case in {row["case"] for row in trials}:
        summaries[case] = {label: {key: statistics.mean(row[key] for row in trials
            if row["case"] == case and row["variant"] == label)
            for key in ["wall_s", "cpu_s", "logical_write_bytes"]} for label in ["A", "B"]}
    if discovery:
        summaries["discovery"] = {label: {key: statistics.mean(row[key] for row in discovery if row["variant"] == label)
            for key in ["wall_s", "cpu_s"]} for label in ["A", "B"]}
    result = {"schema_version": 1, "state": "verified", "scope": __doc__, "production_writes": 0,
        "input_seed": str(seed_path), "input_seed_sha256": hashlib.sha256(seed).hexdigest(), "input_bytes": len(seed),
        "retained_observations": str(observations_path), "changed_source_path": changed_path,
        "capture_rejections": capture_rejections,
        "replayed_result": str(args.replay) if args.replay else None,
        "changed_source_axis": "retained ctime_ns plus one; original source never mutated",
        "footer_refresh_fixture": {"paths": footer_paths if args.case == "source_footer_refreshed" else [],
            "selection": "widest retained valid ordinary-file schemas first",
            "values": "retained original complete footer counts, bounds, schema and null vectors; no invented values",
            "source_footer_io": "replayed retained original footer observations; source disk I/O excluded"},
        "incremental_store": args.incremental_store,
        "partitioned_metadata": args.partitioned_metadata,
        "factored_stats": args.factored_stats,
        "baseline_storage_version": args.baseline_storage_version,
        "baseline_capture_features": args.baseline_capture_features,
        "trial_order": trial_order,
        "baseline_record_store": str(args.baseline_record_store) if args.baseline_record_store is not None else None,
        "interned_schemas": args.interned_schemas, "capture_features": args.capture_features,
        "cache_comparison": "complete canonical sorted logical JSON" if args.incremental_store else "physical JSON bytes",
        "source_code_before": before, "source_code_after": after, "code_stable": True,
        "retained_input_pins": input_pins,
        "index_comparison_excludes": ["per-trial cache inode signature", "checksum binding that signature"],
        "all_outputs_equal_within_each_case": True, "discovery": discovery, "trials": trials, "means": summaries}
    write_new(args.output, json.dumps(result, ensure_ascii=False).encode())
    print(json.dumps({"state": result["state"], "input_bytes": len(seed), "means": summaries}))


if __name__ == "__main__":
    main()

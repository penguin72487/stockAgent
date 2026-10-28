#!/usr/bin/env python3
"""Compare complete bar-cache transactions on one retained source observation.

SQL, JSON, discovery, stat and date-projection results are captured once through
the original owner, then replayed in exact call order. Cold-work clocks are also
replayed so both variants do exactly the same audits and retain pending/invalid
states. Timing includes cache decode, all caller checks/path work, aggregation,
canonical atomic cache publication, complete public JSON and byte readback.
Physical source I/O and query execution, imports, HTTP and browser are excluded.
Production sources, caches, services and resource limits are never changed.
"""
from __future__ import annotations

import argparse
from contextlib import ExitStack, contextmanager
from datetime import UTC, datetime
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import sqlite3
import statistics
import sys
import tempfile
import time
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from scripts import snapshot_data_refresh_services as owner
from stockagent import private_json
from stockagent.live import bar_inventory as candidate
from stockagent.live import data_monitor_inventory as inventory
from stockagent.live import tick_inventory as candidate_tick


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def digest(body):
    return hashlib.sha256(body).hexdigest()


def public_bytes(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def write_new(path, body):
    with path.open("xb") as stream:
        stream.write(body)
    path.chmod(0o600)


class Observation:
    """Retain outer source checks; opaque source operations are separate."""
    def __init__(self, root, events=None, *, sql_image=None):
        self.root = str(root)
        self.recording = events is None
        self.events = {} if events is None else events
        self.offsets = {}
        self.opaque = 0
        self.stat = os.stat
        self.glob = Path.glob
        self.monotonic = time.monotonic
        self.sql_image = sql_image

    def source(self, name):
        return name.startswith((self.root + "/data_", self.root + "/configs/"))

    def invoke(self, kind, key, loader):
        index = self.offsets.get(kind, 0)
        self.offsets[kind] = index + 1
        if self.recording:
            self.opaque += 1
            try:
                result = {"value": loader()}
            except (OSError, ValueError, RuntimeError, KeyError, sqlite3.Error) as error:
                result = {"error": type(error).__name__, "args": error.args,
                          "filename": getattr(error, "filename", None), "message": str(error)}
                if isinstance(error, json.JSONDecodeError):
                    result["args"] = [error.msg, error.doc, error.pos]
                self.events.setdefault(kind, []).append([key, result])
                raise
            finally:
                self.opaque -= 1
            self.events.setdefault(kind, []).append([key, result])
        else:
            expected, result = self.events[kind][index]
            if key != expected:
                raise AssertionError((kind, index, key, expected))
        if "error" in result:
            classes = {cls.__name__: cls for cls in (OSError, FileNotFoundError, PermissionError,
                       ValueError, RuntimeError, KeyError, sqlite3.Error, sqlite3.OperationalError)}
            classes["JSONDecodeError"] = json.JSONDecodeError
            if result["error"] not in classes:
                raise AssertionError("unsupported captured exception: " + result["error"])
            cls = classes[result["error"]]
            arguments = result["args"]
            error = cls(*arguments, result["filename"]) if result["filename"] else cls(*arguments)
            if str(error) != result["message"]:
                raise AssertionError("source exception cannot be replayed exactly")
            raise error
        return result["value"]

    def source_stat(self, path, *args, **kwargs):
        name = os.fsdecode(path) if isinstance(path, (str, bytes, os.PathLike)) else ""
        if self.opaque or not self.source(name):
            return self.stat(path, *args, **kwargs)
        def read():
            try:
                value = self.stat(path, *args, **kwargs)
            except FileNotFoundError:
                return None
            return {key: getattr(value, key) for key in
                    ("st_dev", "st_ino", "st_mode", "st_size", "st_mtime_ns", "st_ctime_ns")}
        value = self.invoke("stat", name, read)
        if value is None:
            raise FileNotFoundError(name)
        return SimpleNamespace(**value)

    def source_glob(self, path, pattern, *args, **kwargs):
        if self.opaque or not self.source(str(path)):
            return self.glob(path, pattern, *args, **kwargs)
        values = self.invoke("glob", [str(path), pattern],
                             lambda: list(map(str, self.glob(path, pattern, *args, **kwargs))))
        return map(Path, values)

    @contextmanager
    def applied(self, module, tick):
        database, read_json, project, discover = module._database, module._json, module._project_dates, module._pattern_sources
        def query(path, sql, parameters=(), **kwargs):
            # The new physical-plan hint changes no opaque operation or row
            # contract. Older retained observations predate this keyword.
            original_kwargs = {k: v for k, v in kwargs.items() if k != "monitor_counts"}
            value = self.invoke("sql", [str(path), sql, list(parameters), original_kwargs],
                                lambda: database(path, sql, parameters, **kwargs))
            if (self.sql_image is not None and str(path).endswith("data_finmind/complement/queue.sqlite3")
                    and "GROUP BY dataset" in sql):
                return database(self.sql_image, sql, parameters, **kwargs)
            return value
        def project_dates(path, **kwargs):
            value = self.invoke("project", str(path), lambda: project(path, **kwargs))
            if value.get("date_chunk_keys") and kwargs.get("chunks") is not None:
                chunks = kwargs["chunks"]
                retained = self.invoke("project_chunks", str(path), lambda: {
                    key: chunks[key] for key in value["date_chunk_keys"]})
                chunks.update(retained)
            return value
        with ExitStack() as stack:
            stack.enter_context(patch.object(os, "stat", self.source_stat))
            stack.enter_context(patch.object(Path, "glob", lambda path, pattern, *a, **k: self.source_glob(path, pattern, *a, **k)))
            stack.enter_context(patch.object(module, "time", SimpleNamespace(
                monotonic=lambda: self.invoke("clock", "", self.monotonic))))
            stack.enter_context(patch.object(module, "_database", query))
            stack.enter_context(patch.object(module, "_json", lambda path: self.invoke("json", str(path), lambda: read_json(path))))
            stack.enter_context(patch.object(module, "_project_dates", project_dates))
            stack.enter_context(patch.object(module, "_pattern_sources", lambda root, pattern: list(map(Path,
                self.invoke("discovery", [str(root), pattern], lambda: list(map(str, discover(root, pattern))))))))
            yield
        if not self.recording:
            assert self.offsets == {kind: len(values) for kind, values in self.events.items()}, "source work omitted or repeated"


def transaction(root, output, module, *, seed, records, frame, now, observation, tick):
    cache = output.with_name("bar_inventory_cache.json")
    cache.parent.mkdir(parents=True, exist_ok=True)
    cache.write_bytes(seed)
    cpu, started = time.process_time(), time.perf_counter()
    with observation.applied(module, tick):
        value = owner._catalog_snapshot(output, name="bar", builder=module.build_bar_inventory,
            observed=now, audit_seconds=2., record_inventory=records, inventory_snapshot=frame)
        body = public_bytes(value)
        persisted = cache.read_bytes()
        # Decode the entire published cache, not only its length or summary.
        current = private_json.loads(persisted)
        assert current["version"] == 1 and isinstance(current["dates"], dict)
        result = {"public_sha256": digest(body), "cache_sha256": digest(persisted),
                  "public_bytes": len(body), "cache_bytes": len(persisted),
                  "groups": len(value["groups"]), "collection": value["collection"]}
    result.update(wall_s=time.perf_counter() - started, cpu_s=time.process_time() - cpu,
                  source_checks=observation.offsets)
    return result, body, persisted


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-bar", required=True, type=Path)
    parser.add_argument("--baseline-tick", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--replay", type=Path, help="reuse a complete retained input capsule without any source queries")
    parser.add_argument("--sql-image", type=Path,
                        help="with --replay, execute the complete complement aggregate on a frozen private queue image")
    args = parser.parse_args()
    if args.output.exists():
        raise ValueError("preserve prior acceptance evidence")
    if args.sql_image is not None and args.replay is None:
        raise ValueError("--sql-image requires a retained complete input capsule")
    pins = [Path(__file__), args.baseline_bar, args.baseline_tick, Path(candidate.__file__),
            Path(candidate_tick.__file__), ROOT / "stockagent/data/partitioned_parquet.py",
            Path(owner.__file__), Path(inventory.__file__), ROOT / "stockagent/live/data_monitor_record_store.py"]
    if args.sql_image is not None:
        pins.append(ROOT / "downloader/finmind_queue_indexes.py")
    def code():
        return {str(path): digest(path.read_bytes()) for path in pins}
    before = code()
    baseline = load_module("_bar_work_A", args.baseline_bar)
    baseline_tick = load_module("_bar_work_tick_A", args.baseline_tick)
    baseline._Sources = baseline_tick._Sources
    baseline._signature = baseline_tick._signature
    baseline._database = baseline_tick._database
    baseline._db_paths = baseline_tick._db_paths
    stem = args.output.with_suffix("")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    frame = inventory.InventorySnapshot(ROOT)
    if args.replay is not None:
        previous = private_json.loads(args.replay.read_bytes())
        for name, expected in previous["input_pins"].items():
            assert digest(Path(name).read_bytes()) == expected, name
        seed_path, observation_path = Path(previous["seed"]), Path(previous["observations"])
        record_path, selected_path = Path(previous["record_datasets"]), Path(previous["selected_files"])
        seed = seed_path.read_bytes()
        records = private_json.loads(record_path.read_bytes())
        frame.selected = {key: list(map(Path, values)) for key, values in private_json.loads(selected_path.read_bytes()).items()}
        events = private_json.loads(observation_path.read_bytes())
        now, reference = datetime.fromisoformat(previous["observed_at_utc"]), previous["reference"]
    else:
        records = inventory.build_record_inventory(ROOT, refresh=False, snapshot=frame)
        frame.selected = inventory._selected_files(ROOT)
        assert inventory._bound_dataset_memberships(frame.payload) == inventory._dataset_memberships(frame.selected)
        path = ROOT / "artifacts/live/data_monitor/bar_inventory_cache.json"
        info = inventory._cache_signature(path)
        seed = path.read_bytes()
        assert info is not None and inventory._cache_signature(path) == info, "bar cache advanced during capture"
        now = datetime.now(UTC)
        seed_path = Path(str(stem) + "_seed.json")
        record_path, selected_path = Path(str(stem) + "_record_datasets.json"), Path(str(stem) + "_selected_files.json")
        write_new(seed_path, seed)
        write_new(record_path, public_bytes(records))
        write_new(selected_path, public_bytes({key: list(map(str, values)) for key, values in frame.selected.items()}))
        observed = Observation(ROOT)
        with tempfile.TemporaryDirectory(prefix="bar-capture-", dir=args.output.parent) as directory:
            reference, public, persisted = transaction(ROOT, Path(directory) / "public_status.json", baseline,
                seed=seed, records=records, frame=frame, now=now, observation=observed, tick=baseline_tick)
        observation_path = Path(str(stem) + "_observations.json")
        observation_bytes = public_bytes(observed.events)
        write_new(observation_path, observation_bytes)
        write_new(Path(str(stem) + "_expected_public.json"), public)
        write_new(Path(str(stem) + "_expected_cache.json"), persisted)
        events = private_json.loads(observation_bytes)
        del observation_bytes, public, persisted
    input_pins = {str(path): digest(path.read_bytes()) for path in (seed_path, observation_path, record_path, selected_path)}
    original_reference = reference
    if args.sql_image is not None:
        input_pins[str(args.sql_image)] = digest(args.sql_image.read_bytes())
        # The SQL snapshot has a later fixed identity than the opaque receipt
        # capsule. Establish a new full baseline oracle for this explicitly
        # composite fixture; never compare against stale aggregate counts.
        with tempfile.TemporaryDirectory(prefix="bar-sql-reference-", dir=args.output.parent) as directory:
            reference, public, persisted = transaction(ROOT, Path(directory) / "public_status.json", baseline,
                seed=seed, records=records, frame=frame, now=now,
                observation=Observation(ROOT, events, sql_image=args.sql_image), tick=baseline_tick)
        write_new(Path(str(stem) + "_expected_public.json"), public)
        write_new(Path(str(stem) + "_expected_cache.json"), persisted)
    print(json.dumps({"capture": reference}), flush=True)
    trials = []
    checkpoint = Path(str(stem) + "_trials.jsonl")
    write_new(checkpoint, b"")
    for label in "ABBABAAB":
        module, tick = (baseline, baseline_tick) if label == "A" else (candidate, candidate_tick)
        with tempfile.TemporaryDirectory(prefix="bar-work-", dir=args.output.parent) as directory:
            result, _, _ = transaction(ROOT, Path(directory) / "public_status.json", module,
                seed=seed, records=records, frame=frame, now=now,
                observation=Observation(ROOT, events, sql_image=args.sql_image), tick=tick)
        for key in ("public_sha256", "cache_sha256", "source_checks", "groups", "collection"):
            assert result[key] == reference[key], (label, key, result[key], reference[key])
        trials.append({"variant": label, **result})
        with checkpoint.open("ab") as stream:
            stream.write(public_bytes(trials[-1]) + b"\n")
        print(json.dumps({key: trials[-1][key] for key in ("variant", "wall_s", "cpu_s", "groups")}), flush=True)
    assert before == code(), "source changed during acceptance"
    assert input_pins == {name: digest(Path(name).read_bytes()) for name in input_pins}
    result = {"schema_version": 1, "state": "verified", "scope": __doc__, "production_writes": 0,
              "sql_image": str(args.sql_image) if args.sql_image is not None else None,
              "sql_scope": ("Composite fixed fixture: actual complement count/date SQL, type guard and complete "
                            "transaction run inside every trial; all remaining opaque source operations use "
                            "the retained capsule. Baseline establishes new full public/cache oracles. "
                            "Includes full cache decode, checks, aggregation, publication and readback; "
                            "excludes source queue capture/index setup, other physical source I/O and HTTP."
                            if args.sql_image is not None else None),
              "original_reference": original_reference,
              "seed": str(seed_path), "seed_sha256": digest(seed), "observations": str(observation_path),
              "observations_sha256": digest(observation_path.read_bytes()), "observed_at_utc": now.isoformat(),
              "record_datasets": str(record_path), "selected_files": str(selected_path), "input_pins": input_pins,
              "replayed_result": str(args.replay) if args.replay is not None else None,
              "cold_budget_seconds_each": 2., "reference": reference, "trials": trials,
              "all_outputs_and_source_work_equal": True, "source_code_before": before, "source_code_after": code(),
              "means": {label: {key: statistics.mean(row[key] for row in trials if row["variant"] == label)
                        for key in ("wall_s", "cpu_s")} for label in "AB"}}
    write_new(args.output, public_bytes(result))
    print(json.dumps({"state": result["state"], "means": result["means"]}))


if __name__ == "__main__":
    main()

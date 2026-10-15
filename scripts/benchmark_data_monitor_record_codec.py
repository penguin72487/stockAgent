#!/usr/bin/env python3
"""Matched full-namespace record-cache replay, including validation and writes.

Real source stat/absence observations are frozen. No provider request, source
write, Parquet scan, service restart or promotion occurs. Complete record
decode, source identity scan, aggregation, cache persistence and quick-index
publication are included. Import/discovery/cold source I/O and UI are excluded.
"""

from __future__ import annotations

import argparse
import ast
from contextlib import contextmanager, nullcontext
import gc
import hashlib
import json
from pathlib import Path
import resource
import statistics
import sys
import tempfile
import time
from types import ModuleType
from unittest.mock import patch
from uuid import UUID

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from downloader.artifact_io import atomic_write_json  # noqa: E402
from scripts.benchmark_data_monitor_projection_pipeline import capture  # noqa: E402
from stockagent import private_json  # noqa: E402
from stockagent.live import data_monitor_inventory as inventory  # noqa: E402
from scripts import data_monitor_deferred_counts_prototype as deferred  # noqa: E402
from stockagent.live.data_monitor_record_codec import encode_record_cache  # noqa: E402
from stockagent.runtime_identity import identity_sha256  # noqa: E402


STRUCT_NAMES = frozenset(("_FooterStats", "_FileEntry", "_Dataset", "_RecordCache"))


def struct_gc_source(source: str, *, tracked: bool) -> str:
    """Change only temporary validation Struct GC flags in the complete codec.

    The original payload, strict type proof, native encoder and legacy fallback
    remain identical. No interpreter-wide GC operation is a candidate.
    """
    tree = ast.parse(source)
    classes = [node for node in ast.walk(tree) if isinstance(node, ast.ClassDef)]
    if ({node.name for node in classes} != STRUCT_NAMES or len(classes) != len(STRUCT_NAMES)
            or any(len(node.bases) != 1 or ast.unparse(node.bases[0]) != "_native.Struct"
                   or any(keyword.arg not in {"forbid_unknown_fields", "gc"} for keyword in node.keywords)
                   for node in classes)):
        raise ValueError("complete strict metadata codec Struct shape changed")
    for node in classes:
        node.keywords = [keyword for keyword in node.keywords if keyword.arg != "gc"]
        node.keywords.append(ast.keyword(arg="gc", value=ast.Constant(value=tracked)))
    return ast.unparse(ast.fix_missing_locations(tree)) + "\n"


@contextmanager
def struct_gc_encoders():
    """Private prototype modules; restore even a pre-existing module on exit."""
    source = (REPO_ROOT / "stockagent/live/data_monitor_record_codec.py").read_text()
    encoders, prior = {}, {}
    try:
        for variant, tracked in (("tracked_native", True), ("untracked_native", False)):
            name = f"_stockagent_record_codec_{variant}"
            prior[name] = sys.modules.get(name)
            module = ModuleType(name)
            module.__file__ = f"<{name}>"
            sys.modules[name] = module  # Resolve deferred nested type annotations.
            exec(compile(struct_gc_source(source, tracked=tracked), module.__file__, "exec"), module.__dict__)
            if any(getattr(module, name).__struct_config__.gc != tracked for name in STRUCT_NAMES):
                raise ValueError("actual prototype Struct GC configuration differs")
            encoders[variant] = module.encode_record_cache
        yield encoders
    finally:
        for name, previous in prior.items():
            if previous is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = previous


def benchmark(root: Path, *, repeats: int = 2, mode: str = "decode") -> dict:
    if mode not in {"decode", "encode", "struct_gc", "deferred", "mapping"}:
        raise ValueError("unknown record codec benchmark mode")
    gc_before = gc.isenabled()
    code_paths = (Path(__file__).resolve(), Path(inventory.__file__),
                  REPO_ROOT / "stockagent/live/data_monitor_record_codec.py",
                  Path(deferred.__file__),
                  REPO_ROOT / "stockagent/private_json.py",
                  REPO_ROOT / "scripts/benchmark_data_monitor_projection_pipeline.py")
    def code_identity():
        return {str(path.relative_to(REPO_ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
                for path in code_paths}
    code_before = code_identity()
    generation = capture(root)
    original_stat = Path.stat
    body = json.dumps(generation["payload"], separators=(",", ":")).encode()
    native_loads = private_json.loads
    trials = []
    outputs = []
    output_bytes_hashes = []
    all_sources_checked = []
    names = (("tracked_native", "untracked_native") if mode == "struct_gc" else
             ("eager_native", "deferred_native") if mode == "deferred" else
             ("abc_mapping", "dict_first_mapping") if mode == "mapping" else ("stdlib", "native"))
    include_features = mode in {"deferred", "mapping"}
    mapping_type = inventory.Mapping
    mapping_owner = "_RUNTIME_MAPPING_TYPES" if hasattr(inventory, "_RUNTIME_MAPPING_TYPES") else "Mapping"
    default_mapping_types = getattr(inventory, mapping_owner)
    feature_outputs = []
    original_read_json = inventory._read_json
    prototypes = struct_gc_encoders() if mode == "struct_gc" else nullcontext(None)
    with tempfile.TemporaryDirectory(prefix="stockagent-record-codec-") as directory, prototypes as encoders:
        for repeat in range(repeats):
            order = ((names[1], names[0], names[0], names[1]) if mode == "mapping" and repeat % 2
                     else (names[0], names[1], names[1], names[0]))
            for index, variant in enumerate(order):
                trial_root = Path(directory) / f"{repeat}-{index}"
                cache = trial_root / "artifacts/live/data_monitor/record_inventory_cache.json"
                cache.parent.mkdir(parents=True)
                cache.write_bytes(body)
                stat_calls = 0
                checked_paths = set()

                def frozen_stat(path, *args, **kwargs):
                    nonlocal stat_calls
                    key = str(path)
                    if key in generation["source_stats"]:
                        stat_calls += 1
                        checked_paths.add(key)
                        value = generation["source_stats"][key]
                        if value is None:
                            raise FileNotFoundError(key)
                        return value
                    return original_stat(path, *args, **kwargs)

                encoder_observation = {}
                decoder_observation = {}
                def legacy_dumps(value, *, observation=None):
                    if observation is not None:
                        observation.update(backend="stdlib", fallback_reason="comparison_baseline")
                    return json.dumps(value, separators=(",", ":")).encode()

                def candidate_dumps(value, *, observation=None):
                    encoder = (encoders[variant] if encoders is not None else
                               deferred.encode_record_cache if variant == "deferred_native" else encode_record_cache)
                    encoded = encoder(value, observation=encoder_observation)
                    if observation is not None:
                        observation.update(encoder_observation)
                    return encoded

                def read_json(path):
                    if mode == "deferred" and path.name == "record_inventory_cache.json":
                        try:
                            decode = deferred.decode_record_cache if variant == "deferred_native" else native_loads
                            body = path.read_bytes()
                            return (decode(body, observation=decoder_observation) if variant == "deferred_native"
                                    else decode(body))
                        except (OSError, ValueError, UnicodeError):
                            return None
                    return original_read_json(path)

                if mode in {"deferred", "mapping"}:
                    loads, dumps = native_loads, candidate_dumps
                elif mode == "decode":
                    loads = json.loads if variant == "stdlib" else native_loads
                    dumps = legacy_dumps
                elif mode == "struct_gc":
                    loads, dumps = native_loads, candidate_dumps
                else:
                    loads = native_loads
                    dumps = legacy_dumps if variant == "stdlib" else candidate_dumps
                # New production versions own only this private cache writer;
                # older versions still route through private_json.dumps. The
                # fixture comparison freezes exactly the selected boundary.
                encoder_owner = inventory if hasattr(inventory, "encode_record_cache") else private_json
                encoder_name = "encode_record_cache" if encoder_owner is inventory else "dumps"
                gc.collect()
                frame = inventory.InventorySnapshot(trial_root)
                with patch.object(Path, "stat", frozen_stat), \
                     patch.object(inventory, "_selected_files", return_value=generation["selected"]), \
                     patch.object(inventory, "uuid4", return_value=UUID(int=1)), \
                     patch.object(private_json, "loads", loads), \
                     patch.object(inventory, mapping_owner,
                                  (dict, mapping_type) if variant == "dict_first_mapping" else
                                  (mapping_type,) if mode == "mapping" and mapping_owner != "Mapping" else
                                  mapping_type if mode == "mapping" else default_mapping_types), \
                     patch.object(inventory, "_COUNT_VECTOR_TYPE",
                                  (list, deferred.DeferredCounts) if variant == "deferred_native" else list), \
                     patch.object(inventory, "_read_json", read_json), \
                     patch.object(encoder_owner, encoder_name, dumps):
                    started = time.perf_counter()
                    cpu_started = time.process_time()
                    result = inventory.build_record_inventory(
                        trial_root, refresh=True, max_refresh_files=0, snapshot=frame,
                    )
                    record_wall_ms = (time.perf_counter()-started)*1000
                    if include_features:
                        feature_result = inventory.build_feature_inventory(trial_root, snapshot=frame)
                        feature_bytes = private_json.dumps(feature_result)
                    wall_ms = (time.perf_counter()-started)*1000
                    cpu_ms = (time.process_time()-cpu_started)*1000
                if include_features:
                    feature_outputs.append(hashlib.sha256(feature_bytes).hexdigest())
                persisted = cache.read_bytes()
                output = json.loads(persisted)
                output_bytes_hashes.append(hashlib.sha256(persisted).hexdigest())
                outputs.append(identity_sha256(output))
                all_sources_checked.append(checked_paths == generation["source_stats"].keys())
                trials.append({"variant": variant, "wall_ms": round(wall_ms, 3),
                               "cpu_ms": round(cpu_ms, 3),
                               "record_wall_ms": round(record_wall_ms, 3),
                               "source_stat_calls": stat_calls, "cached_files": result["cached_files"],
                               "source_observation_root": result["source_observation_root"],
                               "source_observation_matches_cache": result["source_observation_matches_cache"],
                               "output_bytes": cache.stat().st_size, "output_semantic_sha256": outputs[-1],
                               "encoder_observation": encoder_observation,
                               "decoder_observation": decoder_observation,
                               "features": len(feature_result["rows"]) if include_features else None,
                               "feature_bytes": len(feature_bytes) if include_features else None,
                               "feature_sha256": feature_outputs[-1] if include_features else None,
                               "timing_ms": result["timing_ms"]})
                del frame, output, result
                if include_features:
                    del feature_result, feature_bytes
    parity = len(set(outputs)) == 1
    bytes_parity = len(set(output_bytes_hashes)) == 1
    medians = {name: statistics.median(t["wall_ms"] for t in trials if t["variant"] == name)
               for name in names}
    cpu_medians = {name: statistics.median(t["cpu_ms"] for t in trials if t["variant"] == name)
                   for name in names}
    native_encoder_used = mode == "decode" or all(
        row["encoder_observation"].get("backend") == (
            "msgspec_validated_deferred_counts" if row["variant"] == "deferred_native" else "msgspec_validated_metadata")
        for row in trials if mode in {"struct_gc", "deferred", "mapping"} or row["variant"] == "native"
    )
    code_after = code_identity()
    code_stable = code_before == code_after
    gc_unchanged = gc.isenabled() == gc_before
    actual_struct_configs = mode != "struct_gc" or all(
        row["encoder_observation"].get("temporary_struct_gc_tracking")
        is (row["variant"] == "tracked_native") for row in trials
    )
    feature_parity = not include_features or len(set(feature_outputs)) == 1
    deferred_decoder_used = mode != "deferred" or all(
        row["decoder_observation"].get("backend") == "msgspec_deferred_counts"
        for row in trials if row["variant"] == "deferred_native"
    )
    accepted = (parity and feature_parity and deferred_decoder_used and (mode == "encode" or bytes_parity) and native_encoder_used
                and all(all_sources_checked) and code_stable and gc_unchanged and actual_struct_configs)
    return {"schema_version": 4 if include_features else 3 if mode == "struct_gc" else 2, "state": "verified" if accepted else "mismatch",
            "scope": "full record cache decode, frozen-source identity checks, aggregation and atomic cache/index writes"
            + ("; full feature verification, reduction and JSON" if include_features else ""),
            "excludes": ["imports", "directory_discovery", "live_or_cold_source_io", "Parquet_footer_refresh",
                         "feature_publisher", "provider_calls", "network", "browser"],
            "input_sha256": hashlib.sha256(body).hexdigest(), "input_bytes": len(body),
            "captured_source_sha256": generation["sha256"],
            "input_representation": "ASCII-escaped canonical trial seed; captured source may be UTF-8 JSON",
            "native_available": private_json.native_available(),
            "mode": mode,
            "native_backend": "msgspec-decode_stdlib-encode" if mode == "decode" else "msgspec-validated-metadata-encode",
            "native_encoder_used": native_encoder_used,
            "deferred_decoder_used": deferred_decoder_used,
            "feature_bytes_parity": feature_parity,
            "native_version": private_json._native.__version__,
            "production_source_writes": 0,
            "code_before": code_before, "code_after": code_after, "code_stable": code_stable,
            "source_signature": generation["signature"],
            "selected_source_paths": len(generation["source_stats"]),
            "all_source_identities_checked": all(all_sources_checked),
            "parity": parity, "cache_bytes_parity": bytes_parity, "median_ms": medians,
            "cpu_median_ms": cpu_medians,
            "speedup": round(medians[names[0]] / medians[names[1]], 4),
            "codec_reference_source": (REPO_ROOT / "stockagent/live/data_monitor_record_codec.py").read_text()
            if mode == "struct_gc" else None,
            "interpreter_gc_unchanged": gc_unchanged,
            "actual_struct_configs_verified": actual_struct_configs,
            "process_peak_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
            "rss_scope": "whole ABBA process high-water mark; not per-variant peak",
            "trials": trials}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--native-package-path", type=Path)
    parser.add_argument("--repeats", type=int, default=2)
    parser.add_argument("--mode", choices=("decode", "encode", "struct_gc", "deferred", "mapping"), default="decode")
    args = parser.parse_args()
    if args.repeats < 1:
        parser.error("repeats must be positive")
    if args.native_package_path:
        if args.mode != "decode":
            parser.error("encoding comparison must use the installed canonical runtime")
        sys.path.insert(0, str(args.native_package_path.resolve()))
        import msgspec
        private_json._native = msgspec
    if not private_json.native_available():
        parser.error("native candidate must be available; install or pass --native-package-path")
    if args.output.exists():
        parser.error("output must be a new evidence file")
    result = benchmark(REPO_ROOT, repeats=args.repeats, mode=args.mode)
    atomic_write_json(args.output, result)
    print(json.dumps({key: result[key] for key in ("state", "parity", "input_bytes", "median_ms", "speedup")}))
    return 0 if result["state"] == "verified" else 1


if __name__ == "__main__":
    raise SystemExit(main())

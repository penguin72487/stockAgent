"""Incremental, lossless value-only view of a verified factorized panel.

This changes the model input ABI, not release clocks, NULL barriers, eligibility,
execution or sampling. It does not download or rebuild source observations.
"""
from __future__ import annotations

import copy
import json
import os
from pathlib import Path
import time

import numpy as np

from downloader.artifact_io import atomic_write_json
from stockagent.data.factorized_panel import (
    CONTRACT, VALUE_ONLY_CONTRACT, _BlockCache, _ColumnBlock,
    file_sha256, verify_factorized_members, write_array_block,
)
from stockagent.data.tw_feature_semantic_report import write_factorized_feature_report


def _value_positions(names: list[str]) -> np.ndarray:
    if len(names) % 4:
        raise ValueError("parent channels are not complete value/metadata groups")
    for offset in range(0, len(names), 4):
        value = names[offset]
        if names[offset:offset + 4] != [value, value + "__available", value + "__age_days", value + "__updated"]:
            raise ValueError("parent channel names do not prove value/metadata ordering")
    return np.arange(0, len(names), 4, dtype=np.int64)


def project_factorized_values(parent_path: Path, output: Path) -> dict:
    """Keep every value bit; emit only value channels and complete provenance."""
    started = time.perf_counter()
    parent_path = parent_path.resolve(strict=True)
    root = parent_path.parent
    parent_sha = file_sha256(parent_path)
    parent = json.loads(parent_path.read_text())
    if (parent.get("contract") != CONTRACT or parent.get("status") != "complete"
            or parent.get("research_only") is not True
            or parent.get("historical_point_in_time") is not False
            or parent.get("feature_lag") != 1):
        raise ValueError("projection requires a complete, fixed legacy research panel")
    individual = _value_positions(parent["individual_channels"])
    common = _value_positions(parent["common_channels"])
    if (len(individual) + len(common) != parent["value_features"]
            or len(individual) != parent["individual_quantities"]
            or len(common) != parent["shared_quantities"]):
        raise ValueError("parent quantities differ from proven channel groups")
    if output.exists():
        raise FileExistsError("new channel ABI requires a fresh output root")
    verify_factorized_members(root, parent)
    output.mkdir(parents=True)
    result = copy.deepcopy(parent)
    result.update(contract=VALUE_ONLY_CONTRACT, model_channel_policy="value_only",
        individual_channels=[parent["individual_channels"][i] for i in individual],
        common_channels=[parent["common_channels"][i] for i in common],
        logical_model_channels=len(parent["base_feature_names"]) + parent["value_features"],
        training_missingness="last already released finite value; neutral zero for missing input; raw NULL, TTL, report and issuer lifecycle barriers preserved; observation metadata excluded from model",
        training_ready=False, gpu_training_verified=False, execution_preflight_passed=False,
        parent_feature_manifest_sha256=parent_sha,
        projection_contract="lossless_value_channel_projection_v1")
    blocks = []
    total_bytes = 0
    for block in parent["blocks"]:
        proofs = block.get("symbol_blocks", [block])
        # At most one decoded physical part is resident in this transformer.
        reader = _BlockCache(root, proofs, 0)
        converted = []
        for index, proof in enumerate(proofs):
            data = reader.get(index)
            if isinstance(data, _ColumnBlock):
                selected = data.columns % 4 == 0
                columns = data.columns[selected].astype(np.int64) // 4
                values = data.values[..., selected]
            else:
                columns = individual
                values = data[..., individual]
                columns = columns // 4
            new_proof = write_array_block(output / proof["path"], values,
                omit_zero_columns=True, logical_columns=columns,
                logical_shape=(*proof["shape"][:2], len(individual)))
            # Independently decode the newly encoded part and compare every
            # retained bit, including -0.0. Implicit positive zero stays exact.
            decoded = _BlockCache(output, [new_proof], 0).get(0)
            if not isinstance(decoded, _ColumnBlock):
                raise ValueError("value projection did not retain the lossless codec")
            expected_columns = np.flatnonzero(np.any(values.view(np.uint32) != 0, axis=(0, 1)))
            if (not np.array_equal(decoded.columns, columns[expected_columns])
                    or not np.array_equal(decoded.values.view(np.uint32), values[..., expected_columns].view(np.uint32))):
                raise ValueError("projected numerical value bits differ from parent")
            if "symbol_blocks" in block:
                new_proof["symbol_start"] = proof["symbol_start"]
            converted.append(new_proof)
            total_bytes += new_proof["bytes"]
        if "symbol_blocks" in block:
            blocks.append({"start": block["start"],
                "shape": [*block["shape"][:2], len(individual)],
                "symbol_blocks": converted, "bytes": sum(p["bytes"] for p in converted)})
        else:
            blocks.append({**converted[0], "start": block["start"]})
        print(f"[factorized-values] projected dates {block['start']}:{block['start'] + block['shape'][0]}", flush=True)
    result["blocks"] = blocks
    original_common = np.load(root / parent["common"]["path"], allow_pickle=False, mmap_mode="r")
    if original_common.shape != (len(parent["dates"]), len(parent["common_channels"])):
        raise ValueError("parent shared feature calendar/channel mismatch")
    common_values = np.ascontiguousarray(original_common[:, common])
    np.save(output / "common.npy", common_values, allow_pickle=False)
    reloaded = np.load(output / "common.npy", allow_pickle=False)
    if not np.array_equal(reloaded.view(np.uint32), common_values.view(np.uint32)):
        raise ValueError("projected common value bits differ")
    result["common"] = {"path": "common.npy", "shape": list(common_values.shape),
        "sha256": file_sha256(output / "common.npy"), "bytes": (output / "common.npy").stat().st_size}
    # Same-filesystem immutable names preserve raw NULL observations, clocks
    # and audit metadata without copying or re-reading provider datasets.
    members = [p["path"] for p in parent.get("observations", []) + parent.get("annual_events", [])]
    members += ["feature_dictionary.json", "feature_coverage.csv", "quality_masks.json", "execution_rules.parquet"]
    for relative in dict.fromkeys(members):
        source = (root / relative).resolve(strict=True)
        target = output / relative
        if not source.is_relative_to(root) or Path(relative).is_absolute() or ".." in Path(relative).parts:
            raise ValueError("parent evidence escaped its immutable root")
        target.parent.mkdir(parents=True, exist_ok=True)
        os.link(source, target)
    result["projection_wall_seconds"] = time.perf_counter() - started
    result["builder_sha256"] = file_sha256(Path(__file__))
    result.pop("build_wall_seconds", None)
    if file_sha256(parent_path) != parent_sha:
        raise ValueError("parent manifest changed during value projection")
    verify_factorized_members(output, result)
    manifest_path = output / "factorized_manifest.json"
    atomic_write_json(manifest_path, result)
    report = write_factorized_feature_report(manifest_path, output / "feature_report")
    proof = {"state": "value_only_view_verified_not_training_acceptance",
        "contract": result["projection_contract"], "parent_manifest_sha256": parent_sha,
        "manifest_sha256": file_sha256(manifest_path),
        "source_snapshot_id": result["source_snapshot_id"],
        "value_features": result["value_features"],
        "logical_model_channels": result["logical_model_channels"],
        "removed_metadata_channels": parent["logical_model_channels"] - result["logical_model_channels"],
        "individual_compressed_bytes": total_bytes,
        "retained_value_bits_exact": True, "raw_null_and_execution_unchanged": True,
        "complete_workflow_wall_seconds": time.perf_counter() - started,
        "feature_report": report, "formal_training_started": False, "training_ready": False}
    atomic_write_json(output / "value_projection_receipt.json", proof)
    return proof

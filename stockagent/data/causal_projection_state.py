"""Lossless preparer state, distinct from Float32 model inputs and checkpoints.

The original causal sweep owns transitions. These helpers only bind and store
its Float64 values, release ordinals, period floors and column candidates.
Private generation publication/locking remains with the existing producer.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pyarrow as pa

from downloader.artifact_io import atomic_write_json
from stockagent.data.factorized_panel import file_sha256
from stockagent.data_sync.factorized_projection_reuse import seal_new_file

CONTRACT = "factorized_exact_causal_preparer_state_v1"
ARRAYS = ("values", "observed", "available_count")


def implementation_identity():
    from stockagent.data import tw_public_incremental
    return {path.name: file_sha256(path) for path in (Path(__file__), Path(tw_public_incremental.__file__))}


def projection_prefix(dates, lifecycle, events, row):
    """Bind every consumed event, decision date and issuer episode exactly.

    Events are original typed per-session proofs, not only a maximum date or
    a row count. Changed/deleted old rows invalidate continuation even if the
    source simultaneously appends a new session.
    """
    clock = dates[row].isoformat()
    partitions = {feature: {day: proof for day, proof in item["sessions"].items() if day <= clock}
                  for feature, item in events.items()}
    partitions = {feature: proof for feature, proof in partitions.items() if proof}
    encoded = json.dumps({"calendar": [day.isoformat() for day in dates[:row + 1]],
                          "events": partitions}, sort_keys=True, separators=(",", ":")).encode()
    return {"row": row, "decision": clock,
            "events_calendar_sha256": hashlib.sha256(encoded).hexdigest(),
            "lifecycle_sha256": hashlib.sha256(np.ascontiguousarray(lifecycle[1:row + 1]).view(np.uint8)).hexdigest()}


def continuation_row(previous, identity, dates, lifecycle, event_proofs):
    """A missing/incompatible checkpoint is a full build, never stale reuse."""
    daily = previous.get("daily_incremental", {})
    prior_identity = previous.get("paper_inference", {}).get("source_identity", {})
    fixed = ("parent", "start_date", "builder_sha", "sweep_sha", "writer_sha", "config_sha",
             "storage_implementation", "annual_preparation", "daily_implementation", "normalization_implementation")
    if not isinstance(daily, dict) or daily.get("contract") != CONTRACT:
        return 0, "checkpoint_bootstrap"
    if any(prior_identity.get(key) != identity.get(key) or key not in identity for key in fixed):
        return 0, "implementation_or_selected_abi_changed"
    row = daily.get("prefix", {}).get("row")
    if type(row) is not int or not 0 < row < len(dates):
        return 0, "checkpoint_calendar_not_covered"
    if daily.get("prefix") != projection_prefix(dates, lifecycle, event_proofs, row):
        return 0, "historical_events_calendar_or_lifecycle_changed"
    if set(daily.get("states", {})) != {"individual", "common"}:
        return 0, "checkpoint_states_missing"
    if any(daily["states"][kind].get("row") != row for kind in ("individual", "common")):
        return 0, "checkpoint_state_clocks_disagree"
    return row, "verified_exact_prefix"


def sweep_identity(symbols, definitions, policy, symbol_block_size):
    return hashlib.sha256(json.dumps({"contract": CONTRACT, "symbols": list(symbols),
        "definitions": definitions, "policy": policy, "symbol_block_size": symbol_block_size},
        sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def validate_state(state, dates, symbols, definitions, policy, symbol_block_size):
    expected = sweep_identity(symbols, definitions, policy, symbol_block_size)
    row = state.get("row")
    if (state.get("contract") != CONTRACT or state.get("identity") != expected
            or type(row) is not int or not 0 < row < len(dates)
            or state.get("decision") != dates[row].isoformat()):
        raise ValueError("incompatible causal preparer checkpoint")
    for key, dtype, shape in (("values", np.dtype("float64"), (len(symbols), len(definitions))),
                              ("observed", np.dtype("int32"), (len(symbols), len(definitions))),
                              ("available_count", np.dtype("int64"), (len(definitions),))):
        value = state.get(key)
        if not isinstance(value, np.ndarray) or value.dtype != dtype or value.shape != shape:
            raise ValueError(f"causal checkpoint {key} lost its original precision/shape")
    for key in ("report_periods", "event_periods"):
        rows = state.get(key)
        if (not isinstance(rows, list) or any(not isinstance(item, (tuple, list)) or len(item) != 3
                                            or any(not isinstance(value, str) for value in item) for item in rows)
                or len({tuple(item[:2]) for item in rows}) != len(rows)):
            raise ValueError("invalid causal checkpoint report floors")
    candidates = state.get("column_candidates")
    if (not isinstance(candidates, list) or len(candidates) != (len(symbols) + symbol_block_size - 1) // symbol_block_size
            or any(not isinstance(columns, list) or columns != sorted(set(columns))
                   or any(type(column) is not int or not 0 <= column < len(definitions) for column in columns)
                   for columns in candidates)):
        raise ValueError("invalid causal checkpoint column candidates")
    return row


def save_state(root: Path, name: str, state: dict, *, write_budget=None) -> dict:
    """Write only new private members; seal before the generation is committed."""
    if Path(name).name != name or not name:
        raise ValueError("state name must be an immediate generation member")
    metadata = {key: value for key, value in state.items() if key not in ARRAYS}
    metadata["arrays"] = {}
    for key in ARRAYS:
        array = state[key]
        path = root / f"{name}_{key}.npy.zst"
        with (write_budget.open(path) if write_budget is not None else path.open("xb")) as raw:
            with pa.CompressedOutputStream(raw, "zstd") as stream:
                np.save(stream, array, allow_pickle=False)
        seal_new_file(path)
        metadata["arrays"][key] = {"path": path.name, "sha256": file_sha256(path),
            "bytes": path.stat().st_size, "shape": list(array.shape), "dtype": array.dtype.str}
    path = root / f"{name}.json"
    if write_budget is not None:
        write_budget.charge_json(metadata)
    atomic_write_json(path, metadata)
    seal_new_file(path)
    return {"path": path.name, "sha256": file_sha256(path), "bytes": path.stat().st_size,
            "row": state["row"], "decision": state["decision"], "arrays": metadata["arrays"]}


def load_state(root: Path, proof: dict) -> dict:
    relative = proof["path"]
    if Path(relative).name != relative:
        raise ValueError("causal checkpoint escaped its generation")
    path = root / relative
    if path.stat().st_size != proof["bytes"] or file_sha256(path) != proof["sha256"]:
        raise ValueError("causal checkpoint metadata changed")
    state = json.loads(path.read_text())
    if state.get("arrays") != proof.get("arrays"):
        raise ValueError("causal checkpoint array inventory changed")
    for key in ARRAYS:
        item = state["arrays"][key]
        relative = item["path"]
        if Path(relative).name != relative:
            raise ValueError("causal state array escaped its generation")
        path = root / relative
        if path.stat().st_size != item["bytes"] or file_sha256(path) != item["sha256"]:
            raise ValueError("causal state array changed")
        with pa.input_stream(path, compression="zstd") as stream:
            array = np.lib.format.read_array(stream, allow_pickle=False)
        if list(array.shape) != item["shape"] or array.dtype.str != item["dtype"]:
            raise ValueError("causal state array shape/precision changed")
        state[key] = array
    return state

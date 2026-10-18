"""Read-only service dependency gates shared by artifact storage operations."""

from __future__ import annotations

from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import re
import stat
from typing import Any, Iterable

import yaml

from stockagent.data_sync.desync_snapshots import SnapshotError

FORWARD_CONSUMER_CONTRACT = "completed_forward_current_consumers_v1"
MAX_SERVICE_MARKETS = 256
MAX_CONTROL_BYTES = 1024**2
MAX_SIGNAL_SUMMARY_BYTES = 64 * 1024**2


def _market_config_paths(repo_root: Path) -> list[Path]:
    root = repo_root / "services/discord_bot/markets"
    # Match the canonical live loader, including its YAML/YML ordering.
    return sorted(root.glob("*.yaml")) + sorted(root.glob("*.yml"))


def _control_bytes(path: Path, *, limit: int) -> tuple[bytes, dict]:
    """Read bounded regular control metadata, not a cache/model payload."""
    path = Path(os.path.abspath(path))
    if path.resolve(strict=True) != path:
        raise ValueError("redirected service control file")
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC)
    with os.fdopen(descriptor, "rb") as stream:
        before = os.fstat(stream.fileno())
        if (not stat.S_ISREG(before.st_mode) or before.st_size > limit
                or before.st_uid != os.geteuid() or before.st_mode & 0o022):
            raise ValueError("untrusted or oversized service control file")
        raw = stream.read(limit + 1)
        def generation(info):
            return [info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns,
                    info.st_ctime_ns, info.st_mode, info.st_uid, info.st_gid]
        if (len(raw) != before.st_size or generation(before) != generation(os.fstat(stream.fileno()))
                or generation(before) != generation(path.lstat())):
            raise ValueError("service control file changed during read")
    return raw, {"path": str(path), "identity": generation(before),
                 "sha256": hashlib.sha256(raw).hexdigest(), "limit": limit}


def _json_object(raw: bytes) -> dict:
    def unique(pairs):
        value = {}
        for key, child in pairs:
            if key in value:
                raise ValueError("duplicate service metadata key")
            value[key] = child
        return value
    try:
        value = json.loads(raw, object_pairs_hook=unique)
    except RecursionError as error:
        raise ValueError("service metadata nesting exceeds parser bound") from error
    if not isinstance(value, dict):
        raise ValueError("service metadata is not an object")
    return value


def _completed_forward_dependencies(forward):
    """Yield the two output roles the canonical v3 producer can commit.

    Yield known dependencies before validating the next one, so a malformed
    optional record never removes an already observed current pin. Unknown
    nesting is incomplete metadata, not permission to ignore another output.
    This is reference discovery, not payload verification or result acceptance.
    """
    def key(record):
        if (not isinstance(record, dict)
                or record.get("contract") != "completed_tw_panel_forward_cpu_v3"
                or not isinstance(record.get("key"), str)
                or re.fullmatch(r"[0-9a-f]{64}", record["key"]) is None):
            raise ValueError("latest signal has no supported forward key")
        return record["key"]

    yield "masked", key(forward)
    if "unconstrained" in forward:
        raw = forward["unconstrained"]
        yield "unconstrained", key(raw)
        if "unconstrained" in raw:
            raise ValueError("unsupported nested unconstrained forward dependency")


def completed_forward_consumer_pins(repo_root: Path) -> dict:
    """Discover selected current TW day-trade signals without importing Torch.

    This is a scoped, read-only census, not all-fleet ownership or integrity
    acceptance. Use the live loader/output resolver, not an independent model
    selection rule. Verify every observed config/state/pointer/summary again;
    absence, races and unknown forward contracts leave the census incomplete.
    Both masked and optional unconstrained outputs are dependencies. No
    historical signal tree, model, provider or forward payload is opened.
    """
    from stockagent.data_sync.node_roles import training_only_node
    from stockagent.live.market_config import load_market_config, resolved_live_output_dir

    repo_root = repo_root.resolve()
    controls, consumers, errors, absent, keys = {}, [], [], set(), set()
    report = {"contract": FORWARD_CONSUMER_CONTRACT,
              "scope": "enabled_tw_day_trade_selected_latest_signals",
              "complete_for_scope": False, "all_fleet_consumers_exhaustive": False}

    def read(path, limit=MAX_CONTROL_BYTES):
        raw, observed = _control_bytes(path, limit=limit)
        previous = controls.setdefault(observed["path"], observed)
        if previous != observed:
            raise ValueError("service control generation changed")
        return raw

    paths = []
    try:
        if training_only_node():
            report.update(scope="locally_enrolled_training_only_node", complete_for_scope=True,
                          keys=[], consumers=[], controls=[], errors=[])
            return report
        directory = repo_root / "services/discord_bot/markets"
        if not directory.is_dir() or directory.resolve(strict=True) != directory:
            raise ValueError("service market directory is missing or redirected")
        paths = _market_config_paths(repo_root)
        if len(paths) > MAX_SERVICE_MARKETS:
            raise ValueError("service market inventory exceeds its bound")
        state_path = repo_root / "artifacts/discord_bot/state.json"
        try:
            state = _json_object(read(state_path))
            runtime_markets = state["markets"]
            if not isinstance(runtime_markets, dict):
                raise ValueError("invalid runtime market state")
        except FileNotFoundError:
            absent.add(state_path)
            runtime_markets = {}
        seen = set()
        for config_path in paths:
            try:
                raw = yaml.safe_load(read(config_path))
                if not isinstance(raw, dict):
                    raise ValueError("market config is not an object")
                selection = raw.get("model_selection_path")
                if selection:
                    selection_path = Path(selection)
                    if not selection_path.is_absolute():
                        selection_path = config_path.parent / selection_path
                    read(selection_path)
                # The canonical loader applies selection and parses all flags.
                cfg = load_market_config(config_path)
                if cfg.market in seen:
                    raise ValueError("duplicate market identity")
                seen.add(cfg.market)
                runtime = runtime_markets.get(cfg.market, {})
                if not isinstance(runtime, dict):
                    raise ValueError("invalid runtime market entry")
                active = cfg.enabled or runtime.get("enabled") is True or cfg.overnight_simulation_enabled
                if not active or not cfg.day_trade_simulation_enabled or cfg.market_type not in {None, "tw"}:
                    continue
                output = resolved_live_output_dir(cfg)
                if not output.is_absolute():
                    output = repo_root / output
                output = Path(os.path.abspath(output))
                pointer_path = output / "latest_signal.json"
                pointer = _json_object(read(pointer_path))
                if (type(pointer.get("schema_version")) is not int or pointer["schema_version"] != 2
                        or pointer.get("market") != cfg.market
                        or not isinstance(pointer.get("signal_id"), str) or not pointer["signal_id"]
                        or not isinstance(pointer.get("summary_path"), str) or not pointer["summary_path"]):
                    raise ValueError("invalid latest signal pointer")
                summary_path = Path(pointer["summary_path"])
                if not summary_path.is_absolute():
                    summary_path = repo_root / summary_path
                summary_path = Path(os.path.abspath(summary_path))
                if output not in summary_path.parents:
                    raise ValueError("summary escapes the selected live output")
                summary = _json_object(read(summary_path, MAX_SIGNAL_SUMMARY_BYTES))
                if summary.get("market") != cfg.market or summary.get("signal_id") != pointer["signal_id"]:
                    raise ValueError("latest signal and summary identities differ")
                forward = (summary.get("live_latency") or {}).get("completed_forward_cache")
                for output_kind, key in _completed_forward_dependencies(forward):
                    keys.add(key)
                    consumers.append({"market": cfg.market, "key": key, "output_kind": output_kind,
                                      "config_path": str(config_path), "pointer_path": str(pointer_path),
                                      "summary_path": str(summary_path), "signal_id": pointer["signal_id"]})
            except (OSError, ValueError, TypeError, KeyError, AttributeError, yaml.YAMLError) as error:
                errors.append({"path": str(config_path), "error_type": type(error).__name__,
                               "reason": str(error)})
        if any(not isinstance(value, dict) for value in runtime_markets.values()):
            raise ValueError("invalid runtime market entry")
        if any(value.get("enabled") is True and name not in seen for name, value in runtime_markets.items()):
            raise ValueError("runtime enables an unregistered service market")
        for observed in tuple(controls.values()):
            if _control_bytes(Path(observed["path"]), limit=observed["limit"])[1] != observed:
                raise ValueError("consumer metadata changed during census")
        if (paths != _market_config_paths(repo_root)
                or any(path.exists() or path.is_symlink() for path in absent)):
            raise ValueError("consumer namespace changed during census")
    except (OSError, ValueError, TypeError, KeyError, SnapshotError, yaml.YAMLError) as error:
        errors.append({"path": str(repo_root), "error_type": type(error).__name__, "reason": str(error)})
    report.update(complete_for_scope=not errors, keys=sorted(keys), consumers=consumers,
                  controls=sorted(controls.values(), key=lambda row: row["path"]),
                  absent_controls=sorted(str(path) for path in absent), errors=errors)
    return report


def _strings(value: Any, prefix: str = ""):
    if isinstance(value, (str, os.PathLike)):
        yield prefix, os.fspath(value)
    elif isinstance(value, dict):
        for key, child in value.items():
            yield from _strings(child, f"{prefix}.{key}" if prefix else str(key))
    elif isinstance(value, (list, tuple)):
        for index, child in enumerate(value):
            yield from _strings(child, f"{prefix}[{index}]")


def is_repository_data_path(value: str) -> bool:
    return (Path(value).is_absolute() or value.startswith("artifacts/")
            or (value.startswith("data_") and "/" in value))


def _explicit_empty_minute_replay(replay: dict[str, Any]) -> bool:
    """Recognize the canonical producer's no-position/no-pending-order scope.

    No requested symbols means the minute loader opened no price files. This
    is not missing-price evidence, an empty market, or permission to retire any
    other replay input. Unknown contracts and incomplete/contradictory counters
    still fail the normal source-pin gate.
    """
    return (
        type(replay.get("contract")) is str
        and replay["contract"] in {
            "historical_1m_ohlcv_counterfactual_not_executable_bid_ask",
            "historical_1m_ohlcv_full_target_counterfactual_no_liquidity_claim_v1",
        }
        and replay.get("historical_best_bid_ask_claimed") is False
        and all(type(replay.get(key)) is int and replay[key] == 0 for key in (
            "requested_symbols", "resolved_symbols", "ignored_zero_volume_rows",
            "ignored_unverified_volume_rows",
        ))
        and replay.get("missing_symbols") == []
        and replay.get("official_no_trade_carried_symbols") == []
        and replay.get("source_counts") == {}
        and replay.get("source_files") == []
    )


def service_artifact_paths(repo_root: Path) -> list[dict[str, str]]:
    """Resolve active Discord and independently enabled simulation consumers.

    Includes model-selection overrides/candidates and resolved experiment data,
    runner and initialization paths. A disabled Discord toggle does not stop an
    overnight engine. Missing or malformed referenced configs fail closed.
    """
    from stockagent.data_sync.node_roles import training_only_node
    if training_only_node():
        return []
    from stockagent.config import load_config
    from stockagent.live.market_config import (
        _apply_model_selection,
        _bool_value,
        _str_tuple,
    )

    repo_root = repo_root.resolve()
    state_path = repo_root / "artifacts/discord_bot/state.json"
    runtime_markets = {}
    if state_path.is_file():
        state = json.loads(state_path.read_text())
        if not isinstance(state, dict) or not isinstance(state.get("markets"), dict):
            raise SnapshotError("Discord runtime market state is invalid")
        runtime_markets = state["markets"]
    paths = []
    enabled_markets = []
    replay_state_roots: set[Path] = set()
    for config_path in _market_config_paths(repo_root):
        raw = yaml.safe_load(config_path.read_text())
        if not isinstance(raw, dict):
            raise SnapshotError(f"invalid service market config: {config_path}")
        market = str(raw.get("market") or raw.get("id") or config_path.stem)
        runtime = runtime_markets.get(market, {})
        if not isinstance(runtime, dict):
            raise SnapshotError(f"invalid Discord runtime state for {market}")
        if not (
            _bool_value(raw.get("enabled"), True)
            or runtime.get("enabled") is True
            or _bool_value(raw.get("overnight_simulation_enabled"), False)
        ):
            continue
        enabled_markets.append(market)
        # Protect both configured and selected assets, including fallback inputs.
        selected = _apply_model_selection(raw, market_config_path=config_path)
        for document in (raw, selected):
            value = document.get("day_trade_simulation_state_dir")
            if value:
                path = Path(str(value))
                replay_state_roots.add(path if path.is_absolute() else repo_root / path)
        documents = [
            (str(config_path.relative_to(repo_root)), raw),
            (f"{market}:model-selection", selected),
        ]
        experiments = {
            item.get("config_path") or item.get("config") for item in (raw, selected)
        } - {None, ""}
        experiments.update(_str_tuple(selected.get("model_candidate_config_paths")))
        for experiment in sorted(experiments):
            config_sources = {}
            documents.append(
                (str(experiment), asdict(load_config(repo_root / experiment, source_signatures=config_sources)))
            )
            for source in config_sources:
                paths.append({"market": market, "document": str(experiment), "field": "config_source",
                              "configured_path": str(source), "path": str(source.absolute()),
                              "resolved_path": str(source.resolve(strict=False))})
        for label, document in documents:
            for field, value in _strings(document):
                if not is_repository_data_path(value):
                    continue
                path = Path(value)
                if not path.is_absolute():
                    path = repo_root / path
                paths.append(
                    {
                        "market": market,
                        "document": label,
                        "field": field,
                        "configured_path": value,
                        "path": str(path.absolute()),
                        "resolved_path": str(path.resolve(strict=False)),
                    }
                )
    if enabled_markets:
        # The live panel disk cache is an implicit runtime dependency, even
        # when no experiment YAML names it. Resolve its canonical default.
        from stockagent.live.market_config import live_panel_disk_cache_root

        path = live_panel_disk_cache_root(repo_root=repo_root)
        paths.append({"market": "shared-live-panels", "document": "live.signal_engine",
                      "field": "live_panel_disk_cache_root", "configured_path": str(path),
                      "path": str(path.absolute()), "resolved_path": str(path.resolve(strict=False))})
        # The original signal producer resolves this implicit cache separately
        # from the live output root. Preserve its exact override semantics (no
        # strip/expanduser), without importing inference code/Torch to find it.
        value = os.environ.get("STOCKAGENT_COMPLETED_INFERENCE_CACHE_ROOT")
        path = Path(value or str(repo_root / "artifacts/cache/completed_live_inference"))
        if not path.is_absolute():
            path = repo_root / path
        paths.append({"market": "shared-completed-forward", "document": "live.signal_engine",
                      "field": "completed_inference_cache_root", "configured_path": str(path),
                      "path": str(path.absolute()), "resolved_path": str(path.resolve(strict=False))})
        # The service supervisor sets this default before launching the web
        # process. A storage command need not inherit that process environment.
        value = os.environ.get("STOCKAGENT_DASHBOARD_INDEX_CACHE_DIR", "").strip()
        path = Path(value) if value else repo_root / "artifacts/cache/tw_day_trade_dashboard_indexes"
        if not path.is_absolute():
            path = repo_root / path
        paths.append({"market": "shared-day-trade-dashboard", "document": "scripts/run_tw_day_trade_services.sh",
                      "field": "dashboard_index_cache_dir", "configured_path": str(path),
                      "path": str(path.absolute()), "resolved_path": str(path.resolve(strict=False))})
        # Minute maintenance reads implicit local fallbacks, including historical
        # chunks under data_repair. These are live inputs despite their location.
        from stockagent.live.minute_source_paths import (
            DEFAULT_LOCAL_MINUTE_ROOTS, DEFAULT_LOCAL_MINUTE_CACHE_ROOTS,
        )

        for value in (*DEFAULT_LOCAL_MINUTE_ROOTS, *DEFAULT_LOCAL_MINUTE_CACHE_ROOTS):
            path = value if value.is_absolute() else repo_root / value
            paths.append({"market": "shared-day-trade-minute-curves",
                          "document": "scripts/rebuild_tw_day_trade_minute_curves.py",
                          "field": "local_minute_source", "configured_path": str(path),
                          "path": str(path.absolute()), "resolved_path": str(path.resolve(strict=False))})
        replay_state_roots.add(repo_root / "artifacts/live/tw_day_trade_simulation")
        for state_root in sorted(replay_state_roots):
            receipt_path = state_root / "rebuild_receipt.json"
            if not receipt_path.is_file():
                continue
            receipt = json.loads(receipt_path.read_text())
            if not isinstance(receipt, dict) or not isinstance(receipt.get("sessions", []), list):
                raise SnapshotError(f"invalid execution replay source receipt: {receipt_path}")
            for session in receipt.get("sessions") or ():
                if not isinstance(session, dict):
                    raise SnapshotError(f"invalid execution replay session: {receipt_path}")
                replay = session.get("intraday_replay") or {}
                if not replay:
                    continue
                if isinstance(replay, dict) and _explicit_empty_minute_replay(replay):
                    continue
                if not isinstance(replay, dict) or not isinstance(replay.get("source_files"), list) or not replay["source_files"]:
                    raise SnapshotError(f"execution replay source pins are invalid: {receipt_path}")
                for item in replay["source_files"]:
                    value = item.get("path") if isinstance(item, dict) else None
                    digest = str(item.get("sha256") or "") if isinstance(item, dict) else ""
                    if not isinstance(value, str) or not value or len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
                        raise SnapshotError(f"invalid execution replay source pin: {receipt_path}")
                    path = Path(value)
                    if not path.is_absolute():
                        path = repo_root / path
                    paths.append({"market": "shared-day-trade-execution-replay",
                                  "document": str(receipt_path), "field": "intraday_replay.source_files",
                                  "configured_path": str(path), "path": str(path.absolute()),
                                  "resolved_path": str(path.resolve(strict=False))})
    return paths


def artifact_service_references(
    sources: Iterable[Path], repo_root: Path
) -> dict[str, list[str]]:
    sources = tuple(path.absolute() for path in sources)
    result = {str(path): [] for path in sources}
    source_views = [(source, {"path": str(source), "resolved_path": str(source.resolve(strict=False))})
                    for source in sources]
    seen = set()
    for row in service_artifact_paths(repo_root):
        identity = tuple(row[key] for key in ("document", "field", "configured_path", "path", "resolved_path"))
        if identity in seen:
            continue
        seen.add(identity)
        reference_views = {key: str(Path(row[key])) for key in ("path", "resolved_path")}
        for source, views in source_views:
            for key in ("path", "resolved_path"):
                reference = reference_views[key]
                resolved = views[key]
                if (
                    reference == resolved
                    or resolved.startswith(reference.rstrip("/") + "/")
                    or reference.startswith(resolved.rstrip("/") + "/")
                ):
                    result[str(source)].append(
                        f"{row['document']}:{row['field']}:{row['configured_path']}"
                    )
                    break
    return {path: sorted(set(refs)) for path, refs in result.items()}

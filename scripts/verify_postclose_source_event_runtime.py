"""Verify the native cache-only event bridge without changing market sources.

--baseline captures the current curve/signal identities. --notify tests an
explicit temporary engineering PathChanged target on the same native path
owner; real sources and their publication clocks are never touched. The final
--readback mode requires the normal watch set and an unchanged terminal worker.
This proves engineering notification delivery, not a future real publication.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json
from scripts import verify_completed_session_cache as cache
from scripts.verify_public_feature_index_runtime import PROTECTED, UNIT, identity, sha
from stockagent.live.tw_day_trade_simulation import resolve_day_trade_rule_data_dir

PATH_UNIT = "stockagent-discord-artifact-maintenance.path"
CACHE_UNIT = "stockagent-discord-postclose-cache.service"
RULE_ROOT = Path("/srv/stockagent-live/data_tw_public")
MARKER = Path("/run/stockagent-postclose-source-event-proof/notification.json")
WATCHES = {str(ROOT / "artifacts/data_refresh/tw_public/completed_session/latest.json"),
           *(str(RULE_ROOT / f"{venue}_day_trade_eligibility.parquet")
             for venue in ("twse", "tpex"))}


def code_proofs():
    paths = (Path(__file__), ROOT / "scripts/run_discord_artifact_maintenance.py",
             ROOT / "services/discord_bot/bot.py", ROOT / "stockagent/live/signal_engine.py",
             ROOT / "stockagent/live/completed_inference_cache.py",
             ROOT / "deploy/systemd/stockagent-discord-artifact-maintenance.path.in",
             ROOT / "scripts/run_discord_artifact_maintenance.sh",
             Path("/etc/systemd/system") / CACHE_UNIT)
    return {str(path): sha(path) for path in paths}


def show(unit, fields):
    result = subprocess.run(["systemctl", "show", unit,
                             *(arg for field in fields for arg in ("-p", field))],
                            capture_output=True, check=True, text=True, timeout=5)
    # systemctl prints list properties (notably Paths) as repeated keys.
    # A dict comprehension silently loses every entry except the last.
    values = {}
    for line in result.stdout.splitlines():
        if "=" in line:
            key, value = line.split("=", 1)
            values[key] = f"{values[key]} {value}" if key in values else value
    return values


def worker():
    return show(CACHE_UNIT, ("ActiveState", "SubState", "Result", "ExecMainPID",
                            "ExecMainStatus", "ExecMainStartTimestampMonotonic",
                            "ExecMainExitTimestampMonotonic", "CPUUsageNSec"))


def check_cache_only_owner():
    state = show(CACHE_UNIT, ("Type", "ExecStart", "DropInPaths"))
    args = re.search(r"argv\[\]=(.*?) ; ignore_errors=", state.get("ExecStart", ""))
    expected = f"/usr/bin/bash {ROOT}/scripts/run_discord_artifact_maintenance.sh --signal-cache-only"
    if (state.get("Type") != "oneshot" or args is None or args.group(1) != expected
            or state.get("DropInPaths")):
        raise ValueError("native_target_not_the_unmodified_canonical_cache_only_owner")
    return state


def check_wire(*, previous=False, engineering=False):
    state = show(PATH_UNIT, ("ActiveState", "SubState", "Paths", "Triggers", "FragmentPath"))
    entries = re.findall(r"(\S+) \(([^)]+)\)", state.get("Paths", ""))
    watches = {path for path, _ in entries}
    expected = {str(ROOT / "artifacts/data_refresh/tw_public/completed_session/latest.json")} if previous else WATCHES
    if engineering:
        expected = expected | {str(MARKER)}
    if (state.get("ActiveState") != "active" or state.get("SubState") != "waiting"
            or state.get("Triggers") != CACHE_UNIT or watches != expected
            or any(kind != "PathChanged" for _, kind in entries)):
        raise ValueError("unexpected_native_path_watch_set_or_cache_only_owner")
    if not previous:
        template = (ROOT / "deploy/systemd/stockagent-discord-artifact-maintenance.path.in").read_text()
        if Path(state["FragmentPath"]).read_text() != template.replace("@REPO_ROOT@", str(ROOT)):
            raise ValueError("loaded_native_base_does_not_match_canonical_template")
    return state


def fingerprints():
    result = cache.verify()  # Model/CUDA calls are forbidden by the canonical verifier.
    rows = []
    for market in result["markets"]:
        cfg = cache.discord_bot._effective_market_config(cache.discord_bot._resolve_market(market["market"]))
        config = cache.load_config(cfg.config_path)
        rule_root = resolve_day_trade_rule_data_dir(
            cfg.day_trade_rule_data_dir, parquet_root=Path(config.data.parquet_root), repo_root=ROOT)
        if Path(rule_root).resolve() != RULE_ROOT.resolve():
            raise ValueError("current_market_rule_root_not_bound_to_native_watch_set")
        path = Path(market["summary_path"])
        payload = json.loads(path.read_bytes())
        weights = cache.discord_bot._summary_artifact_path(payload, "weights_path", path)
        rows.append({**market, "summary_sha256": sha(path), "weights_sha256": sha(weights)})
    return {"markets": rows, "cuda_initialized": result["cuda_initialized"],
            "model_forward_calls": result["model_forward_calls"],
            "provider_calls": result["provider_calls"], "trading_writes": result["trading_writes"]}


def require_same(before, after, message):
    if before != after:
        raise ValueError(message)


def await_terminal(control, deadline_seconds=40):
    deadline = time.monotonic() + deadline_seconds
    start = int(control.get("ExecMainStartTimestampMonotonic") or 0)
    observations = []
    while True:
        current = worker()
        observations.append(current)
        if (int(current.get("ExecMainStartTimestampMonotonic") or 0) > start
                and current.get("ActiveState") not in {"active", "activating", "deactivating"}):
            if current.get("Result") != "success" or current.get("ExecMainStatus") != "0":
                raise ValueError("native_cache_worker_failed")
            return current, observations
        if time.monotonic() >= deadline:
            # A timeout is not permission to restart the worker. Preserve its
            # actual handle/state and re-observe it via native systemd.
            raise TimeoutError(f"native_worker_observation_timeout_not_terminal: {current}")
        time.sleep(.2)


def run(args):
    if args.output.exists():
        raise FileExistsError("never overwrite previous event evidence")
    protected = {unit: identity(unit) for unit in (*PROTECTED, UNIT)}
    proofs = code_proofs()
    result = {"contract": "postclose_native_source_event_cpu_reuse_v1",
              "observed_at_utc": datetime.now(timezone.utc).isoformat(),
              "protected": protected, "code_sha256": proofs,
              "source_writes": False, "trading_writes": False,
              "broker_orders": 0, "real_new_publication_tested": False}
    try:
        if not all(value["ActiveState"] == "active" for value in protected.values()):
            raise ValueError("protected_owner_not_active")
        result["cache_only_owner"] = check_cache_only_owner()
        current = fingerprints()
        result["cache"] = current
        control = worker()
        if args.baseline:
            result.update(state="baseline", path=check_wire(engineering=args.engineering_watch), worker=control)
        else:
            previous = json.loads(args.control.read_bytes())
            require_same(previous["code_sha256"], proofs, "event_bridge_code_changed")
            require_same(previous["protected"], protected, "protected_owner_changed")
            require_same(previous["cache"], current, "source_signal_weights_or_forward_identity_changed")
            result["path"] = check_wire(engineering=args.notify)
            if args.notify:
                if control.get("ActiveState") != "inactive":
                    raise ValueError("cache_worker_already_live_do_not_duplicate")
                if not MARKER.is_file():
                    raise ValueError("engineering_marker_must_preexist_before_path_activation")
                atomic_write_json(MARKER, {"purpose": "engineering_notification_not_market_publication",
                                          "observed_at_utc": datetime.now(timezone.utc).isoformat()})
                terminal, observations = await_terminal(control)
                result.update(state="native_engineering_event_delivered", worker=terminal,
                              observations=observations, exact_cache_weights_parity=True)
            else:
                require_same(previous["worker"], control, "unexpected_idle_worker_activation")
                result.update(state="normal_watch_set_and_stable_terminal_worker_verified", worker=control)
        require_same(current, fingerprints(), "source_or_complete_cache_changed_during_verification")
        require_same(protected, {unit: identity(unit) for unit in protected}, "protected_owner_changed_during_probe")
        require_same(proofs, code_proofs(), "event_bridge_code_changed_during_probe")
    except Exception as error:
        result.update(state="failed_verification", error_type=type(error).__name__, error=str(error))
        raise
    finally:
        atomic_write_json(args.output, result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument("--baseline", action="store_true")
    modes.add_argument("--notify", action="store_true")
    modes.add_argument("--readback", action="store_true")
    parser.add_argument("--control", type=Path)
    parser.add_argument("--engineering-watch", action="store_true",
                        help="baseline only: require the explicitly installed temporary engineering watch")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not args.baseline and args.control is None:
        parser.error("notify/readback require a previous control receipt")
    if args.engineering_watch and not args.baseline:
        parser.error("engineering-watch is only for the current baseline")
    result = run(args)
    print(json.dumps({"state": result["state"], "output": str(args.output)}, ensure_ascii=False))


if __name__ == "__main__":
    main()

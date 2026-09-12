#!/usr/bin/env python3
"""Add a separately capitalized replay account without replacing any old mode.

The user must explicitly authorize account addition. This is not a bypass for
strategy replacement: an ID appearing anywhere in the live ledger is rejected.
Use the canonical replay validator and same-filesystem atomic exchange. Every
old mode and old ledger byte prefix is preserved, even when it has inventory.
The caller owns stopping/restarting the known live writer around --apply.
"""

from __future__ import annotations

import argparse
from contextlib import ExitStack
import copy
from datetime import date, datetime
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from scripts.promote_tw_day_trade_replay import (
    TAIPEI,
    _acquire_engine_lock,
    _atomic_json,
    _exchange_directories,
    _load_object,
    _promotion_input_hashes,
    _revalidate_margin_sources,
    _sha256,
    _validate_benchmarks,
    _validate_minute_curve_coverage,
    _validate_rebuild,
)
from scripts.rebuild_tw_day_trade_minute_curves import validate_existing_strategy_marks
from stockagent.live.market_config import load_market_config
from stockagent.live.tw_day_trade_simulation import (
    TwDayTradeSimulationEngine,
    minute_curve_write_lock,
)

LEDGERS = (
    "signals.jsonl",
    "orders.jsonl",
    "fills.jsonl",
    "marks.jsonl",
    "events.jsonl",
    "latency.jsonl",
)
PROJECTIONS = {"state.json", "status.json", "positions.json", "service_sync.json"}
# Query IPC and readiness observations have independent writers. They are not
# financial ledger evidence and must not be relocated as in-flight requests.
VOLATILE_OBSERVATIONS = {"quote_broker", "preopen_readiness.json"}


def _account_input_hashes(root: Path) -> dict[str, str]:
    return {
        str(path.relative_to(root)): _sha256(path)
        for path in sorted(root.rglob("*"))
        if path.relative_to(root).parts[0] not in VOLATILE_OBSERVATIONS
        and path.is_file()
        and not path.name.startswith(".")
    }


def _rows(path):
    if path.exists():
        with path.open() as stream:
            for line in stream:
                if line.strip():
                    yield json.loads(line)


def validate_new_identity(
    live: Path, candidate: Path, market: str
) -> tuple[dict, dict]:
    old = _load_object(live / "state.json")
    incoming = _load_object(candidate / "state.json")
    if set(incoming.get("modes", {})) != {market}:
        raise ValueError("candidate must contain exactly the one new account")
    if (
        not market
        or market in old.get("modes", {})
        or market in old.get("enabled_markets", [])
    ):
        raise ValueError("account addition may never replace an existing market")
    for name in LEDGERS:
        if any(row.get("market") == market for row in _rows(live / name)):
            raise ValueError(f"account ID already occurs in live {name}")
        for row in _rows(candidate / name):
            if row.get("market") not in (None, "", market):
                raise ValueError(f"foreign account in candidate {name}")
    for root in (live, candidate):
        if root.is_symlink() or any(path.is_symlink() for path in root.rglob("*")):
            raise ValueError("account import refuses symlink trees")
    return old, incoming


def _prefix_hash(path: Path, size: int) -> str:
    digest = hashlib.sha256()
    remaining = size
    with path.open("rb") as stream:
        while remaining:
            block = stream.read(min(8 * 1024 * 1024, remaining))
            if not block:
                raise ValueError("old ledger prefix was truncated")
            remaining -= len(block)
            digest.update(block)
    return digest.hexdigest()


def verify_completed_prefix(path: Path, digest: str, end: str) -> int:
    """A live ledger may append today's rows, but never alter accepted history."""
    observed = hashlib.sha256()
    matched = False
    prefix_bytes = 0
    with path.open("rb") as stream:
        for line in stream:
            if not matched:
                observed.update(line)
                prefix_bytes += len(line)
                matched = observed.hexdigest() == digest
            else:
                row = json.loads(line)
                day = str(row.get("session_date") or row.get("fill_at") or "")[:10]
                if not day or day <= end:
                    raise ValueError(
                        f"historical row appended after acceptance: {path.name}"
                    )
    if not matched:
        raise ValueError(f"accepted historical prefix changed: {path.name}")
    return prefix_bytes


def validate_composed_history(
    live: Path, candidate: Path, merged: Path, market: str
) -> dict:
    """Compose already independently verified accounts, then audit every minute.

    This never rewrites marks or fabricates a new replay of the old accounts.
    The old full-replay receipt retains its original scope; the account-addition
    receipt binds that scope and the separately validated new-account replay.
    """
    old = _load_object(live / "minute_curve_receipt.json")
    new = _load_object(candidate / "minute_curve_receipt.json")
    sessions = old["strategy"]["session_dates"]
    if not sessions or sessions != new["strategy"]["session_dates"]:
        raise ValueError("account addition requires identical completed session scope")
    for component in (old, new):
        if not (
            component.get("independent_carried_valuation_parity_passed") is True
            and component.get("independent_carried_valuation_parity_required") is True
            and component.get("carried_inventory_revalued_from_unchanged_executions")
            is True
            and component["strategy"].get("differing_original_equity_points") == 0
            and 0
            <= component["strategy"].get("maximum_original_equity_difference_twd", -1)
            <= 1e-6
            and component.get("linear_interpolation_used") is False
            and component.get("simulation_only") is True
            and component.get("production_order_possible") is False
            and component.get("coverage_after_fetch", {}).get("missing_pairs") == 0
        ):
            raise ValueError(
                "component does not have accepted independent minute parity"
            )
    prefix_proofs = {
        "marks": verify_completed_prefix(
            live / "marks.jsonl", old["outputs"]["marks"]["sha256"], sessions[-1]
        ),
        "fills": verify_completed_prefix(
            live / "fills.jsonl", old["unchanged_fills_sha256"], sessions[-1]
        ),
    }
    _, stats = validate_existing_strategy_marks(
        list(_rows(merged / "marks.jsonl")),
        start=date.fromisoformat(sessions[0]),
        end=date.fromisoformat(sessions[-1]),
    )
    expected = set(_load_object(merged / "state.json")["modes"])
    if set(stats["markets"]) != expected or stats["session_dates"] != sessions:
        raise ValueError("composed minute account/calendar mismatch")
    benchmarks = _validate_benchmarks(merged, completed_session_dates=sessions)
    archive = (
        merged / "account_receipts" / market / "previous_minute_curve_receipt.json"
    )
    shutil.copy2(live / "minute_curve_receipt.json", archive)
    receipt = copy.deepcopy(old)
    receipt.update(
        created_at=datetime.now(TAIPEI).isoformat(),
        composition_contract="disjoint_account_verified_history_prefix_composition_v1",
        independent_parity_evidence="unchanged independently revalued component histories; no new executions",
        components=[
            {
                "markets": old["strategy"]["markets"],
                "receipt": str(archive.relative_to(merged)),
                "sha256": _sha256(archive),
                "verified_prefix_bytes": prefix_proofs,
            },
            {
                "markets": [market],
                "receipt": f"account_receipts/{market}/minute_curve_receipt.json",
                "sha256": _sha256(candidate / "minute_curve_receipt.json"),
            },
        ],
        unchanged_fills_sha256=_sha256(merged / "fills.jsonl"),
        source_carried_accounting_signature=None,
        source_ledger_hashes={
            name: _sha256(merged / name)
            for name in ("marks.jsonl", "fills.jsonl", "orders.jsonl")
        },
        strategy={
            **stats,
            "differing_original_equity_points": 0,
            "maximum_original_equity_difference_twd": max(
                x["strategy"]["maximum_original_equity_difference_twd"]
                for x in (old, new)
            ),
        },
        outputs={
            name: {"path": str(live / filename), "sha256": _sha256(merged / filename)}
            for name, filename in (
                ("marks", "marks.jsonl"),
                ("benchmark_history", "benchmark_history.json"),
            )
        },
    )
    receipt["outputs"]["marks"]["rows"] = sum(1 for _ in _rows(merged / "marks.jsonl"))
    for key in (
        "local_minute_sources",
        "local_minute_cache_sources",
        "official_no_trade_carried_pairs",
    ):
        receipt[key] = list(
            {
                json.dumps(row, sort_keys=True): row
                for component in (old, new)
                for row in component.get(key, [])
            }.values()
        )
    _atomic_json(merged / "minute_curve_receipt.json", receipt)
    failures = []
    validation = _validate_minute_curve_coverage(
        merged,
        completed_session_dates=sessions,
        expected_markets=expected,
        failures=failures,
        require_carried_parity=True,
    )
    if failures:
        raise ValueError("; ".join(failures))
    return {
        "minute_curves": validation,
        "benchmarks": benchmarks,
        "previous_minute_receipt_archive": str(archive.relative_to(merged)),
    }


def _sync_tree(root: Path) -> None:
    for path in root.rglob("*"):
        if path.is_file():
            with path.open("rb") as handle:
                os.fsync(handle.fileno())
    for path in [*reversed(list(root.rglob("*"))), root]:
        if path.is_dir():
            descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)


def compose_account(live: Path, candidate: Path, merged: Path, market: str) -> dict:
    """Called only under writer locks; no source state is mutated."""
    old, incoming = validate_new_identity(live, candidate, market)
    if merged.exists():
        raise ValueError("merged destination must be new")
    before = _account_input_hashes(live)
    shutil.copytree(
        live,
        merged,
        ignore=lambda directory, names: (
            {"quote_broker"} if Path(directory) == live else set()
        ),
    )
    appended = {}
    for name in LEDGERS:
        source, destination = candidate / name, merged / name
        if not source.exists():
            continue
        count = 0
        with destination.open("ab") as stream:
            # Preserve the old prefix, including its whitespace/row ordering.
            if destination.stat().st_size:
                with destination.open("rb") as check:
                    check.seek(-1, os.SEEK_END)
                    if check.read(1) != b"\n":
                        raise ValueError("live JSONL has an incomplete terminal row")
            for row in _rows(source):
                if row.get("market") != market:
                    continue
                stream.write(
                    (
                        json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n"
                    ).encode()
                )
                count += 1
            stream.flush()
            os.fsync(stream.fileno())
        appended[name] = count
    history = candidate / "position_history"
    if history.exists():
        for path in history.rglob("*.json"):
            payload = _load_object(path)
            if payload.get("market") != market:
                raise ValueError("foreign or unidentified position history")
            relative = path.relative_to(candidate)
            destination = merged / relative
            if destination.exists():
                raise ValueError("position history collision")
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, destination)
    state = copy.deepcopy(old)
    mode = copy.deepcopy(incoming["modes"][market])
    mode["executed_positions_path"] = str(live / "positions.json")
    mode["account_origin"] = "user_authorized_isolated_replay_account_v1"
    state.setdefault("modes", {})[market] = mode
    state["enabled_markets"] = [
        *old.get("enabled_markets", sorted(old["modes"])),
        market,
    ]
    # Reuse projection/persistence, not an alternate cash or fill implementation.
    engine = TwDayTradeSimulationEngine(merged)
    engine.state = state
    engine._persist(datetime.now(TAIPEI))
    after = _load_object(merged / "state.json")
    for old_market, old_mode in old["modes"].items():
        if after["modes"][old_market] != old_mode:
            raise ValueError(f"existing account changed: {old_market}")
    for key in ("benchmarks", "minute_liquidity"):
        if after.get(key) != old.get(key):
            raise ValueError(f"existing shared observation changed: {key}")
    preserved = {}
    for relative, digest in before.items():
        if relative in PROJECTIONS:
            continue
        size = (live / relative).stat().st_size
        actual = (
            _prefix_hash(merged / relative, size)
            if relative in LEDGERS
            else _sha256(merged / relative)
        )
        if actual != digest:
            raise ValueError(f"existing file changed: {relative}")
        preserved[relative] = {"bytes": size, "sha256": digest}
    after_inputs = _account_input_hashes(live)
    if before != after_inputs:
        changed = sorted(
            name
            for name in before.keys() | after_inputs.keys()
            if before.get(name) != after_inputs.get(name)
        )
        raise ValueError(f"live source changed while composing account: {changed}")
    proofs = merged / "account_receipts" / market
    proofs.mkdir(parents=True, exist_ok=False)
    for name in (
        "rebuild_receipt.json",
        "minute_curve_receipt.json",
        "benchmark_history.json",
    ):
        if (candidate / name).is_file():
            shutil.copy2(candidate / name, proofs / name)
    result = {
        "schema_version": 1,
        "account": market,
        "accounting_scope": "independent_market_capital_positions_and_pnl",
        "added_initial_capital_twd": mode["initial_capital_twd"],
        "source_candidate": str(candidate),
        "preserved_markets": sorted(old["modes"]),
        "preserved_old_files": preserved,
        "old_modes_unchanged": True,
        "old_ledger_prefixes_unchanged": True,
        "volatile_observations_excluded_from_financial_snapshot": sorted(
            VOLATILE_OBSERVATIONS
        ),
        "quote_broker_inflight_requests": "not imported; clients use their existing bounded retry after writer restart",
        "appended_rows": appended,
        "simulation_only": True,
        "production_order_possible": False,
        "aggregate_historical_revalidation_required": True,
    }
    _atomic_json(merged / "account_addition_receipt.json", result)
    return result


def main(*, before_exchange=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--live-dir", type=Path, required=True)
    parser.add_argument("--candidate-dir", type=Path, required=True)
    parser.add_argument("--market-config", type=Path, required=True)
    parser.add_argument("--expected-checkpoint-sha256", required=True)
    parser.add_argument("--receipt-path", type=Path)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    live, candidate = (
        args.live_dir.resolve(strict=True),
        args.candidate_dir.resolve(strict=True),
    )
    if (
        live == candidate
        or live in candidate.parents
        or candidate in live.parents
        or live.stat().st_dev != candidate.stat().st_dev
    ):
        raise ValueError("distinct same-filesystem account directories required")
    cfg = load_market_config(args.market_config)
    if _sha256(Path(cfg.checkpoint_path)) != args.expected_checkpoint_sha256:
        raise ValueError("checkpoint identity differs from the requested model")
    old, incoming = validate_new_identity(live, candidate, cfg.market)
    mode = incoming["modes"][cfg.market]
    if (
        float(mode["initial_capital_twd"]) != float(cfg.initial_capital)
        or Path(mode["checkpoint_path"]).resolve()
        != Path(cfg.checkpoint_path).resolve()
    ):
        raise ValueError("candidate funding/checkpoint does not match configuration")
    acceptance = _validate_rebuild(
        candidate, expected_markets={cfg.market}, allow_margin_carry=True
    )
    plan = {
        "action": "add_isolated_account_not_replace",
        "market": cfg.market,
        "preserved_markets": sorted(old["modes"]),
        "acceptance": acceptance,
    }
    if not args.apply:
        if args.receipt_path:
            _atomic_json(args.receipt_path, plan)
        print(json.dumps(plan, ensure_ascii=False, sort_keys=True))
        return
    if before_exchange is not None:
        before_exchange()
    with ExitStack() as stack:
        stack.enter_context(_acquire_engine_lock(live))
        stack.enter_context(_acquire_engine_lock(candidate))
        with minute_curve_write_lock(live), minute_curve_write_lock(candidate):
            if acceptance["promotion_input_hashes"] != _promotion_input_hashes(
                candidate
            ):
                raise ValueError("candidate changed after acceptance")
            # Keep the composed successor for audit; after exchange this same
            # path contains the exact old ledger as a recoverable rollback.
            container = Path(
                tempfile.mkdtemp(prefix="account-addition-", dir=live.parent)
            )
            merged = container / "ledger"
            receipt = compose_account(live, candidate, merged, cfg.market)
            receipt["aggregate_history"] = validate_composed_history(
                live, candidate, merged, cfg.market
            )
            receipt["aggregate_historical_revalidation_required"] = False
            receipt["refreshed_projection_receipts"] = ["minute_curve_receipt.json"]
            receipt["preserved_old_files"].pop("minute_curve_receipt.json", None)
            receipt.update(
                acceptance=acceptance,
                rollback_directory=str(merged),
                live_directory=str(live),
            )
            _atomic_json(merged / "account_addition_receipt.json", receipt)
            _sync_tree(merged)
            _revalidate_margin_sources(candidate, acceptance)
            _exchange_directories(live, merged)
            for directory in (live.parent, merged.parent):
                descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
    print(
        json.dumps(
            {
                "status": "account_added",
                "market": cfg.market,
                "rollback_directory": str(merged),
                "old_modes_unchanged": True,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()

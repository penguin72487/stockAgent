#!/usr/bin/env python3
"""Prepare a conservative, private source scope from original signal revisions.

This is source discovery only: no paper orders, ledger writes, or publication.
Every nonzero target in a selected revision is included when an official open
is available. A symbol is not dropped merely because an estimated account NAV
would have made it smaller than one lot.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime
import hashlib
import json
import math
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import polars as pl

from scripts.plan_tw_day_trade_historical_revisions import _write_new_plan
from scripts.rebuild_tw_day_trade_open_price_replay import (
    DEFAULT_TPEX_DAILY_OHLCV_PATH,
    DEFAULT_TWSE_DAILY_OHLCV_PATH,
    _official_open_map,
)
from scripts import repair_tw_day_trade_flat_opening as revision_owner
from scripts.repair_tw_day_trade_flat_opening import _published_revision_inventory, _spec
from stockagent.data_sync import workspace_volume
from stockagent.live.tw_day_trade_simulation import TAIPEI


def _source_generation(path: Path) -> tuple[int, int, int, int, int]:
    stat = path.stat()
    return stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns


def _nonzero_targets(versions: list[dict]) -> set[str]:
    """Read every selected target file without applying a capital/lot shortcut."""
    selected = [row for row in versions if row.get("effective_entry_at")]
    if not selected:
        raise ValueError("selected revisions are required")
    paths = [Path(row["weights_path"]).resolve(strict=True) for row in selected]
    generations = {path: _source_generation(path) for path in paths}
    symbols: set[str] = set()
    for path in paths:
        frame = pl.read_parquet(path, columns=["symbol", "target_weight"])
        seen: set[str] = set()
        for symbol, weight in frame.iter_rows():
            if (not isinstance(symbol, str) or not symbol or symbol in seen
                    or isinstance(weight, bool) or not isinstance(weight, (int, float))
                    or not math.isfinite(float(weight))):
                raise ValueError(f"invalid or duplicate source target: {path}")
            seen.add(symbol)
            if weight != 0:
                symbols.add(symbol)
    if any(_source_generation(path) != before for path, before in generations.items()):
        raise RuntimeError("a target source changed during scope preparation")
    return symbols


def prepare(*, revision_plan: Path, markets_dir: Path, output_root: Path) -> dict:
    pins = (Path(__file__).resolve(), Path(revision_owner.__file__).resolve(), revision_plan)
    pin_before = {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in pins}
    source = json.loads(revision_plan.read_text(encoding="utf-8"))
    session = date.fromisoformat(source["session_date"])
    if (session >= datetime.now(TAIPEI).date()
            or source.get("status") != "sources_pinned_not_replayed"
            or source.get("simulation_only") is not True
            or source.get("production_order_possible") is not False
            or source.get("ledger_modified") is not False):
        raise ValueError("a completed, source-only historical plan is required")
    plans = source.get("source_plans")
    if not isinstance(plans, list) or not plans:
        raise ValueError("source plans are required")
    markets = [p.get("market") for p in plans]
    if (any(not isinstance(m, str) or not m or Path(m).name != m for m in markets)
            or len(markets) != len(set(markets))):
        raise ValueError("unique safe market identifiers are required")

    # Fail before the expensive source scan when the destination cannot be
    # admitted. A second check is still required immediately before writing.
    workspace = workspace_volume.configuration()
    root = output_root.resolve()
    build_root = Path(workspace["mount_point"]) / "build"
    if not root.is_relative_to(build_root) or root == build_root:
        raise ValueError("private scope must use a new E build subdirectory")
    if root.exists():
        raise FileExistsError("private scope destination already exists")
    workspace_volume.admit(required_bytes=128 * 1024**2, c=workspace)

    current, specs, selected = [], [], {}
    for prior in plans:
        spec, _ = _spec(prior["market"], markets_dir, True)
        verified = _published_revision_inventory(spec, session)
        if verified["plan_sha256"] != prior.get("plan_sha256"):
            raise RuntimeError(f"published revisions changed: {spec.market}")
        version = verified["versions"][0]
        summary_path, weights_path = Path(version["summary_path"]), Path(version["weights_path"])
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        selected[spec.market] = (
            datetime.fromisoformat(version["publication_upper_bound"]), summary_path,
            weights_path, summary, pl.read_parquet(weights_path).to_dicts(),
        )
        specs.append(spec)
        current.append(verified)
    # A later selected revision can introduce a symbol absent from the first
    # publication. The official lookup must cover the whole selected union.
    targets_by_market = {item["market"]: _nonzero_targets(item["versions"]) for item in current}
    for market, entry in selected.items():
        first_rows = entry[4]
        first_symbols = {str(row.get("symbol") or "") for row in first_rows}
        selected[market] = (*entry[:4], [*first_rows, *(
            {"symbol": symbol} for symbol in sorted(targets_by_market[market] - first_symbols)
        )])
    opens, source_counts, no_trade = _official_open_map(
        specs, selected, session, twse_daily_ohlcv_path=DEFAULT_TWSE_DAILY_OHLCV_PATH,
        tpex_daily_ohlcv_path=DEFAULT_TPEX_DAILY_OHLCV_PATH,
    )
    inventories = []
    for item in current:
        targets = targets_by_market[item["market"]]
        unavailable = targets - opens.keys()
        inventories.append({
            "plan_sha256": item["plan_sha256"], "session_date": session.isoformat(),
            "scope_contract": "selected_revision_all_nonzero_targets_with_official_open_v1",
            "scope_is_not_execution_eligibility": True,
            "official_open_source_counts": source_counts,
            "nonzero_target_count": len(targets),
            "minute_symbols": sorted(targets & opens.keys()),
            "official_open_unavailable_symbols": sorted(unavailable),
            "official_no_trade_symbols": sorted(unavailable & no_trade),
            "unresolved_official_open_symbols": sorted(unavailable - no_trade),
            "scope_implementation_sha256": pin_before[str(Path(__file__).resolve())],
            "revision_owner_implementation_sha256": pin_before[str(Path(revision_owner.__file__).resolve())],
            "parent_revision_plan_sha256": pin_before[str(revision_plan)],
            "simulation_only": True, "production_order_possible": False,
            "ledger_modified": False,
        })

    # Detect revisions changed while opening data and target files were read.
    for spec, prior in zip(specs, current, strict=True):
        if _published_revision_inventory(spec, session)["plan_sha256"] != prior["plan_sha256"]:
            raise RuntimeError(f"published revisions changed during preparation: {spec.market}")
    if any(hashlib.sha256(path.read_bytes()).hexdigest() != pin_before[str(path)] for path in pins):
        raise RuntimeError("scope implementation or parent plan changed during preparation")
    workspace_volume.admit(required_bytes=128 * 1024**2, c=workspace)
    root.mkdir(mode=0o700, parents=True, exist_ok=False)
    for item, inventory in zip(current, inventories, strict=True):
        directory = root / item["market"]
        directory.mkdir(mode=0o700)
        _write_new_plan(directory / "published_revision_plan.json", item)
        _write_new_plan(directory / "source_inventory.json", inventory)
    result = {
        "contract": "historical_signal_source_scope_v1",
        "status": "source_scope_only_not_replayed", "session_date": session.isoformat(),
        "markets": [{"market": item["market"], "nonzero_target_count": inventory["nonzero_target_count"],
                     "minute_symbol_count": len(inventory["minute_symbols"]),
                     "official_open_unavailable_symbols": inventory["official_open_unavailable_symbols"],
                     "unresolved_official_open_symbols": inventory["unresolved_official_open_symbols"]}
                    for item, inventory in zip(current, inventories, strict=True)],
        "simulation_only": True, "production_order_possible": False,
        "ledger_modified": False, "broker_orders_created": 0,
    }
    _write_new_plan(root / "source_scope_receipt.json", result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--revision-plan", type=Path, required=True)
    parser.add_argument("--markets-dir", type=Path, default=ROOT / "services/discord_bot/markets")
    parser.add_argument("--output-root", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(prepare(revision_plan=args.revision_plan.resolve(strict=True),
                             markets_dir=args.markets_dir.resolve(strict=True),
                             output_root=args.output_root), ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()

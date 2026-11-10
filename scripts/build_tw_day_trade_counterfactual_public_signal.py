#!/usr/bin/env python3
"""Project one independently checked paper-research signal without a fill claim."""
from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import date, datetime
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sys
from zoneinfo import ZoneInfo

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stockagent.live.market_config import load_market_config
from stockagent.live.tw_day_trade_completed_replay import verify_completed_open_sources
from stockagent.live.tw_day_trade_research_candidates import research_session_is_completed

CONTRACT = "public_source_checked_counterfactual_open_signal_v1"
TAIPEI = ZoneInfo("Asia/Taipei")


def _sha(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def _object(path: Path) -> dict:
    result = json.loads(path.read_bytes())
    if not isinstance(result, dict):
        raise ValueError("expected an object")
    return result


def _retained_source_proof(raw_root: Path, session: date, proof: dict) -> dict:
    """Accept the same source bytes at a retained immutable close-evidence path.

    Daily aliases may be refreshed after generation. Never rewrite the original
    signal receipt or accept a changed digest as the original observation.
    """
    retained = deepcopy(proof)
    sources = retained.get("official_sources")
    if not isinstance(sources, dict) or set(sources) != {"twse", "tpex"}:
        raise ValueError("same-day official source membership differs")
    for exchange in ("twse", "tpex"):
        source = sources[exchange]
        if not isinstance(source, dict):
            raise ValueError("same-day official source is invalid")
        digest = str(source.get("sha256") or "")
        if not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise ValueError("same-day official source digest is invalid")
        original = Path(str(source.get("path") or ""))
        if not original.is_absolute():
            raise ValueError("same-day official source path is invalid")
        try:
            original_valid = _sha(original.resolve(strict=True)) == digest
        except OSError:
            original_valid = False
        if not original_valid:
            replacement = raw_root / ".close_evidence" / digest / f"{exchange}_daily_ohlcv" / f"{session}.json"
            if not replacement.is_file() or _sha(replacement) != digest:
                raise ValueError("original official source and retained bytes are unavailable")
            source["path"] = str(replacement.resolve(strict=True))
    return retained


def build(*, candidate_config: Path, preflight: Path, backfill_receipt: Path,
          checkpoint_sha256: str, now: datetime) -> dict:
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("clock must be timezone aware")
    now = now.astimezone(TAIPEI)
    config = load_market_config(candidate_config.resolve(strict=True))
    checkpoint = Path(str(config.checkpoint_path or "")).resolve(strict=True)
    if (config.enabled or not config.day_trade_simulation_enabled
            or checkpoint.name != "checkpoint_best.pt" or _sha(checkpoint) != checkpoint_sha256):
        raise ValueError("candidate checkpoint or disabled scope changed")
    preflight_path = preflight.resolve(strict=True)
    receipt_path = backfill_receipt.resolve(strict=True)
    preflight_sha, receipt_sha = _sha(preflight_path), _sha(receipt_path)
    admission = _object(preflight_path)
    artifacts = admission.get("artifacts")
    if (admission.get("contract") != "exact_selected_signal_preflight_v1"
            or admission.get("status") != "accepted_sources_only"
            or admission.get("simulation_only") is not True
            or admission.get("production_order_possible") is not False
            or admission.get("execution_or_minute_coverage_accepted") is not False
            or admission.get("expected_signals") != 1 or admission.get("accepted_signals") != 1
            or admission.get("failures") != [] or not isinstance(artifacts, list) or len(artifacts) != 1):
        raise ValueError("single-session signal source preflight is incomplete")
    item = artifacts[0]
    session = date.fromisoformat(str(item.get("session_date") or ""))
    if (item.get("market") != config.market or not research_session_is_completed(session, now)
            or session != now.date()):
        raise ValueError("signal is not the selected completed current session")
    paths = {name: Path(str(item.get(f"{name}_path") or "")).resolve(strict=True)
             for name in ("summary", "weights", "open_input")}
    live_roots = ((ROOT / "artifacts/live/tw_day_trade_simulation").resolve(),
                  (ROOT / "artifacts/live_signals").resolve())
    if any(path == live or path.is_relative_to(live) for path in paths.values() for live in live_roots):
        raise ValueError("counterfactual artifacts overlap a live namespace")
    if any(_sha(paths[name]) != item.get(f"{name}_sha256") for name in paths):
        raise ValueError("signal preflight source changed")
    summary = _object(paths["summary"])
    signal_id = f"{config.market}-counterfactual-official-open-{session}"
    generated = datetime.fromisoformat(str(summary.get("generated_at") or ""))
    if (generated.tzinfo is None or generated.utcoffset() is None or generated > now
            or not research_session_is_completed(session, generated)
            or summary.get("market") != config.market or summary.get("signal_id") != signal_id
            or item.get("signal_id") != signal_id
            or summary.get("simulation_only") is not True or summary.get("production_order_possible") is not False
            or summary.get("counterfactual_signal_regeneration") is not True
            or summary.get("live_session_open_feature_applied") is not True
            or summary.get("replay_effective_signal_at") != f"{session}T09:00:00+08:00"
            or Path(str(summary.get("checkpoint_path") or "")).resolve(strict=True) != checkpoint):
        raise ValueError("signal identity, generation clock or research disclosure differs")
    artifact_proof = summary.get("counterfactual_artifact_proofs") or {}
    recipe = artifact_proof.get("recipe_identity") or {}
    if (artifact_proof.get("weights_sha256") != item["weights_sha256"]
            or recipe.get("checkpoint_sha256") != checkpoint_sha256):
        raise ValueError("signal does not bind the selected checkpoint and weights")
    receipt = _object(receipt_path)
    sessions = receipt.get("sessions")
    if (receipt.get("status") != "source_hashes_unchanged_candidate_not_yet_accepted"
            or receipt.get("simulation_only") is not True or receipt.get("production_order_possible") is not False
            or receipt.get("market") != config.market or not isinstance(sessions, list) or len(sessions) != 1
            or sessions[0].get("session_date") != str(session)
            or sessions[0].get("summary_sha256") != item["summary_sha256"]
            or sessions[0].get("signal_id") != signal_id):
        raise ValueError("backfill completion receipt differs")
    provenance = summary.get("counterfactual_open_provenance") or {}
    proof = provenance.get("completed_same_day_source_evidence")
    if not isinstance(proof, dict) or not config.day_trade_rule_data_dir:
        raise ValueError("same-day official source proof is missing")
    raw_root = Path(config.day_trade_rule_data_dir).resolve(strict=True) / "raw"
    retained_proof = _retained_source_proof(raw_root, session, proof)
    verify_completed_open_sources(session=session, observed_at=now, prices_csv=paths["open_input"],
        raw_root=raw_root, expected=retained_proof)
    frame = pl.read_parquet(paths["weights"])
    required = {"symbol", "name", "target_weight", "tradable", "can_buy", "can_sell"}
    if not required.issubset(frame.columns) or frame.height > 10_000:
        raise ValueError("signal weight schema or scope differs")
    rows, seen = [], set()
    for item_row in frame.select(*sorted(required)).iter_rows(named=True):
        symbol, name = str(item_row["symbol"] or ""), str(item_row["name"] or "")
        weight = item_row["target_weight"]
        if (not re.fullmatch(r"[A-Z0-9]{1,12}", symbol) or symbol in seen or len(name) > 80
                or type(weight) not in (int, float) or not math.isfinite(weight) or abs(weight) > 1
                or any(type(item_row[key]) is not bool for key in ("tradable", "can_buy", "can_sell"))):
            raise ValueError("signal contains an invalid symbol or weight")
        seen.add(symbol)
        if weight:
            rows.append({"symbol": symbol, "name": name, "target_weight_pct": weight * 100,
                         "tradable": item_row["tradable"], "can_buy": item_row["can_buy"],
                         "can_sell": item_row["can_sell"]})
    rows.sort(key=lambda row: (-abs(row["target_weight_pct"]), row["symbol"]))
    verify_completed_open_sources(session=session, observed_at=now, prices_csv=paths["open_input"],
        raw_root=raw_root, expected=retained_proof)
    if (any(_sha(paths[name]) != item.get(f"{name}_sha256") for name in paths)
            or _sha(checkpoint) != checkpoint_sha256 or _sha(preflight_path) != preflight_sha
            or _sha(receipt_path) != receipt_sha):
        raise ValueError("signal or checkpoint changed during projection")
    return {"contract": CONTRACT, "market": config.market, "session_date": str(session),
        "generated_at": generated.isoformat(), "published_at": now.isoformat(),
        "checkpoint_sha256": checkpoint_sha256,
        "checkpoint_fingerprint": summary.get("checkpoint_fingerprint"),
        "simulation_only": True, "counterfactual": True, "live_signal": False,
        "paper_execution_enabled": False, "broker_fill": False, "exchange_fill": False,
        "execution_and_minute_coverage_verified": False, "signal_count": len(rows), "signals": rows}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("candidate-config", "preflight", "backfill-receipt", "output"):
        parser.add_argument(f"--{name}", type=Path, required=True)
    parser.add_argument("--checkpoint-sha256", required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    allowed = (ROOT / "artifacts/operations/public_research").resolve(strict=True)
    if not output.is_relative_to(allowed) or output.exists():
        raise ValueError("public projection must use a new file in its research namespace")
    result = build(candidate_config=args.candidate_config, preflight=args.preflight,
        backfill_receipt=args.backfill_receipt, checkpoint_sha256=args.checkpoint_sha256,
        now=datetime.now(TAIPEI))
    encoded = (json.dumps(result, ensure_ascii=False, allow_nan=False, separators=(",", ":")) + "\n").encode()
    fd = os.open(output, os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_NOFOLLOW", 0), 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(encoded)
        stream.flush()
        os.fsync(stream.fileno())
    print(json.dumps({"path": str(output), "sha256": hashlib.sha256(encoded).hexdigest(),
                      "signals": result["signal_count"], "live_signal": False}, ensure_ascii=False))


if __name__ == "__main__":
    main()

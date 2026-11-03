#!/usr/bin/env python3
"""Read back finite holiday-adjustment requests owned by the TEJ scheduler.

This observer never sends a provider query, alters its queue, or admits a
financial value.  In particular, a next-session query is only a control: its
date never replaces the effective date of the exchange announcement.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime, timedelta
import json
from pathlib import Path
import sqlite3
import sys
import time

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.provider_priority import inspect
from scripts.audit_tw_futures_corporate_vendor import _compare, _inside
from scripts.audit_tw_futures_holiday_generation_terms import verify_originals

TABLE = "117392c87504f94e71a04d01"
FIELDS = ("Future Code", "Adjusted Date", "Shares per Contract", "Cash Dividends per Unit")


def validate_scope(row: dict, entry: dict) -> None:
    """Require the exact previously registered native task, never a broad guess."""
    if (row.get("task_id") != entry["task_id"] or row.get("table_id") != TABLE
            or row.get("kind") != "download" or row.get("scope_contract") != "editable_source_scope_v1"):
        raise ValueError("native task identity or source scope changed")
    request = json.loads(row["request_json"])
    symbol, day = entry["symbol"], entry["query_date"]
    companies = request.get("company_labels")
    if (request.get("action") != "download" or not isinstance(companies, list)
            or len(companies) != 1 or not str(companies[0]).startswith(symbol + "==>")
            or request.get("date_labels") != [day.replace("-", "")]
            or request.get("start") != day or request.get("end") != day):
        raise ValueError("native task is not the declared exact contract and date")


def compare_observation(rows: list[dict], raw: dict, entry: dict, term: dict) -> dict:
    """Compare the native cell grid and parquet without replacing source dates."""
    symbol, day = entry["symbol"], entry["query_date"]
    matches = [r for r in rows if r.get("_query_symbol") == symbol and r.get("_query_period") == day]
    if not matches:
        if rows:
            raise ValueError("nonempty native source does not match the requested cell")
        return {"state": "no_matching_observation", "official_effective_date": term["effective_date"],
                "query_date": day, "absence_establishes_no_event": False}
    if len(matches) != 1:
        raise ValueError("ambiguous native observation for the requested cell")
    row = matches[0]
    cells = raw.get("cells")
    if not isinstance(cells, list) or not cells or not isinstance(cells[0], list):
        raise ValueError("TEJ raw preview has no cell grid")
    headers = cells[0]
    if len(headers) != len(set(headers)) or not set(FIELDS).issubset(headers):
        raise ValueError("TEJ raw preview headers are missing or ambiguous")
    raw_matches = []
    for values in cells[1:]:
        if not isinstance(values, list) or len(values) != len(headers):
            raise ValueError("TEJ raw preview row width differs")
        original = dict(zip(headers, values, strict=True))
        if all(original[name] == row[name] for name in ("Future Code", "Adjusted Date")):
            raw_matches.append(original)
    if (len(raw_matches) != 1 or not str(row["Future Code"]).startswith(symbol + " ")
            or any(raw_matches[0][name] != row[name] for name in FIELDS)):
        raise ValueError("TEJ raw preview and parquet identity or values differ")
    native_day = str(row["Adjusted Date"]).replace("/", "-")
    official_day = str(term["effective_date"])
    cash = term["equity_cash_credit_twd"] if term["has_equity_credit_fields"] else None
    return {"state": "source_cell_compared", "query_date": day,
            "vendor_adjusted_date": native_day, "official_effective_date": official_day,
            "same_event_date": native_day == official_day,
            "control_only": day != official_day,
            "shares_per_contract": _compare(term["contract_multiplier"], row["Shares per Contract"]),
            "cash_credit_per_contract_twd": _compare(cash, row["Cash Dividends per Unit"]),
            "raw_cells_match_parquet": True,
            "source_units_and_account_transition_verified": False}


def read_completed(root: Path, entry: dict, term: dict, task: dict, item: dict) -> dict:
    """Read an owner receipt and recheck exact source hashes before and after."""
    validate_scope(task, entry)
    receipt = _inside(root, "receipts/" + entry["task_id"] + ".json")
    if not receipt.is_file():
        raise ValueError("complete native task has no receipt")
    receipt_sha = sha256_file(receipt)
    doc = json.loads(receipt.read_text())
    if (doc.get("task_id") != entry["task_id"] or doc.get("table_id") != TABLE
            or doc.get("provider") != "tej_smart_wizard" or task.get("state") != "complete"
            or type(doc.get("exported_rows")) is not int or item.get("rows") != doc["exported_rows"]):
        raise ValueError("complete source receipt identity or row count differs")
    pins = {str(receipt): receipt_sha}
    for kind in ("raw", "parquet"):
        path = _inside(root, doc[kind + "_path"])
        digest = sha256_file(path)
        if digest != doc[kind + "_sha256"]:
            raise ValueError("native " + kind + " source digest differs")
        pins[str(path)] = digest
    data = pl.read_parquet(_inside(root, doc["parquet_path"]))
    if data.height != doc["exported_rows"]:
        raise ValueError("native parquet and receipt row counts differ")
    result = compare_observation(data.to_dicts(), json.loads(_inside(root, doc["raw_path"]).read_text()),
                                 entry, term)
    if any(sha256_file(Path(path)) != digest for path, digest in pins.items()):
        raise ValueError("source changed while being read")
    return {**result, "task_id": entry["task_id"], "request_id": entry["request_id"],
            "receipt_path": str(receipt), "receipt_sha256": receipt_sha,
            "raw_sha256": doc["raw_sha256"], "parquet_sha256": doc["parquet_sha256"],
            "exported_rows": data.height, "source_outcome": doc.get("source_outcome"),
            "financial_or_training_adopted": False}


def load_contract(spec_path: Path) -> tuple[dict, dict, dict[str, dict], dict[str, str]]:
    spec_path = spec_path.resolve(strict=True)
    if not spec_path.is_relative_to(ROOT / "artifacts/markets"):
        raise ValueError("spec must be a formal market artifact")
    spec = json.loads(spec_path.read_text())
    if spec.get("contract") != "tw_futures_holiday_tej_observer_v1":
        raise ValueError("unsupported observer specification")
    report = _inside(ROOT, spec["candidate_report"])
    if sha256_file(report) != spec["candidate_report_sha256"]:
        raise ValueError("candidate report changed")
    prior = json.loads(report.read_text())
    if prior.get("contract") != "tw_futures_holiday_generation_terms_source_audit_v8":
        raise ValueError("unexpected official terms audit")
    entries = spec.get("requests")
    if not isinstance(entries, list) or not entries or len({x["task_id"] for x in entries}) != len(entries):
        raise ValueError("missing or duplicate requests")
    selected = {}
    for entry in entries:
        rows = [r for r in prior["results"] if r.get("status") == "unique_candidate_term"
                and r.get("pre_transfer_target_candidate") + r.get("contract") == entry["symbol"]]
        if len(rows) != 1 or rows[0]["term"]["effective_date"] != entry["official_effective_date"]:
            raise ValueError("official candidate is missing or ambiguous")
        selected[entry["task_id"]] = rows[0]
    originals = verify_originals(ROOT, list(selected.values()))
    pinned_paths = (spec_path, report, Path(__file__).resolve(), ROOT / "downloader/provider_priority.py",
                    ROOT / "scripts/audit_tw_futures_corporate_vendor.py",
                    ROOT / "scripts/audit_tw_futures_holiday_generation_terms.py")
    pins = {str(p): sha256_file(p) for p in pinned_paths}
    for value in originals.values():
        path = _inside(ROOT, value["path"])
        if sha256_file(path) != value["raw_sha256"]:
            raise ValueError("official original changed after independent verification")
        pins[str(path)] = value["raw_sha256"]
    return spec, prior, selected, pins


def observe(spec: dict, selected: dict[str, dict], pins: dict[str, str], output: Path) -> dict:
    if any(sha256_file(Path(p)) != h for p, h in pins.items()):
        raise ValueError("observer specification or implementation changed")
    root = ROOT / "data_tej"
    db = root / "queue.sqlite3"
    result_items = []
    with sqlite3.connect(f"{db.as_uri()}?mode=ro", uri=True, timeout=5) as con:
        con.row_factory = sqlite3.Row
        for entry in spec["requests"]:
            status = inspect(root, entry["request_id"])
            if status.get("provider") != "tej" or status.get("request_id") != entry["request_id"]:
                raise ValueError("priority request identity differs")
            items = [i for i in status.get("items", []) if i.get("key") == entry["task_id"]]
            row = con.execute("SELECT * FROM tasks WHERE task_id=?", (entry["task_id"],)).fetchone()
            if len(items) != 1 or row is None:
                raise ValueError("priority item or native task is missing")
            task = dict(row)
            validate_scope(task, entry)
            item = items[0]
            if item["state"] != task["state"]:
                # The owner may finish between the two read-only snapshots.
                # Reobserve rather than interpret either half as completion.
                result_items.append({"task_id": entry["task_id"], "symbol": entry["symbol"],
                                     "state": "observing_transition",
                                     "priority_active": status["priority_active"]})
                continue
            observed = {"task_id": entry["task_id"], "symbol": entry["symbol"],
                        "state": item["state"], "priority_active": status["priority_active"]}
            if item["state"] == "complete":
                detail = read_completed(root, entry, selected[entry["task_id"]]["term"], task, item)
                destination = output / ("tej_arrival_" + entry["task_id"] + ".json")
                if destination.exists():
                    if json.loads(destination.read_text()) != detail:
                        raise ValueError("retained arrival result differs from source readback")
                else:
                    atomic_write_json(destination, detail, durable=True)
                observed.update(result_path=str(destination), result_sha256=sha256_file(destination),
                                comparison_state=detail["state"])
            result_items.append(observed)
    return {"contract": "tw_futures_holiday_tej_arrival_status_v1",
            "observed_at_utc": datetime.now(UTC).isoformat(), "items": result_items,
            "observer_input_pins": pins, "provider_calls": 0,
            "financial_or_training_adopted": False, "full_history_complete": False}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--hours", type=float, default=144)
    parser.add_argument("--poll-seconds", type=int, default=120)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    output = args.output_dir.resolve(strict=True)
    if (not output.is_relative_to(ROOT / "artifacts/markets") or not output.is_dir()
            or not 0 < args.hours <= 168 or args.poll_seconds < 30):
        raise ValueError("bounded observer and formal output directory required")
    spec, _, selected, pins = load_contract(args.spec)
    deadline = datetime.now(UTC) + timedelta(hours=args.hours)
    previous = None
    while True:
        status = observe(spec, selected, pins, output)
        atomic_write_json(output / "tej_arrival_status.json", status, durable=True)
        states = [(x["task_id"], x["state"], x["priority_active"]) for x in status["items"]]
        if states != previous:
            print(json.dumps({"observed_at_utc": status["observed_at_utc"], "items": status["items"],
                              "provider_calls": 0}, ensure_ascii=False), flush=True)
            previous = states
        if all(x["state"] == "complete" for x in status["items"]):
            return 0
        if (args.once or datetime.now(UTC) >= deadline
                or any(x["state"] in {"blocked", "failed"} or not x["priority_active"]
                       for x in status["items"] if x["state"] != "complete")):
            return 75
        time.sleep(args.poll_seconds)


if __name__ == "__main__":
    raise SystemExit(main())

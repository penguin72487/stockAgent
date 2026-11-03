#!/usr/bin/env python3
"""Compare a pinned TAIFEX trade archive with a selected training product scope.

This is a streaming source audit, not a listing, fill, PIT or accounting test.
The product scope must be copied from the exact financial preparation and pinned
separately; older currency classifications are deliberately not extrapolated to
products absent from the supplied catalog.
"""
from __future__ import annotations

import argparse
from collections import Counter
import csv
from datetime import date, datetime, timezone
import io
import json
import math
from pathlib import Path
import re
import zipfile

from downloader.artifact_io import atomic_write_json, sha256_file
from scripts.download_taifex_recent_index_derivatives_ticks import (
    _parse_float, _parse_time, _validate_event_date,
)
from stockagent.data.tw_futures_official_ticks import pin_official_tick_inventory


def _rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as stream:
        return list(csv.DictReader(stream))


def audit(*, manifest: Path, day: date, scope: Path, scope_sha256: str,
          catalog: Path) -> dict:
    manifest, scope, catalog = (p.resolve(strict=True) for p in (manifest, scope, catalog))
    if not re.fullmatch(r"[0-9a-f]{64}", scope_sha256):
        raise ValueError("scope must have a pinned SHA-256")
    inputs = {str(p): sha256_file(p) for p in (manifest, scope, catalog)}
    if inputs[str(scope)] != scope_sha256:
        raise ValueError("copied training scope differs from its pinned identity")
    selected, seen_scope = set(), set()
    for item in _rows(scope):
        product = (item.get("product") or "").strip()
        if not product or product in seen_scope:
            raise ValueError("training scope contains duplicate or empty products")
        seen_scope.add(product)
        selected_count = int(item["selected_lifetimes"])
        if selected_count < 0:
            raise ValueError("negative selected lifetime count")
        if selected_count:
            selected.add(product)
    if not selected:
        raise ValueError("training scope selected no product")
    classifications = {}
    for item in _rows(catalog):
        product = (item.get("product") or "").strip()
        if not product or product in classifications:
            raise ValueError("source catalog contains duplicate or empty products")
        classifications[product] = {key: item.get(key) or None for key in (
            "product_name", "asset_class", "region", "settlement_currency")}

    pinned = pin_official_tick_inventory(manifest, [day])[0]
    archive = Path(pinned["path"])
    if archive.stat().st_size != pinned["bytes"] or sha256_file(archive) != pinned["sha256"]:
        raise ValueError("official trade archive differs from the canonical receipt")
    product_rows: Counter[str] = Counter()
    product_contracts: dict[str, set[str]] = {}
    event_dates: set[str] = set()
    skipped_spreads = 0
    member_name = f"Daily_{day.strftime('%Y_%m_%d')}.csv"
    with zipfile.ZipFile(archive) as zipped:
        members = zipped.infolist()
        if (len(members) != 1 or members[0].filename != member_name or members[0].is_dir()
                or members[0].flag_bits & 1 or members[0].file_size > 128 * 1024 * 1024):
            raise ValueError("unexpected official archive member")
        with zipped.open(members[0]) as binary, io.TextIOWrapper(binary, encoding="cp950", newline="") as stream:
            rows = csv.reader(stream)
            if not next(rows, None):
                raise ValueError("official archive has no header")
            for number, raw in enumerate(rows, start=2):
                cells = [cell.strip() for cell in raw]
                if not any(cells):
                    continue
                if len(cells) < 9 or (len(cells) > 9 and any(cells[9:])):
                    raise ValueError(f"row {number}: unexpected futures columns")
                if not re.fullmatch(r"[A-Z0-9]+", cells[1]) or not cells[2]:
                    raise ValueError(f"row {number}: unknown product or contract")
                _validate_event_date(cells[0], trading_date=day, row_number=number)
                _parse_time(cells[3], row_number=number)
                if "/" in cells[2]:
                    skipped_spreads += 1
                    continue
                price = _parse_float(cells[4], row_number=number, field="price")
                if not math.isfinite(price):
                    raise ValueError(f"row {number}: non-finite price")
                quantity = int(cells[5])
                if quantity <= 0 or quantity % 2 or cells[8] not in {"", "*"}:
                    raise ValueError(f"row {number}: invalid quantity or auction marker")
                product_rows[cells[1]] += 1
                product_contracts.setdefault(cells[1], set()).add(cells[2])
                event_dates.add(cells[0])
    observed = set(product_rows)
    if not observed:
        raise ValueError("no observed outright product in source archive")
    details = []
    for product in sorted(observed):
        category = classifications.get(product, {})
        details.append({"product": product, **category,
                        "catalog_status": "present" if category else "not_in_catalog",
                        "in_selected_training_scope": product in selected,
                        "source_outright_print_rows": product_rows[product],
                        "source_contract_labels": sorted(product_contracts[product])})
    known_twd = {p for p in observed if classifications.get(p, {}).get("settlement_currency") == "TWD"}
    unknown = {p for p in observed if p not in classifications}
    if any(sha256_file(Path(p)) != digest for p, digest in inputs.items()):
        raise ValueError("source input changed during audit")
    if sha256_file(archive) != pinned["sha256"]:
        raise ValueError("official trade archive changed during audit")
    return {
        "contract": "taifex_observed_print_vs_selected_training_scope_v1",
        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_trading_date": str(day), "source_event_dates": sorted(event_dates),
        "inputs_sha256": inputs, "source_archive": pinned,
        "code_sha256": sha256_file(Path(__file__)),
        "parser_sha256": sha256_file(Path(__file__).parent / "download_taifex_recent_index_derivatives_ticks.py"),
        "selected_training_products": len(selected), "observed_outright_products": len(observed),
        "observed_in_training_scope": len(observed & selected),
        "observed_outside_training_scope": sorted(observed - selected),
        "observed_known_twd_settlement_products": len(known_twd),
        "known_twd_settlement_in_training_scope": len(known_twd & selected),
        "known_twd_settlement_outside_training_scope": sorted(known_twd - selected),
        "unknown_to_historical_catalog": sorted(unknown),
        "spread_rows_excluded": skipped_spreads, "products": details,
        "limitations": [
            "The original archive trading date may include the preceding evening's prints.",
            "Print occurrence is not the complete listed universe or evidence of an executable order.",
            "Historical catalog classification or settlement currency is not a verified current quote currency.",
            "Scope membership means some lifetime was selected, not availability or training at this trading date.",
            "This audit does not establish source completeness, PIT features, account readiness or fills.",
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--date", type=date.fromisoformat, required=True)
    parser.add_argument("--scope", type=Path, required=True)
    parser.add_argument("--scope-sha256", required=True)
    parser.add_argument("--catalog", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError("preserve the existing audit and choose a new output")
    result = audit(manifest=args.manifest, day=args.date, scope=args.scope,
                   scope_sha256=args.scope_sha256, catalog=args.catalog)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_json(args.output, result)
    print(json.dumps({key: result[key] for key in (
        "source_trading_date", "selected_training_products", "observed_outright_products",
        "observed_in_training_scope", "known_twd_settlement_outside_training_scope",
        "unknown_to_historical_catalog")}, ensure_ascii=False))


if __name__ == "__main__":
    main()

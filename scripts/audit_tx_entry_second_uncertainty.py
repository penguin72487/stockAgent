#!/usr/bin/env python3
"""Summarize within-second price ambiguity in a retained TX tick audit.

The public prints are not a fill or a bid/ask book. A price range multiplied by
the TX point value is only the spread of observed prices at the same second;
it is neither a slippage estimate nor a bound on an actual order's fill.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import UTC, datetime
import json
import math
from pathlib import Path
import sys

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from downloader.artifact_io import atomic_write_json, sha256_file  # noqa: E402


def summarize(doc: dict) -> dict:
    """Check the parent audit and aggregate its explicitly observed probes."""
    if (doc.get("contract") != "tx_retained_finmind_contract_day_clock_audit_v2"
            or doc.get("benchmark_admitted") is not False
            or doc.get("training_admitted") is not False):
        raise ValueError("unsupported or already admitted parent audit")
    rows = doc.get("rows")
    counts = doc.get("counts", {})
    if not isinstance(rows, list) or len(rows) != counts.get("target_contract_days"):
        raise ValueError("parent target-day count differs")
    allowed = {"year_entry": {"08:45:00", "09:00:00"}, "expiry_reopen": {"13:31:00"}}
    groups: dict[tuple[str, str], list[float]] = defaultdict(list)
    seen: set[tuple[str, str, str, str]] = set()
    for row in rows:
        role, day, contract = row.get("role"), row.get("date"), row.get("contract_month")
        probes = row.get("entry_probes")
        if (role not in allowed or row.get("product") != "TX"
                or not isinstance(day, str) or not isinstance(contract, str)
                or not row.get("valid_tick_values") or not row.get("full_book_matches")
                or not isinstance(probes, list) or len(probes) != len(allowed[role])):
            raise ValueError("parent has an invalid target or incomplete tick book")
        for probe in probes:
            clock = probe.get("after")
            identity = (day, contract, role, clock)
            if clock not in allowed[role] or identity in seen or probe.get("observed") is not True:
                raise ValueError("missing, duplicate or unobserved probe")
            seen.add(identity)
            prices = probe.get("distinct_prices")
            if (not isinstance(prices, list) or not prices
                    or any(isinstance(x, bool) or not isinstance(x, (int, float))
                           or not math.isfinite(x) or x <= 0 for x in prices)
                    or prices != sorted(set(prices))
                    or probe.get("same_second_price_unambiguous") != (len(prices) == 1)):
                raise ValueError("invalid observed price set or ambiguity flag")
            groups[(role, clock)].append(float(max(prices) - min(prices)))
    if len(seen) != counts.get("clock_probes"):
        raise ValueError("parent clock-probe count differs")
    observed_ambiguous = sum(value > 0 for values in groups.values() for value in values)
    if observed_ambiguous != counts.get("same_second_multi_price_probes"):
        raise ValueError("parent multi-price count differs")
    # The existing diagnostic independently checked that the selected TX
    # source rows have a TWD 200 multiplier. This is a scenario assumption,
    # not newly validated historical specification evidence.
    results = []
    for (role, clock), values in sorted(groups.items()):
        spread_twd = np.asarray(values, dtype=np.float64) * 200.0
        results.append({"role": role, "after": clock, "probes": len(values),
                        "multi_price_probes": int(np.count_nonzero(spread_twd)),
                        "mean_observed_range_twd": float(spread_twd.mean()),
                        "median_observed_range_twd": float(np.median(spread_twd)),
                        "p95_observed_range_twd": float(np.percentile(spread_twd, 95)),
                        "max_observed_range_twd": float(spread_twd.max())})
    return {"contract": "tx_same_second_observed_price_range_v1",
            "created_at_utc": datetime.now(UTC).isoformat(),
            "assumed_twd_per_point": 200, "probe_count": len(seen),
            "multi_price_probe_count": observed_ambiguous, "groups": results,
            "benchmark_admitted": False, "training_admitted": False,
            "limitations": [
                "First observed second after a probe, not an exchange order or a broker fill.",
                "Observed price range is not a slippage estimate or a guaranteed fill-price bound.",
                "The same retained vendor tick audit is used; no historical receive-time proof.",
                "The multiplier is a declared scenario, not newly verified dated specifications.",
            ]}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--parent-audit", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists() or args.output.is_symlink():
        raise FileExistsError("preserve earlier audit evidence; use a new output")
    parent_hash = sha256_file(args.parent_audit)
    result = summarize(json.loads(args.parent_audit.read_text()))
    if sha256_file(args.parent_audit) != parent_hash:
        raise ValueError("parent audit changed during readback")
    result["parent_audit"] = str(args.parent_audit.resolve())
    result["parent_sha256"] = parent_hash
    result["implementation_sha256"] = sha256_file(Path(__file__))
    atomic_write_json(args.output, result)
    print(json.dumps({"probes": result["probe_count"],
                      "multi_price_probes": result["multi_price_probe_count"]}))


if __name__ == "__main__":
    main()

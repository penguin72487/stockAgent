#!/usr/bin/env python3
"""Snapshot current Shioaji Contract V2 FUT/OPT tick metadata.

The snapshot is supporting evidence for current rules.  It is not historical
proof: dated exchange notices remain authoritative for past rule changes.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path

from dotenv import load_dotenv


def _root_value(item: object) -> str:
    if isinstance(item, (tuple, list)):
        return str(item[0])
    return str(getattr(item, "root", item))


def _serialize_info(info: object, bands: object) -> dict[str, object]:
    fields = (
        "security_type",
        "region",
        "exchange",
        "code",
        "name",
        "root",
        "begin_date",
        "delivery_date",
        "underlying_kind",
        "underlying_code",
        "quote_ccy",
        "tick_basis",
        "tick_rule",
        "tick",
        "tick_value",
        "spec_kind",
        "decimal_locator",
        "update_date",
    )
    row = {field: getattr(info, field, None) for field in fields}
    row["bands"] = bands
    return row


def _json_default(value: object) -> object:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(
            "artifacts/data_quality/tw_price_precision/"
            "shioaji_contract_v2_tick_snapshot.json"
        ),
    )
    parser.add_argument(
        "--production",
        action="store_true",
        help="use a production-entitled key instead of the simulation endpoint",
    )
    args = parser.parse_args()
    load_dotenv(args.env_file)
    key = os.environ.get("SHIOAJI_API_KEY", "").strip()
    secret = os.environ.get("SHIOAJI_SECRET_KEY", "").strip()
    if not key or not secret:
        raise RuntimeError("SHIOAJI_API_KEY and SHIOAJI_SECRET_KEY are required")

    import shioaji as sj

    api = sj.Shioaji(simulation=not args.production)
    api.set_event_callback(lambda *_args: None)
    api.login(api_key=key, secret_key=secret, subscribe_trade=False)
    rows: list[dict[str, object]] = []
    problems: list[str] = []
    try:
        groups = (
            ("FUT", sorted({_root_value(item) for item in api.contracts.futures_roots()})),
            ("OPT", sorted({_root_value(item) for item in api.contracts.option_roots()})),
        )
        for security_type, roots in groups:
            for root in roots:
                try:
                    chain = (
                        api.contracts.futures(root)
                        if security_type == "FUT"
                        else api.contracts.options(root)
                    )
                    info = next(
                        (
                            item
                            for item in chain
                            if not str(getattr(item, "code", "")).endswith(("R1", "R2"))
                        ),
                        None,
                    )
                    if info is None:
                        problems.append(f"{security_type}:{root}:no_current_contract")
                        continue
                    tick_rule = getattr(info, "tick_rule", None)
                    bands = api.contracts.tick_bands(info) if tick_rule else None
                    rows.append(_serialize_info(info, bands))
                except Exception as exc:
                    problems.append(f"{security_type}:{root}:{type(exc).__name__}:{exc}")
    finally:
        api.logout()

    payload = {
        "schema_version": 1,
        "source": "shioaji_contract_v2",
        "meaning": (
            "Current provider contract metadata only; use dated official exchange "
            "notices for historical effective-date rules."
        ),
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "simulation_endpoint": not args.production,
        "rows": rows,
        "problems": problems,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_suffix(args.output.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, args.output)
    print(
        json.dumps(
            {
                "output": str(args.output),
                "rows": len(rows),
                "problems": len(problems),
            },
            ensure_ascii=False,
        )
    )
    return 0 if not problems else 2


if __name__ == "__main__":
    raise SystemExit(main())

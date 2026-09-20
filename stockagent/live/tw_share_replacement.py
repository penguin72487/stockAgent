"""Receipt-backed physical share actions; prices and executions remain separate."""
from __future__ import annotations

from datetime import date
from functools import lru_cache
import hashlib
import json
from pathlib import Path

import polars as pl

from downloader.artifact_io import sha256_file
from stockagent.backtest.tw_day_trade_contract import ODD_LOT_BOARD_PRICE as ODD_LOT_BOARD_PRICE

SHARE_REPLACEMENT_CONTRACT = "physical_share_replacement_separate_cash_claim_v1"


def load_share_replacements(root: Path, *, required_start: date | None = None,
                            required_end: date | None = None) -> tuple[dict, ...]:
    path = root / "tw_share_replacement_reference.parquet"
    summary = path.with_suffix(".summary.json")
    if not path.exists() and not summary.exists():
        if required_start is not None or required_end is not None:
            raise ValueError("share replacement source is required for physical carried inventory")
        return ()
    signature = tuple((p.stat().st_size, p.stat().st_mtime_ns, p.stat().st_ctime_ns)
                      for p in (path, summary))
    rows, proof = _load(str(path.resolve()), signature)
    if required_start is not None or required_end is not None:
        first = date.fromisoformat(str(proof.get("coverage_start") or ""))
        last = date.fromisoformat(str(proof.get("coverage_end") or ""))
        announced_end = date.fromisoformat(str(proof.get("announced_resumption_query_end") or proof.get("coverage_end") or ""))
        if (set(proof.get("covered_markets") or ()) != {"twse", "tpex"}
                or announced_end <= last
                or (required_start is not None and first > required_start)
                or (required_end is not None and last < required_end)):
            raise ValueError("share replacement coverage does not cover carried interval and both markets")
    return rows


@lru_cache(maxsize=8)
def _load(name: str, signature: tuple) -> tuple[tuple[dict, ...], dict]:
    path = Path(name)
    proof = json.loads(path.with_suffix(".summary.json").read_text())
    raw = proof.get("raw_receipt_manifest") or {}
    raw_path = (path.parent / str(raw.get("relative_path", ""))).resolve()
    if (not proof.get("source_download_complete") or proof.get("failure_count") != 0
            or not raw_path.is_relative_to(path.parent)
            or sha256_file(path) != (proof.get("output_receipt") or {}).get("sha256")
            or sha256_file(raw_path) != raw.get("sha256")):
        raise ValueError("share replacement source receipt is invalid")
    # A manifest digest is not proof that its referenced official responses
    # still exist. Verify the complete retained request/response chain.
    for line in raw_path.read_text().splitlines():
        item = json.loads(line)
        if not isinstance(item, dict) or not {"path", "request", "response_size", "response_sha256", "request_sha256"} <= item.keys():
            raise ValueError("share replacement raw receipt schema is invalid")
        source = (path.parent / item["path"]).resolve()
        request = json.dumps(item["request"], ensure_ascii=True,
                             separators=(",", ":"), sort_keys=True).encode()
        if (not source.is_relative_to(path.parent / "raw")
                or source.stat().st_size != item["response_size"]
                or sha256_file(source) != item["response_sha256"]
                or hashlib.sha256(request).hexdigest() != item["request_sha256"]):
            raise ValueError("share replacement raw response/request receipt is invalid")
    rows = pl.read_parquet(path).to_dicts()
    if len(rows) != proof.get("rows"):
        raise ValueError("share replacement row count mismatch")
    return tuple(rows), proof


def halted_symbols(root: Path, day: date) -> set[str]:
    return {e["symbol"] for e in load_share_replacements(root)
            if e.get("suspension_date") is not None
            and e["suspension_date"] <= day < e["resume_date"]}

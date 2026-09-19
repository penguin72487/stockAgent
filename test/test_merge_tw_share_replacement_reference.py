from __future__ import annotations

from datetime import date
import hashlib
import json
from pathlib import Path

import polars as pl
import pytest

from scripts.merge_tw_share_replacement_reference import merge
from stockagent.live.tw_share_replacement import load_share_replacements


def _row(symbol: str, when: date, *, ratio: float | None = 900.0) -> dict:
    return {
        "market": "twse",
        "symbol": symbol,
        "resume_date": when,
        "contract": "exchange_share_replacement_reference_v1",
        "suspension_date": date.fromordinal(when.toordinal() - 3) if ratio else None,
        "new_shares_per_1000_old": ratio,
        "cash_return_per_old_share": 0.0 if ratio else None,
        "cash_payment_date": None,
        "cash_dividend_per_old_share": 0.0 if ratio else None,
        "subscription_shares_per_1000": 0.0 if ratio else None,
        "subscription_terms_present": False,
        "historical_halt_evidence": bool(ratio),
        "executable_price": bool(ratio),
        "accounting_terms_complete": bool(ratio),
        "unresolved_terms": None if ratio else "terms_unavailable",
    }


def _accepted(root: Path, *, rows: list[dict], start: date, end: date) -> None:
    root.mkdir(parents=True)
    raw = root / "raw/tw_share_replacement_reference/source.json"
    raw.parent.mkdir(parents=True)
    response = b'{"stat":"ok"}'
    raw.write_bytes(response)
    request = {"url": "https://official.example/source", "data": {}}
    request_bytes = json.dumps(
        request, ensure_ascii=True, separators=(",", ":"), sort_keys=True
    ).encode()
    item = {
        "path": raw.relative_to(root).as_posix(),
        "request": request,
        "request_sha256": hashlib.sha256(request_bytes).hexdigest(),
        "response_size": len(response),
        "response_sha256": hashlib.sha256(response).hexdigest(),
    }
    manifest_bytes = (json.dumps(item) + "\n").encode()
    digest = hashlib.sha256(manifest_bytes).hexdigest()
    manifest = root / f"raw/tw_share_replacement_reference/manifests/{digest}.jsonl"
    manifest.parent.mkdir(parents=True)
    manifest.write_bytes(manifest_bytes)
    path = root / "tw_share_replacement_reference.parquet"
    pl.from_dicts(rows).write_parquet(path)
    payload = {
        "schema_version": 3,
        "coverage_start": start.isoformat(),
        "coverage_end": end.isoformat(),
        "announced_resumption_query_end": "2026-06-01",
        "covered_markets": ["twse", "tpex"],
        "complete_all_markets": True,
        "lifecycle_catalog_complete": True,
        "source_download_complete": True,
        "failure_count": 0,
        "accounting_failures": [],
        "recovered_exchange_detail_failures": [],
        "rows": len(rows),
        "output_receipt": {"sha256": hashlib.sha256(path.read_bytes()).hexdigest()},
        "raw_receipt_manifest": {
            "relative_path": manifest.relative_to(root).as_posix(),
            "sha256": digest,
        },
    }
    path.with_suffix(".summary.json").write_text(json.dumps(payload))


def _fixtures(tmp_path: Path) -> tuple[Path, Path]:
    root = tmp_path / "public"
    tail = root / "execution_actions"
    _accepted(
        root,
        rows=[
            {**_row("1111", date(2025, 12, 20)), "detail_error": None},
            {**_row("2222", date(2026, 1, 2), ratio=None), "detail_error": "missing"},
        ],
        start=date(2014, 1, 1),
        end=date(2026, 1, 2),
    )
    _accepted(
        tail,
        rows=[
            _row("2222", date(2026, 1, 2)),
            _row("3333", date(2026, 1, 4)),
        ],
        start=date(2026, 1, 1),
        end=date(2026, 1, 4),
    )
    return root, tail


def _merge(root: Path, tail: Path, *, apply: bool) -> dict:
    return merge(
        root,
        tail,
        cutover=date(2026, 1, 1),
        required_start=date(2014, 1, 1),
        required_end=date(2026, 1, 4),
        apply=apply,
    )


def test_merge_preserves_baseline_and_replaces_verified_overlap(tmp_path: Path) -> None:
    root, tail = _fixtures(tmp_path)
    old_hash = hashlib.sha256((root / "tw_share_replacement_reference.parquet").read_bytes()).hexdigest()
    plan = _merge(root, tail, apply=False)
    assert plan["merged_rows"] == 3
    assert plan["overlap_event_count"] == 1
    assert plan["accounting_revisions"] == 1
    assert hashlib.sha256((root / "tw_share_replacement_reference.parquet").read_bytes()).hexdigest() == old_hash

    result = _merge(root, tail, apply=True)
    assert result["verified"] is True
    assert result["output_sha256"] != old_hash
    rows = load_share_replacements(
        root, required_start=date(2014, 1, 1), required_end=date(2026, 1, 4)
    )
    assert len(rows) == 3
    assert next(row for row in rows if row["symbol"] == "2222")["new_shares_per_1000_old"] == 900.0
    assert _merge(root, tail, apply=True)["output_sha256"] == result["output_sha256"]


def test_merge_rejects_missing_overlap_identity(tmp_path: Path) -> None:
    root, tail = _fixtures(tmp_path)
    path = tail / "tw_share_replacement_reference.parquet"
    pl.read_parquet(path).filter(pl.col("symbol") != "2222").write_parquet(path)
    summary_path = path.with_suffix(".summary.json")
    summary = json.loads(summary_path.read_text())
    summary["rows"] = 1
    summary["output_receipt"]["sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
    summary_path.write_text(json.dumps(summary))
    with pytest.raises(ValueError, match="overlapping exchange event identities"):
        _merge(root, tail, apply=False)


def test_merge_rejects_corrupt_raw_receipt(tmp_path: Path) -> None:
    root, tail = _fixtures(tmp_path)
    (tail / "raw/tw_share_replacement_reference/source.json").write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="raw response/request receipt is invalid"):
        _merge(root, tail, apply=False)

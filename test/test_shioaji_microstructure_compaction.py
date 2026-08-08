from __future__ import annotations

from datetime import date
import json
from pathlib import Path
from types import SimpleNamespace

import polars as pl

from scripts import compact_shioaji_microstructure_by_symbol as compact


def _write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def test_compaction_writes_exactly_one_parquet_per_symbol(
    tmp_path: Path,
    monkeypatch,
) -> None:
    trade_dates = ("2026-08-05", "2026-08-06")
    capture_id = "capture1"
    capture_root = tmp_path / "captures"
    selection_root = tmp_path / "hft"
    audit_root = tmp_path / "audits"
    output_root = tmp_path / "symbol_dataset"
    symbols = ("2330", "2317")
    for trade_date in trade_dates:
        for worker, symbol in enumerate(symbols):
            manifest = {
                "schema_version": 3,
                "status": "complete",
                "capture_id": capture_id,
                "worker_index": worker,
                "symbols": [symbol],
                "tick_parts": 1,
                "book_parts": 1,
                "book_1s_parts": 1,
                "tick_rows_written": 2,
                "book_rows_written": 2,
                "book_1s_rows_written": 2,
            }
            _write_json(
                capture_root
                / "manifests"
                / f"trade_date={trade_date}"
                / f"worker={worker:02d}.json",
                manifest,
            )
            for stream, order_column in compact.ORDER_COLUMN.items():
                path = (
                    capture_root
                    / stream
                    / f"trade_date={trade_date}"
                    / "hour=09"
                    / (
                        f"capture={capture_id}-worker={worker:02d}-"
                        f"part=000001-123456789.parquet"
                    )
                )
                path.parent.mkdir(parents=True, exist_ok=True)
                frame = pl.DataFrame(
                    {
                        "code": [symbol, symbol],
                        "trade_date": [date.fromisoformat(trade_date)] * 2,
                        order_column: [100 + worker, 200 + worker],
                        f"{stream}_value": [1.0, 2.0],
                    }
                )
                frame.write_parquet(path)
        _write_json(
            selection_root / f"trade_date={trade_date}" / "summary.json",
            {"status": "ok", "capture_id": capture_id},
        )
        _write_json(
            audit_root / f"hft_{trade_date}.json",
            {
                "status": "ok",
                "failures": {
                    "duplicate_keys": 0,
                    "future_feature_rows": 0,
                    "label_errors": 0,
                },
            },
        )
    monkeypatch.setattr(
        compact,
        "parse_args",
        lambda: SimpleNamespace(
            capture_root=capture_root,
            selection_root=selection_root,
            audit_root=audit_root,
            output_root=output_root,
            through_date=None,
            compression_level=1,
            hash_workers=2,
        ),
    )

    compact.main()

    outputs = sorted(output_root.glob("symbols/symbol=*/data.parquet"))
    assert len(outputs) == 2
    manifest = json.loads((output_root / "manifest.json").read_text())
    assert manifest["layout"] == "one_parquet_per_symbol"
    assert manifest["source_files"] == 12
    assert manifest["rows"] == {
        "ticks": 8,
        "book_events": 8,
        "book_1s": 8,
    }
    assert manifest["total_rows"] == 24
    assert {item["path"] for item in manifest["partitions"]} == {
        "symbols/symbol=2317/data.parquet",
        "symbols/symbol=2330/data.parquet",
    }
    for path in outputs:
        frame = pl.read_parquet(path)
        assert frame["code"].n_unique() == 1
        assert set(frame["source_stream"]) == set(compact.STREAMS)

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from types import SimpleNamespace

import polars as pl

from downloader.download_shioaji_tw_kbars import UniverseRow, _cached_minute_chunk, _sha256
from stockagent.live import quote_provider
from stockagent.live.shioaji_traffic_ledger import (
    StreamingLedgerRecorder,
    quota_window_date,
    rebuild_traffic_summary,
    record_avoided_query,
    shioaji_query,
)


def test_traffic_ledger_groups_raw_events_by_local_observation_date() -> None:
    assert quota_window_date(datetime(2026, 8, 14, 23, 59, tzinfo=timezone.utc)) == (
        "2026-08-15"
    )
    assert quota_window_date(datetime(2026, 8, 15, 0, 0, tzinfo=timezone.utc)) == (
        "2026-08-15"
    )


class _UsageApi:
    def __init__(self) -> None:
        self.used = 100

    def usage(self):
        return SimpleNamespace(bytes=self.used, limit_bytes=1_000)


def test_traffic_ledger_attributes_queries_and_avoided_calls(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("STOCKAGENT_SHIOAJI_TRAFFIC_LEDGER_ROOT", str(tmp_path))
    api = _UsageApi()
    with shioaji_query(
        api,
        consumer="test_consumer",
        method="snapshots",
        asset_class="stock",
        details={"contract_count": 2},
    ) as set_result:
        api.used += 25
        set_result([1, 2])
    record_avoided_query(
        consumer="test_consumer",
        method="snapshots",
        asset_class="stock",
        reason="cache_hit",
    )

    summary = json.loads((tmp_path / "summary.json").read_text(encoding="utf-8"))
    assert summary["totals"]["queries"] == 1
    assert summary["totals"]["avoided_queries"] == 1
    assert summary["totals"]["observed_usage_delta_bytes"] == 25
    assert summary["by_consumer"]["test_consumer"]["rows"] == 2
    ledger = next((tmp_path / "daily").glob("*.jsonl")).read_text(encoding="utf-8")
    assert "secret" not in ledger.lower()


def test_traffic_ledger_starts_quota_epoch_only_after_observed_counter_drop(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("STOCKAGENT_SHIOAJI_TRAFFIC_LEDGER_ROOT", str(tmp_path))
    api = _UsageApi()
    api.used = 900
    with shioaji_query(
        api,
        consumer="history",
        method="ticks",
        asset_class="futures",
    ) as set_result:
        api.used = 950
        set_result([1])

    api.used = 0
    with shioaji_query(
        api,
        consumer="strategy",
        method="snapshots",
        asset_class="stock",
    ) as set_result:
        api.used = 25
        set_result([1, 2])

    summary = json.loads((tmp_path / "summary.json").read_text(encoding="utf-8"))
    assert summary["schema_version"] == 2
    assert summary["quota_epoch"]["boundary_kind"] == "observed_counter_drop"
    assert summary["quota_epoch"]["reset_observed"] is True
    assert summary["latest_reset"]["previous_used_bytes"] == 950
    assert summary["latest_reset"]["new_used_bytes"] == 0
    assert summary["latest_usage"]["used_bytes"] == 25
    assert summary["totals"]["queries"] == 1
    assert summary["totals"]["observed_usage_delta_bytes"] == 25
    assert set(summary["by_consumer"]) == {"strategy"}

    (tmp_path / "summary.json").unlink()
    rebuilt = rebuild_traffic_summary(root=tmp_path)
    assert rebuilt["latest_reset"]["previous_used_bytes"] == 950
    assert rebuilt["latest_usage"]["used_bytes"] == 25
    assert rebuilt["totals"]["queries"] == 1


def test_streaming_ledger_records_deltas_as_quota_exempt(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("STOCKAGENT_SHIOAJI_TRAFFIC_LEDGER_ROOT", str(tmp_path))
    recorder = StreamingLedgerRecorder(
        consumer="taifex_fop_stream",
        asset_class="futures_options",
        details={"worker_index": 0},
    )
    recorder.observe(
        tick_events=10,
        book_events=20,
        snapshot_rows=15,
        dropped_events=0,
        stored_bytes=1000,
    )
    recorder.observe(
        tick_events=15,
        book_events=27,
        snapshot_rows=20,
        dropped_events=1,
        stored_bytes=1400,
    )
    summary = json.loads((tmp_path / "summary.json").read_text(encoding="utf-8"))
    totals = summary["totals"]
    assert totals["queries"] == 0
    assert totals["observed_usage_delta_bytes"] == 0
    assert totals["stream_tick_events"] == 15
    assert totals["stream_book_events"] == 27
    assert totals["stream_snapshot_rows"] == 20
    assert totals["stream_dropped_events"] == 1
    assert totals["stream_stored_bytes"] == 1400


def test_futures_stream_book_is_causal_and_falls_back_when_stale(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setenv(
        "STOCKAGENT_SHIOAJI_TRAFFIC_LEDGER_ROOT", str(tmp_path / "ledger")
    )
    decision = datetime(2026, 8, 14, 1, 0, 1, tzinfo=timezone.utc)
    receive_ns = int(decision.timestamp() * 1e9) - 500_000_000
    runtime = tmp_path / "capture" / "runtime"
    runtime.mkdir(parents=True)
    (runtime / "worker_00.json").write_text(
        json.dumps(
            {
                "contract_metadata": {
                    "TXFH6": {
                        "logical_code": "TXFR1",
                        "delivery_month": "202608",
                        "delivery_date": "2026-08-19",
                        "last_trading_date": "2026-08-19",
                    }
                },
                "books": {
                    "TXFH6": {
                        "snapshot_ts_ns": receive_ns,
                        "book_receive_ts_ns": receive_ns,
                        "bid_price_1": 100.0,
                        "ask_price_1": 101.0,
                        "bid_volume_1": 5,
                        "ask_volume_1": 6,
                        "stale": False,
                        "suspend": False,
                        "simtrade": False,
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        quote_provider,
        "fetch_shioaji_futures_snapshot",
        lambda *_args, **_kwargs: {"source": "fallback"},
    )

    fresh = quote_provider.fetch_futures_snapshot_prefer_stream(
        "TXFR1",
        decision_time=decision,
        capture_root=tmp_path / "capture",
        max_age_seconds=2.0,
    )
    stale = quote_provider.fetch_futures_snapshot_prefer_stream(
        "TXFR1",
        decision_time=decision,
        capture_root=tmp_path / "capture",
        max_age_seconds=0.1,
    )
    assert fresh["current_contract_code"] == "TXFH6"
    assert fresh["quotes"]["TXFH6"]["bid"] == 100.0
    assert fresh["quotes"]["TXFH6"]["delivery_month"] == "202608"
    assert fresh["quotes"]["TXFH6"]["last_trading_date"] == "2026-08-19"
    assert stale == {"source": "fallback"}


def test_daily_materializer_reuses_only_hash_verified_complete_minute_source(
    tmp_path: Path,
) -> None:
    root = tmp_path / "minute"
    data_path = root / "minute_chunks/2330/2026-08-01_2026-08-10.parquet"
    data_path.parent.mkdir(parents=True)
    frame = pl.DataFrame(
        {
            "ts": [datetime(2026, 8, 3, 1, 1)],
            "Open": [100.0],
            "High": [101.0],
            "Low": [99.0],
            "Close": [100.5],
            "Volume": [10.0],
            "Amount": [1_005_000.0],
            "date": [datetime(2026, 8, 3).date()],
            "symbol": ["2330"],
            "market": ["twse"],
            "contract_unit": [1000.0],
        }
    )
    frame.write_parquet(data_path)
    manifest_path = root / "symbols/2330.manifest.json"
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text(
        json.dumps(
            {
                "source": "shioaji_kbars_1m",
                "requested_start": "2026-08-01",
                "requested_end": "2026-08-10",
                "source_gap_dates": [],
                "chunks": [
                    {
                        "start_date": "2026-08-01",
                        "end_date": "2026-08-10",
                        "data_path": str(data_path),
                        "data_sha256": _sha256(data_path),
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    row = UniverseRow("2330", "台積電", "twse", "stock", tmp_path / "2330.parquet")
    cached = _cached_minute_chunk(
        root,
        row,
        start=datetime(2026, 8, 1).date(),
        end=datetime(2026, 8, 10).date(),
        expected_dates={datetime(2026, 8, 3).date()},
    )
    assert cached is not None and cached.height == 1
    data_path.write_bytes(data_path.read_bytes() + b"corrupt")
    assert (
        _cached_minute_chunk(
            root,
            row,
            start=datetime(2026, 8, 1).date(),
            end=datetime(2026, 8, 10).date(),
            expected_dates={datetime(2026, 8, 3).date()},
        )
        is None
    )

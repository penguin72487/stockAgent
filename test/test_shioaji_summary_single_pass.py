"""A coverage summary must share the dispatch rules, not rescan the archive."""

from datetime import date
from itertools import product
from types import SimpleNamespace

from downloader import download_shioaji_historical_market_data as collector


def row():
    return collector.HistoryContract(
        collection="exact_futures", priority=-1, security_type="FUT",
        asset_class="futures", code="TXFJ6", root="TXF", name="TX",
        exchange="TAIFEX", begin_date=date(2026, 10, 12),
        end_date=date(2026, 10, 13), delivery_date=date(2026, 10, 21),
    )


def test_summary_visits_each_required_receipt_only_once(tmp_path, monkeypatch):
    contract = row()
    calls = []
    days = [date(2026, 10, 12), date(2026, 10, 13)]

    def receipt(path, _data_path, *, method, code):
        calls.append((method, code, path.parent.name))
        return {
            "status": "complete", "rows": 3 if method == "kbars" else 7,
            "size": 100, "observed_trading_dates": [str(days[0]), "bad", str(days[0])],
        }

    def forbidden(*_args, **_kwargs):
        raise AssertionError("summary repeated the plan or KBar receipt scan")

    monkeypatch.setattr(collector, "_valid_receipt", receipt)
    monkeypatch.setattr(collector, "observed_tick_dates", forbidden)
    monkeypatch.setattr(collector, "build_tasks", forbidden)
    monkeypatch.setattr(collector, "retry_due", lambda *_a, **_k: True)
    activity = SimpleNamespace(exact_dates=lambda _: set(days), provenance={"fixture": True})
    summary = collector._write_summary(
        tmp_path, [contract], chunk_days=29, state="running", usage=(10, 100),
        progress_path=tmp_path / "progress.json", persist=False,
        refresh_empty=True, activity=activity,
    )
    assert [method for method, *_ in calls] == ["kbars", "ticks", "ticks"]
    assert summary["pending_queries"] == 1  # One genuine official-date KBar gap.
    assert summary["partial_kbar_chunks"] == summary["official_activity_missing_kbar_dates"] == 1
    assert summary["tick_dates"] == summary["resolved_tick_dates"] == 2
    assert summary["kbar_rows"] == 3 and summary["tick_rows"] == 14
    assert summary["stored_bytes"] == 300 and summary["traffic_used_bytes"] == 10
    assert summary["coverage_state"] == "source_gaps"


def test_summary_and_dispatch_match_independent_retry_truth_table(tmp_path, monkeypatch):
    contract = row()
    days = [date(2026, 10, 12), date(2026, 10, 13)]
    activity = SimpleNamespace(exact_dates=lambda _: set(days), provenance={})
    for kbar_status, tick_status, refresh, kbars_only, due, unavailable in product(
        (None, "complete", "source_empty", "partial"),
        (None, "complete", "source_empty"), (False, True), (False, True),
        (False, True), (False, True),
    ):
        def receipt(_path, _data_path, *, method, code):
            status = kbar_status if method == "kbars" else tick_status
            if status is None:
                return None
            return {
                "status": "complete" if status == "partial" else status,
                "rows": 3 if status in {"complete", "partial"} else 0,
                "size": 100 if status in {"complete", "partial"} else 0,
                "observed_trading_dates": ([str(days[0])] if status == "partial"
                                           else [str(day) for day in days] if status == "complete"
                                           else []),
                "coverage_retry": {"status": "source_empty"},
            }

        monkeypatch.setattr(collector, "_valid_receipt", receipt)
        monkeypatch.setattr(collector, "_read_receipt", lambda _:
                            {"status": "contract_unavailable"} if unavailable else None)
        monkeypatch.setattr(collector, "retry_due", lambda *_a, **_k: due)
        blocked = unavailable and refresh and not due
        expected_kbar = kbar_status is None or (
            refresh and due and kbar_status in {"source_empty", "partial"}
        )
        expected_tick = not kbars_only and (tick_status is None or (
            refresh and due and tick_status == "source_empty"
        ))
        expected = 0 if blocked else int(expected_kbar) + 2 * int(expected_tick)
        tasks = collector.build_tasks(
            tmp_path, [contract], chunk_days=29, kbars_only=kbars_only,
            refresh_empty=refresh, activity=activity,
        )
        summary = collector._write_summary(
            tmp_path, [contract], chunk_days=29, state="running", usage=None,
            progress_path=tmp_path / "progress.json", persist=False,
            kbars_only=kbars_only, refresh_empty=refresh, activity=activity,
        )
        assert len(tasks) == summary["pending_queries"] == expected
        assert summary["tick_dates"] == (0 if kbars_only else 2)
        assert summary["provider_unavailable_contracts"] == int(unavailable)


def test_summary_kbars_only_does_not_validate_tick_receipts(tmp_path, monkeypatch):
    calls = []

    def receipt(_path, _data_path, *, method, code):
        calls.append(method)
        assert method == "kbars"
        return None

    monkeypatch.setattr(collector, "_valid_receipt", receipt)
    summary = collector._write_summary(
        tmp_path, [row()], chunk_days=29, state="running", usage=None,
        progress_path=tmp_path / "progress_kbars.json", kbars_only=True,
    )
    assert calls == ["kbars"] and summary["pending_queries"] == 1
    assert summary["tick_dates"] == summary["tick_rows"] == 0
    assert not (tmp_path / "summary.json").exists()
    assert (tmp_path / "summary_kbars.json").exists()

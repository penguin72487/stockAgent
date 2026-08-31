from __future__ import annotations

from datetime import datetime
import hashlib
import json
from pathlib import Path
from zoneinfo import ZoneInfo

import polars as pl
import pytest

from downloader.download_tw_public_data import DEFAULT_DATASETS
from scripts.check_tw_day_trade_preopen_readiness import evaluate_readiness
from scripts import run_tw_public_0830_check
from scripts import finalize_tw_public_completed_session as completed_session
from scripts import watch_tw_public_publication_group as publication
from scripts import watch_tw_public_source_events as source_events
from stockagent.live import tw_public_opening_revision as opening_revision


TAIPEI = ZoneInfo("Asia/Taipei")


def test_preopen_sweep_covers_every_registered_official_dataset() -> None:
    phase = publication.PUBLICATION_PHASES["preopen_all"]
    selected = publication._select_specs(list(phase.selectors))
    assert {spec.name for spec in selected} == set(DEFAULT_DATASETS)
    assert len(selected) == 156


def test_auto_phase_accepts_two_minute_retry() -> None:
    observed = datetime(2026, 8, 17, 14, 52, tzinfo=TAIPEI)
    phase = publication.resolve_phase(observed, window_minutes=20.0)
    assert phase.name == "institutional_initial"


def test_auto_phase_catchup_uses_full_sweep() -> None:
    observed = datetime(2026, 8, 17, 12, 0, tzinfo=TAIPEI)
    phase = publication.resolve_phase(observed, window_minutes=20.0)
    assert phase.name == "preopen_all"


def test_close_command_requires_verified_publication(tmp_path: Path) -> None:
    args = type(
        "Args",
        (),
        {"workers": 8, "date_workers": 4, "timeout": 20, "retries": 2},
    )()
    phase = publication.PUBLICATION_PHASES["close_initial"]
    command = publication._download_command(
        live_root=tmp_path,
        metadata_dir=tmp_path / "receipts",
        phase=phase,
        args=args,
    )
    assert "--require-taiex-session-calendar" in command
    assert "--require-daily-close-publication" in command
    assert "--run-metadata-dir" in command
    assert "--no-write-run-metadata" not in command


def test_completed_session_gate_accepts_official_close_without_next_opening(
    tmp_path: Path,
) -> None:
    phase = "close_initial"
    phase_root = tmp_path / phase
    phase_root.mkdir()
    receipt = {
        "status": "ok",
        "phase": phase,
        "started_at_taipei": "2026-08-26T14:02:00+08:00",
        "selected_datasets": [
            "twse_daily_ohlcv",
            "tpex_daily_ohlcv",
        ],
        "download_summary": {
            "end_date": "2026-08-26",
            "daily_close_ready": True,
            "blocking_failed_count": 0,
            "incomplete_count": 0,
            # Initial close may truthfully retain nonblocking publication lag.
            "coverage_complete": False,
            "publication_lag_count": 2,
        },
    }
    (phase_root / "latest.json").write_text(json.dumps(receipt), encoding="utf-8")

    accepted_phase, accepted, failures = (
        completed_session._accepted_close_publication(
            tmp_path,
            expected_date="2026-08-26",
            required_phase=phase,
        )
    )

    assert accepted_phase == phase
    assert accepted == receipt
    assert failures == {}


def test_completed_session_refreshes_all_causal_close_layers(tmp_path: Path) -> None:
    commands = completed_session._build_commands(
        live_root=tmp_path,
        expected_date="2026-08-26",
        workers=4,
    )

    assert [Path(command[1]).name for command in commands] == [
        "download_tw_corporate_action_reference.py",
        "build_tw_official_symbol_parquets.py",
        "download_tw_corporate_action_entitlements.py",
        "build_tw_public_training_features.py",
    ]
    assert all(
        command[command.index("--end-date") + 1] == "2026-08-26"
        for command in commands
    )
    assert all("activate_tw_public_opening_data.py" not in command for command in commands)
    assert all("fetch_tw_day_trade_eligibility_on_publish.py" not in command for command in commands)


def test_close_publication_triggers_completed_session_finalizer(tmp_path: Path) -> None:
    command = publication._completed_session_finalize_command(
        live_root=tmp_path / "live",
        receipt_root=tmp_path / "publications",
        phase=publication.PUBLICATION_PHASES["close_final"],
        expected_date="2026-08-26",
    )

    assert Path(command[1]).name == "finalize_tw_public_completed_session.py"
    assert command[command.index("--publication-phase") + 1] == "close_final"
    assert command[command.index("--expected-date") + 1] == "2026-08-26"


def test_preopen_command_stops_historical_rows_at_completed_session(
    tmp_path: Path,
) -> None:
    args = type(
        "Args",
        (),
        {"workers": 8, "date_workers": 4, "timeout": 20, "retries": 2},
    )()
    command = publication._download_command(
        live_root=tmp_path,
        metadata_dir=tmp_path / "receipts",
        phase=publication.PUBLICATION_PHASES["preopen_all"],
        args=args,
        end_date="2026-08-17",
    )
    end_index = command.index("--end-date")
    assert command[end_index + 1] == "2026-08-17"


def test_snapshot_fingerprint_ignores_download_clock_but_detects_content(
    tmp_path: Path,
) -> None:
    name = "twse_api_exchangereport_mi_margn"
    path = tmp_path / f"{name}.parquet"
    pl.DataFrame(
        {
            "symbol": ["2330", "2317"],
            "balance": ["10", "20"],
            "_downloaded_at_utc": ["2026-08-17T01:00:00Z"] * 2,
        }
    ).write_parquet(path)
    first = publication._file_hashes(tmp_path, [name])[name]["sha256"]
    pl.DataFrame(
        {
            "symbol": ["2317", "2330"],
            "balance": ["20", "10"],
            "_downloaded_at_utc": ["2026-08-17T02:00:00Z"] * 2,
        }
    ).write_parquet(path)
    reordered = publication._file_hashes(tmp_path, [name])[name]["sha256"]
    assert reordered == first

    pl.DataFrame(
        {
            "symbol": ["2317", "2330"],
            "balance": ["21", "10"],
            "_downloaded_at_utc": ["2026-08-17T03:00:00Z"] * 2,
        }
    ).write_parquet(path)
    changed = publication._file_hashes(tmp_path, [name])[name]["sha256"]
    assert changed != first


def test_content_change_comparison_ignores_parquet_serialization_size() -> None:
    before = {
        "dataset": {
            "sha256": "semantic-fingerprint",
            "bytes": 100,
            "fingerprint_kind": "semantic_without_download_clock",
        }
    }
    after = {
        "dataset": {
            "sha256": "semantic-fingerprint",
            "bytes": 120,
            "fingerprint_kind": "semantic_without_download_clock",
        }
    }
    assert publication._changed_files(before, after) == []


def test_0830_command_refreshes_the_live_preopen_source_not_legacy_snapshot(
    tmp_path: Path,
) -> None:
    command = run_tw_public_0830_check.refresh_command(tmp_path / "tw.yaml")
    assert Path(command[1]).name == "watch_tw_public_publication_group.py"
    assert command[-2:] == ["--phase", "preopen_all"]
    assert "refresh_tw_public_live_snapshot.py" not in command
    forced = run_tw_public_0830_check.refresh_command(
        tmp_path / "tw.yaml", force=True
    )
    assert "--auto-window-minutes" in forced


def test_0830_opening_gate_never_publishes_or_materializes_packed_data() -> None:
    source = Path("scripts/run_tw_public_0830_check.py").read_text(encoding="utf-8")
    assert "run_data_cache.sh" not in source
    assert '"packed_publish"' not in source
    assert '"materialize_verify"' not in source
    assert '"authority": "catalog_mutable_live_root"' in source
    assert '"runtime_materialization_required": False' in source
    publication_source = Path(
        "scripts/watch_tw_public_publication_group.py"
    ).read_text(encoding="utf-8")
    assert "strict_packed_publication_deferred_to_0800" not in publication_source
    assert '"opening_materialization_required": False' in publication_source
    assert '"cold_packed_publication_deferred_to_2350": True' in publication_source


def test_0830_runtime_link_selects_live_root_atomically(tmp_path: Path) -> None:
    first = tmp_path / "live-a"
    second = tmp_path / "live-b"
    first.mkdir()
    second.mkdir()
    link = tmp_path / "data_tw_public"

    run_tw_public_0830_check._atomic_symlink(first, link)
    assert link.is_symlink()
    assert link.resolve(strict=True) == first

    run_tw_public_0830_check._atomic_symlink(second, link)
    assert link.is_symlink()
    assert link.resolve(strict=True) == second
    assert not list(tmp_path.glob(".data_tw_public.tmp.*"))


def test_0830_live_runtime_acceptance_does_not_require_packed_release(
    tmp_path: Path,
) -> None:
    live_root = tmp_path / "live"
    live_root.mkdir()
    link = tmp_path / "data_tw_public"
    link.symlink_to(live_root)
    errors = run_tw_public_0830_check._live_runtime_errors(
        live_root=live_root.resolve(),
        active_link=link,
        active_summary={
            "end_date": "2026-08-21",
            "dataset_count": len(DEFAULT_DATASETS),
            "failed_count": 0,
            "blocking_failed_count": 0,
            "publication_lag_count": 0,
            "incomplete_count": 0,
            "missing_dates_after": 0,
            "coverage_complete": True,
            "daily_close_ready": True,
        },
        expected_latest="2026-08-21",
        derived_status={"current": True},
        audit={"model_safe": True},
    )
    assert errors == []


def test_0830_reuses_only_exact_dependency_audit_receipt(tmp_path: Path) -> None:
    summary = tmp_path / "summary.json"
    summary.write_text('{"model_safe": true}\n', encoding="utf-8")
    cache = tmp_path / "latest.json"
    run_tw_public_0830_check._atomic_json(
        cache,
        {
            "status": "ok",
            "expected_latest_date": "2026-08-25",
            "dependency_sha256": "current-dependencies",
            "audit_summary_path": str(summary),
            "audit_summary_sha256": hashlib.sha256(summary.read_bytes()).hexdigest(),
            "model_safe": True,
        },
    )

    reused = run_tw_public_0830_check._reusable_audit(
        cache,
        expected_latest="2026-08-25",
        dependency_sha256="current-dependencies",
    )
    assert reused is not None
    assert reused[0]["model_safe"] is True
    assert (
        run_tw_public_0830_check._reusable_audit(
            cache,
            expected_latest="2026-08-25",
            dependency_sha256="changed-dependencies",
        )
        is None
    )

    summary.write_text('{"model_safe": false}\n', encoding="utf-8")
    assert (
        run_tw_public_0830_check._reusable_audit(
            cache,
            expected_latest="2026-08-25",
            dependency_sha256="current-dependencies",
        )
        is None
    )


def test_0830_final_revision_rejects_event_applied_after_audit() -> None:
    assert run_tw_public_0830_check._audit_revision_errors(
        derived_status={"current": True},
        audited_dependency_state={"sha256": "same"},
        final_dependency_state={"sha256": "same"},
    ) == []
    errors = run_tw_public_0830_check._audit_revision_errors(
        derived_status={"current": False},
        audited_dependency_state={"sha256": "before"},
        final_dependency_state={"sha256": "after"},
    )
    assert "derived data changed or became stale after audit" in errors
    assert (
        "live-root dependency revision changed after audit; retry convergence"
        in errors
    )

def test_0830_builds_canonical_derived_layers_from_accepted_source_date(
    tmp_path: Path,
) -> None:
    commands = run_tw_public_0830_check._derived_data_commands(
        live_root=tmp_path,
        expected_latest="2026-08-21",
        workers=4,
    )

    assert [Path(command[1]).name for command in commands] == [
        "download_tw_corporate_action_reference.py",
        "build_tw_official_symbol_parquets.py",
        "download_tw_corporate_action_entitlements.py",
        "build_tw_public_training_features.py",
    ]
    assert all(command[command.index("--end-date") + 1] == "2026-08-21" for command in commands)
    assert commands[1][commands[1].index("--output-dir") + 1] == str(
        tmp_path / "stocks"
    )
    assert commands[3][commands[3].index("--output-path") + 1] == str(
        tmp_path / "features" / "tw_public_stock_daily.parquet"
    )
    assert "--allow-daily-publication-lag" in commands[1]
    assert "--allow-daily-publication-lag" in commands[3]


def test_0830_detects_same_date_source_content_revision(tmp_path: Path) -> None:
    source = tmp_path / "tdcc_shareholding_distribution.parquet"
    source.write_bytes(b"old")
    summary = tmp_path / "tw_public_stock_daily.summary.json"
    summary.write_text(
        json.dumps(
            {
                "source_receipts": [
                    {
                        "name": source.name,
                        "size": source.stat().st_size,
                        "sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    assert (
        run_tw_public_0830_check._receipt_dependency_errors(
            summary,
            live_root=tmp_path,
            keys=("source_receipts",),
        )
        == []
    )

    source.write_bytes(b"new")
    errors = run_tw_public_0830_check._receipt_dependency_errors(
        summary,
        live_root=tmp_path,
        keys=("source_receipts",),
    )
    assert errors == ["source_receipts: sha256 mismatch tdcc_shareholding_distribution.parquet"]


def test_preopen_full_sweep_requires_zero_lag_before_live_metadata_promotion() -> None:
    accepted = {
        "end_date": "2026-08-20",
        "dataset_count": len(DEFAULT_DATASETS),
        "failed_count": 0,
        "blocking_failed_count": 0,
        "publication_lag_count": 0,
        "incomplete_count": 0,
        "missing_dates_after": 0,
        "coverage_complete": True,
        "daily_close_ready": True,
    }
    assert publication._preopen_acceptance_errors(
        accepted,
        expected_end_date="2026-08-20",
        expected_dataset_count=len(DEFAULT_DATASETS),
    ) == []
    accepted["publication_lag_count"] = 1
    assert publication._preopen_acceptance_errors(
        accepted,
        expected_end_date="2026-08-20",
        expected_dataset_count=len(DEFAULT_DATASETS),
    )


def test_0830_requires_both_exact_session_venues() -> None:
    snapshot = {
        "same_session_eligibility": {
            "trading_date": "2026-08-17",
            "venues": {
                "twse": {"covered": True, "target_date": "2026-08-17"},
                "tpex": {"covered": True, "target_date": "2026-08-17"},
            },
        }
    }
    assert run_tw_public_0830_check._same_session_accepted(
        snapshot,
        trading_date="2026-08-17",
    )
    snapshot["same_session_eligibility"]["venues"]["tpex"]["covered"] = False
    assert not run_tw_public_0830_check._same_session_accepted(
        snapshot,
        trading_date="2026-08-17",
    )


def test_systemd_timers_have_no_random_delay() -> None:
    publication_timer = (
        Path("deploy/systemd/stockagent-tw-public-publication-sweep.timer.in")
        .read_text(encoding="utf-8")
    )
    acceptance_timer = (
        Path("deploy/systemd/stockagent-tw-public-0830-check.timer.in")
        .read_text(encoding="utf-8")
    )
    assert "07:50:00 Asia/Taipei" in publication_timer
    assert "21:00:30 Asia/Taipei" in publication_timer
    assert "21:01:00 Asia/Taipei" in publication_timer
    assert "21:02:00 Asia/Taipei" in publication_timer
    assert "RandomizedDelaySec=0" in publication_timer
    assert "08:00:00 Asia/Taipei" in acceptance_timer
    assert "08:15:00 Asia/Taipei" in acceptance_timer
    assert "08:24:00 Asia/Taipei" in acceptance_timer
    assert "08:29:00 Asia/Taipei" in acceptance_timer
    assert "09:15:00 Asia/Taipei" in acceptance_timer
    assert "08:30:00 Asia/Taipei" not in acceptance_timer
    assert "RandomizedDelaySec=0" in acceptance_timer

    cold_timer = Path(
        "deploy/systemd/stockagent-tw-public-cold-publish.timer.in"
    ).read_text(encoding="utf-8")
    cold_service = Path(
        "deploy/systemd/stockagent-tw-public-cold-publish.service.in"
    ).read_text(encoding="utf-8")
    cold_runner = Path("scripts/run_tw_public_cold_publish.sh").read_text(
        encoding="utf-8"
    )
    assert "Mon..Fri *-*-* 23:50:00 Asia/Taipei" in cold_timer
    assert "RandomizedDelaySec=0" in cold_timer
    assert "Restart=on-failure" in cold_service
    assert "RestartSec=5min" in cold_service
    assert "run_tw_public_cold_publish.sh" in cold_service
    assert "publish_tw_public_cold_release.py" in cold_runner
    assert " use " not in cold_runner


def test_source_event_registry_covers_all_official_datasets() -> None:
    args = type(
        "Args",
        (),
        {
            "fast_interval_seconds": 60.0,
            "medium_interval_seconds": 300.0,
            "slow_interval_seconds": 900.0,
        },
    )()
    digest, rows = source_events._registry(list(DEFAULT_DATASETS.values()), args)
    assert len(digest) == 64
    assert len(rows) == 156
    assert {row["dataset"] for row in rows} == set(DEFAULT_DATASETS)
    assert {row["interval_seconds"] for row in rows} == {60.0, 300.0, 900.0}


def test_source_event_download_retries_are_more_resilient_than_fast_probes() -> None:
    args = type("Args", (), {"timeout": 20, "retries": 2})()

    resolved = source_events._resilient_download_args(args)

    assert resolved.timeout == 60
    assert resolved.retries == 4
    assert args.timeout == 20
    assert args.retries == 2


def test_source_event_tpex_dependents_refresh_their_calendar_baseline_first() -> None:
    selected = source_events._refresh_selector_names(
        ["tpex_margin_balance", "tpex_daily_valuation"]
    )
    assert selected == [
        "tpex_daily_ohlcv",
        "tpex_daily_valuation",
        "tpex_margin_balance",
    ]
    assert source_events._refresh_selector_names(["twse_notice_stock"]) == [
        "twse_notice_stock"
    ]


def test_source_event_failed_download_retry_is_bounded_independently_of_probes() -> None:
    observed = datetime(2026, 8, 25, 8, 0, tzinfo=TAIPEI)
    assert source_events._download_retry_due({}, observed)
    assert source_events._download_retry_due(
        {"next_download_retry_at_taipei": "2026-08-25T07:59:59+08:00"},
        observed,
    )
    assert not source_events._download_retry_due(
        {"next_download_retry_at_taipei": "2026-08-25T08:00:01+08:00"},
        observed,
    )


def test_source_event_json_fingerprint_ignores_row_order() -> None:
    left = b'[{"symbol":"2330","value":1},{"symbol":"2317","value":2}]'
    right = b'[{"value":2,"symbol":"2317"},{"value":1,"symbol":"2330"}]'
    changed = b'[{"symbol":"2330","value":3},{"symbol":"2317","value":2}]'
    assert source_events._body_sha256(left, expected_json=True) == (
        source_events._body_sha256(right, expected_json=True)
    )
    assert source_events._body_sha256(left, expected_json=True) != (
        source_events._body_sha256(changed, expected_json=True)
    )


def test_source_event_accepts_taifex_csv_representation_but_rejects_html() -> None:
    left = "日期,身份別,口數\n20260817,自營商,1\n20260817,外資,2\n".encode()
    reordered = "日期,身份別,口數\n20260817,外資,2\n20260817,自營商,1\n".encode()
    assert source_events._response_body_sha256(
        left,
        expected_json=True,
        content_type="application/octet-stream",
        content_disposition="attachment; filename=table.csv",
    ) == source_events._response_body_sha256(
        reordered,
        expected_json=True,
        content_type="application/octet-stream",
        content_disposition="attachment; filename=table.csv",
    )
    with pytest.raises(json.JSONDecodeError):
        source_events._response_body_sha256(
            b"<html>publisher error</html>",
            expected_json=True,
            content_type="text/html",
            content_disposition=None,
        )


def test_source_event_version_uses_content_not_dynamic_attachment_name() -> None:
    common = {
        "url": "https://official.example/data.csv",
        "body_sha256": "canonical-content",
        "etag": None,
        "last_modified": None,
        "content_length": "42",
    }
    first = source_events._version(
        **common,
        content_disposition='attachment; filename="report_100001.csv"',
    )
    second = source_events._version(
        **common,
        content_disposition='attachment; filename="report_100002.csv"',
    )
    assert first == second == "canonical-content"


def test_source_event_version_migration_preserves_pending_state() -> None:
    state = {
        "datasets": {
            "accepted": {
                "body_sha256": "accepted-body",
                "observed_version": "old-accepted",
                "applied_version": "old-accepted",
            },
            "pending": {
                "body_sha256": "pending-body",
                "observed_version": "old-observed",
                "applied_version": "older-applied",
            },
        }
    }
    source_events._migrate_version_contract(state)
    assert state["version_contract"] == source_events.VERSION_CONTRACT
    assert state["datasets"]["accepted"]["observed_version"] == "accepted-body"
    assert state["datasets"]["accepted"]["applied_version"] == "accepted-body"
    assert state["datasets"]["pending"]["observed_version"] == "pending-body"
    assert state["datasets"]["pending"]["applied_version"] == "older-applied"


def test_source_event_is_unacknowledged_until_download_accepts_it() -> None:
    spec = DEFAULT_DATASETS["twse_notice_stock"]
    args = type(
        "Args",
        (),
        {
            "fast_interval_seconds": 60.0,
            "medium_interval_seconds": 300.0,
            "slow_interval_seconds": 300.0,
        },
    )()
    state = {
        "datasets": {
            spec.name: {
                "observed_version": "old",
                "applied_version": "old",
            }
        }
    }
    changed = source_events._apply_probe_results(
        state,
        [
            source_events.ProbeResult(
                dataset=spec.name,
                status="ok",
                url=str(spec.url),
                checked_at_taipei="2026-08-18T12:00:00+08:00",
                http_status=200,
                version="new",
                body_sha256="body",
            )
        ],
        specs_by_name={spec.name: spec},
        args=args,
    )
    assert changed == [spec.name]
    assert state["datasets"][spec.name]["observed_version"] == "new"
    assert state["datasets"][spec.name]["applied_version"] == "old"
    source_events._summarize_state(state, specs=[spec], changed=changed)
    assert state["status"] == "degraded"
    assert state["unapplied_event_count"] == 1
    assert state["blocking_unapplied_event_count"] == 1


def test_opening_revision_freeze_is_bounded_to_weekday_recovery_edge(
    tmp_path: Path,
) -> None:
    live_root = tmp_path / "data_tw_public"
    live_root.mkdir()
    assert not opening_revision.create_opening_revision_freeze(
        live_root,
        observed=datetime(2026, 8, 28, 8, 19, 59, tzinfo=TAIPEI),
    )
    created = opening_revision.create_opening_revision_freeze(
        live_root,
        observed=datetime(2026, 8, 28, 8, 30, tzinfo=TAIPEI),
        owner={"service": "test"},
    )

    assert created["defer_apply_until_taipei"] == "2026-08-28T09:05:00+08:00"
    assert opening_revision.active_opening_revision_freeze(
        live_root,
        observed=datetime(2026, 8, 28, 9, 4, 59, tzinfo=TAIPEI),
    )
    assert not opening_revision.active_opening_revision_freeze(
        live_root,
        observed=datetime(2026, 8, 28, 9, 5, tzinfo=TAIPEI),
    )
    assert not opening_revision.create_opening_revision_freeze(
        live_root,
        observed=datetime(2026, 8, 29, 8, 30, tzinfo=TAIPEI),
    )


def test_source_event_exposes_but_does_not_block_opening_deferred_event() -> None:
    spec = DEFAULT_DATASETS["twse_notice_stock"]
    state = {
        "datasets": {
            spec.name: {
                "observed_version": "new",
                "applied_version": "old",
            }
        },
        "opening_apply_deferred_until_taipei": "2026-08-28T09:05:00+08:00",
        "opening_apply_deferred_datasets": [spec.name],
    }

    source_events._summarize_state(
        state,
        specs=[spec],
        changed=[spec.name],
        observed=datetime(2026, 8, 28, 8, 59, tzinfo=TAIPEI),
    )
    assert state["status"] == "ok"
    assert state["unapplied_event_count"] == 1
    assert state["blocking_unapplied_event_count"] == 0
    assert state["opening_apply_deferred"] is True

    source_events._summarize_state(
        state,
        specs=[spec],
        changed=[],
        observed=datetime(2026, 8, 28, 9, 5, tzinfo=TAIPEI),
    )
    assert state["status"] == "degraded"
    assert state["blocking_unapplied_event_count"] == 1
    assert state["opening_apply_deferred"] is False


def test_source_event_does_not_mutate_live_root_during_opening_freeze(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    live_root = tmp_path / "data_tw_public"
    live_root.mkdir()
    state_root = tmp_path / "events"
    opening_revision.create_opening_revision_freeze(
        live_root,
        observed=datetime(2026, 8, 28, 8, 40, tzinfo=TAIPEI),
    )
    monkeypatch.setattr(
        source_events,
        "_refresh_pending_serialized",
        lambda *args, **kwargs: pytest.fail("canonical downloader must not run"),
    )
    class FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):  # type: ignore[no-untyped-def]
            value = datetime(2026, 8, 28, 8, 45, tzinfo=TAIPEI)
            return value if tz is None else value.astimezone(tz)

    monkeypatch.setattr(source_events, "datetime", FrozenDateTime)
    result = source_events._refresh_pending(
        ["twse_notice_stock"],
        state={},
        live_root=live_root,
        state_root=state_root,
        specs_by_name={},
        args=object(),
    )

    assert result["status"] == "deferred_opening_revision"
    assert result["triggered_datasets"] == ["twse_notice_stock"]
    assert result["failed_dataset_count"] == 0
    assert (state_root / "events" / "latest.json").is_file()


def test_source_event_accepts_verified_superseded_endpoint_fallback() -> None:
    shadow_name = "tdcc_shareholding_distribution"
    replacement_name = "data_gov_tdcc_shareholding_distribution"
    specs = [DEFAULT_DATASETS[shadow_name], DEFAULT_DATASETS[replacement_name]]
    state = {
        "datasets": {
            shadow_name: {
                "observed_version": "shadow-old",
                "applied_version": "shadow-old",
                "last_probe_status": "failed",
                "last_download_status": "up_to_date",
            },
            replacement_name: {
                "observed_version": "canonical-current",
                "applied_version": "canonical-current",
                "last_probe_status": "ok",
                "last_download_status": "ok",
            },
        }
    }

    source_events._summarize_state(state, specs=specs, changed=[])

    assert state["status"] == "ok"
    assert state["failed_probe_count"] == 0
    assert state["raw_failed_probe_count"] == 1
    assert state["nonblocking_shadow_failed_probe_count"] == 1
    assert state["accepted_source_fallbacks"] == {
        shadow_name: replacement_name,
    }


@pytest.mark.parametrize(
    ("replacement_probe", "replacement_applied", "replacement_download"),
    [
        ("failed", "canonical-current", "failed"),
        ("ok", "canonical-old", "ok"),
    ],
)
def test_source_event_rejects_unhealthy_superseded_endpoint_fallback(
    replacement_probe: str,
    replacement_applied: str,
    replacement_download: str,
) -> None:
    shadow_name = "tdcc_shareholding_distribution"
    replacement_name = "data_gov_tdcc_shareholding_distribution"
    specs = [DEFAULT_DATASETS[shadow_name], DEFAULT_DATASETS[replacement_name]]
    state = {
        "datasets": {
            shadow_name: {
                "observed_version": "shadow-old",
                "applied_version": "shadow-old",
                "last_probe_status": "failed",
            },
            replacement_name: {
                "observed_version": "canonical-current",
                "applied_version": replacement_applied,
                "last_probe_status": replacement_probe,
                "last_download_status": replacement_download,
            },
        }
    }

    source_events._summarize_state(state, specs=specs, changed=[])

    assert state["status"] == "degraded"
    assert shadow_name in state["failed_probe_datasets"]
    assert state["nonblocking_shadow_failed_probe_count"] == 0


def test_source_event_calendar_failure_is_persisted_without_crashing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    spec = next(
        item
        for item in DEFAULT_DATASETS.values()
        if item.kind == "historical_json_table"
    )
    state = {
        "datasets": {
            spec.name: {
                "observed_version": "new",
                "applied_version": "old",
            }
        }
    }
    args = type("Args", (), {})()
    class MorningDatetime(datetime):
        @classmethod
        def now(cls, tz=None):  # type: ignore[no-untyped-def]
            return cls(2026, 8, 20, 9, 30, tzinfo=tz)

    monkeypatch.setattr(source_events, "datetime", MorningDatetime)
    monkeypatch.setattr(
        source_events,
        "_latest_completed_taiex_session",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("timeout")),
    )
    monkeypatch.setattr(
        source_events,
        "TAIPEI",
        ZoneInfo("Asia/Taipei"),
    )

    result = source_events._refresh_pending(
        [spec.name],
        state=state,
        live_root=tmp_path / "live",
        state_root=tmp_path / "state",
        specs_by_name={spec.name: spec},
        args=args,
    )

    assert result["status"] == "failed"
    assert result["calendar_status"] == "unavailable"
    assert result["failed_dataset_count"] == 1
    assert state["datasets"][spec.name]["last_download_status"] == (
        "blocked_taiex_session_calendar"
    )
    assert (tmp_path / "state" / "events" / "latest.json").is_file()


def test_source_event_service_is_persistent_and_restarting() -> None:
    service = Path(
        "deploy/systemd/stockagent-tw-public-source-events.service.in"
    ).read_text(encoding="utf-8")
    assert "Type=notify" in service
    assert "NotifyAccess=main" in service
    assert "WatchdogSec=5min" in service
    assert "Restart=always" in service
    assert "watch_tw_public_source_events" not in service
    assert "run_tw_public_source_event_monitor.sh" in service


def test_0830_acceptance_uses_bounded_timer_retries() -> None:
    service = (
        Path("deploy/systemd/stockagent-tw-public-0830-check.service.in")
        .read_text(encoding="utf-8")
    )
    timer = (
        Path("deploy/systemd/stockagent-tw-public-0830-check.timer.in")
        .read_text(encoding="utf-8")
    )
    assert "Restart=on-failure" in service
    assert "RestartSec=30s" in service
    assert "StartLimitIntervalSec=5min" in service
    assert "StartLimitBurst=2" in service
    assert "08:00:00 Asia/Taipei" in timer
    assert "08:15:00 Asia/Taipei" in timer
    assert "08:20:00 Asia/Taipei" in timer
    assert "08:24:00 Asia/Taipei" in timer
    assert "08:29:00 Asia/Taipei" in timer
    assert "09:15:00 Asia/Taipei" in timer
    assert "ExecStartPost" not in service


def test_0830_derived_warmup_does_not_wait_for_same_session_eligibility() -> None:
    assert run_tw_public_0830_check._derived_refresh_blockers(
        [
            "eligibility:both exact-session venues are not covered",
            "eligibility:publication watcher receipt is not current",
        ]
    ) == []
    assert run_tw_public_0830_check._derived_refresh_blockers(
        [
            "eligibility:publication watcher receipt is not current",
            "publication:preopen publication is not current",
        ]
    ) == ["publication:preopen publication is not current"]


def test_eligibility_timer_has_weekday_boot_and_opening_catchups() -> None:
    timer = Path(
        "deploy/systemd/stockagent-tw-day-trade-eligibility.timer.in"
    ).read_text(encoding="utf-8")

    for wall_time in (
        "22:29:55",
        "05:30:00",
        "07:30:00",
        "08:00:00",
        "08:50:00",
        "09:00:05",
        "09:15:00",
    ):
        assert f"Mon..Fri *-*-* {wall_time} Asia/Taipei" in timer
    assert "Persistent=true" in timer
    assert "RandomizedDelaySec=0" in timer


def test_final_preopen_gate_requires_public_model_and_executor_proofs() -> None:
    observed = datetime(2026, 8, 17, 8, 59, tzinfo=TAIPEI)
    public = {
        "status": "ok",
        "started_at_taipei": "2026-08-17T08:20:00+08:00",
        "completed_at_taipei": "2026-08-17T08:40:00+08:00",
        "acceptance": {
            "subprocess_ok": True,
            "live_root_receipt_fresh": True,
            "live_root_status_ok": True,
            "live_root_verified": True,
            "coverage_complete": True,
            "same_session_eligibility": True,
            "runtime_materialization_required": False,
            "runtime_materialized_snapshot": False,
        },
    }
    model = {
        "run_id": "discord-process-1",
        "markets": {
            "tw_day_trade": {
                "status": "ready",
                "completed_at": "2026-08-17T08:50:00+08:00",
                "final_arm": {
                    "status": "ready",
                    "run_id": "discord-process-1",
                    "completed_at": "2026-08-17T08:55:00+08:00",
                    "live_latency": {
                        "panel_cache_hit": True,
                        "checkpoint_cache_hit": True,
                        "model_cache_hit": True,
                    },
                    "opening_source_prewarm": {
                        "ready": True,
                        "run_id": "discord-process-1",
                        "source": "twse_tpex:mis",
                    },
                },
            }
        }
    }
    simulation = {
        "status": "ready",
        "session_date": "2026-08-17",
        "updated_at": "2026-08-17T08:55:01+08:00",
        "components": {
            "eligibility": {
                "status": "ready",
                "checked_at": "2026-08-17T08:30:00+08:00",
            },
            "shioaji_quote": {
                "status": "ready",
                "checked_at": "2026-08-17T08:55:01+08:00",
            },
        },
    }
    services = {
        "stockagent-discord-bot.service": "active",
        "stockagent-tw-day-trade-simulation.service": "active",
    }
    ready = evaluate_readiness(
        observed=observed,
        strict_after=datetime(2026, 8, 17, 8, 57, tzinfo=TAIPEI).time(),
        market_names=("tw_day_trade",),
        public_receipt=public,
        model_receipt=model,
        simulation_receipt=simulation,
        service_states=services,
    )
    assert ready["status"] == "ready"
    simulation["components"]["shioaji_quote"]["status"] = "failed"
    failed = evaluate_readiness(
        observed=observed,
        strict_after=datetime(2026, 8, 17, 8, 57, tzinfo=TAIPEI).time(),
        market_names=("tw_day_trade",),
        public_receipt=public,
        model_receipt=model,
        simulation_receipt=simulation,
        service_states=services,
    )
    assert failed["status"] == "failed"
    assert failed["ready"] is False


def test_final_preopen_gate_rejects_legacy_materialized_snapshot_receipt() -> None:
    result = evaluate_readiness(
        observed=datetime(2026, 8, 17, 8, 59, tzinfo=TAIPEI),
        strict_after=datetime(2026, 8, 17, 8, 57, tzinfo=TAIPEI).time(),
        market_names=(),
        public_receipt={
            "status": "ok",
            "started_at_taipei": "2026-08-17T08:20:00+08:00",
            "acceptance": {
                "subprocess_ok": True,
                "snapshot_receipt_fresh": True,
                "snapshot_status_ok": True,
                "coverage_complete": True,
                "same_session_eligibility": True,
                "packed_release_verified": True,
            },
        },
        model_receipt={},
        simulation_receipt={},
        service_states={},
    )
    assert result["public_data"]["ready"] is False
    assert "08:30 public-data acceptance receipt is not ready" in result["failures"]


def test_final_preopen_gate_rejects_unapplied_public_source_event() -> None:
    observed = datetime(2026, 8, 17, 8, 59, tzinfo=TAIPEI)
    event = {
        "status": "ok",
        "coverage_complete": True,
        "registered_dataset_count": 156,
        "monitored_dataset_count": 156,
        "observed_dataset_count": 156,
        "failed_probe_count": 0,
        "unapplied_event_count": 1,
        "updated_at_taipei": "2026-08-17T08:58:30+08:00",
    }
    result = evaluate_readiness(
        observed=observed,
        strict_after=datetime(2026, 8, 17, 8, 57, tzinfo=TAIPEI).time(),
        market_names=(),
        public_receipt={},
        model_receipt={},
        simulation_receipt={},
        service_states={
            "stockagent-discord-bot.service": "active",
            "stockagent-tw-day-trade-simulation.service": "active",
            "stockagent-tw-public-source-events.service": "active",
        },
        event_receipt=event,
    )
    assert result["source_event_monitor"]["ready"] is False
    assert "156-dataset source-event monitor is not healthy" in result["failures"]

    event["blocking_unapplied_event_count"] = 0
    event["opening_apply_deferred"] = True
    event["opening_apply_deferred_count"] = 1
    deferred = evaluate_readiness(
        observed=observed,
        strict_after=datetime(2026, 8, 17, 8, 57, tzinfo=TAIPEI).time(),
        market_names=(),
        public_receipt={},
        model_receipt={},
        simulation_receipt={},
        service_states={
            "stockagent-discord-bot.service": "active",
            "stockagent-tw-day-trade-simulation.service": "active",
            "stockagent-tw-public-source-events.service": "active",
        },
        event_receipt=event,
    )
    assert deferred["source_event_monitor"]["ready"] is True
    assert deferred["source_event_monitor"]["unapplied_event_count"] == 1
    assert deferred["source_event_monitor"]["blocking_unapplied_event_count"] == 0

    event.pop("blocking_unapplied_event_count")
    event["unapplied_event_count"] = 0
    healthy = evaluate_readiness(
        observed=observed,
        strict_after=datetime(2026, 8, 17, 8, 57, tzinfo=TAIPEI).time(),
        market_names=(),
        public_receipt={},
        model_receipt={},
        simulation_receipt={},
        service_states={
            "stockagent-discord-bot.service": "active",
            "stockagent-tw-day-trade-simulation.service": "active",
            "stockagent-tw-public-source-events.service": "active",
        },
        event_receipt=event,
    )
    assert healthy["source_event_monitor"]["ready"] is True


def test_final_preopen_gate_requires_durable_engine_and_exact_discord_revision() -> None:
    observed = datetime(2026, 8, 17, 8, 59, tzinfo=TAIPEI)
    market = "tw_day_trade"
    engine_status = {
        "updated_at": "2026-08-17T08:58:59+08:00",
        "health": "waiting",
        "simulation_only": True,
        "production_order_possible": False,
        "ledger_integrity": {"ready": True, "divergence_count": 0},
        "modes": {
            market: {
                "engine_status": "waiting_09_00_signal",
                "checkpoint_ready": True,
                "readiness_error": None,
            }
        },
    }
    engine_sync = {
        "published_at": "2026-08-17T08:58:59+08:00",
        "engine_run_id": "engine-run",
        "state_revision": 42,
        "simulation_only": True,
        "production_order_possible": False,
        "ledger_integrity_ready": True,
        "enabled_markets": [market],
    }
    discord_status = {
        "updated_at": "2026-08-17T08:58:59+08:00",
        "engine_run_id": "engine-run",
        "engine_state_revision": 42,
        "simulation_only": True,
        "production_order_possible": False,
        "discord_connected": True,
        "day_trade_markets": [market],
    }

    result = evaluate_readiness(
        observed=observed,
        strict_after=datetime(2026, 8, 17, 8, 57, tzinfo=TAIPEI).time(),
        market_names=(market,),
        public_receipt={},
        model_receipt={},
        simulation_receipt={},
        service_states={},
        engine_status_receipt=engine_status,
        engine_sync_receipt=engine_sync,
        discord_status_receipt=discord_status,
    )
    assert result["engine_runtime"]["ready"] is True
    assert result["runtime_sync"]["ready"] is True

    discord_status["engine_state_revision"] = 41
    failed = evaluate_readiness(
        observed=observed,
        strict_after=datetime(2026, 8, 17, 8, 57, tzinfo=TAIPEI).time(),
        market_names=(market,),
        public_receipt={},
        model_receipt={},
        simulation_receipt={},
        service_states={},
        engine_status_receipt=engine_status,
        engine_sync_receipt=engine_sync,
        discord_status_receipt=discord_status,
    )
    assert failed["runtime_sync"]["ready"] is False
    assert "paper engine/Discord revision is not synchronized" in failed["failures"]


def test_post_open_gate_requires_same_session_signal_commit_for_every_mode() -> None:
    observed = datetime(2026, 8, 17, 9, 0, 15, tzinfo=TAIPEI)
    market = "tw_day_trade"
    engine_sync = {
        "modes": {
            market: {
                "session_date": "2026-08-17",
                "signal_id": "signal-1",
                "signal_at": "2026-08-17T09:00:00.100+08:00",
                "entry_completed_at": "2026-08-17T09:00:02+08:00",
                "engine_status": "active",
                "checkpoint_ready": True,
                "entry_fill_policy": "causal_best_quote",
                "entry_price_offset_ticks": 0,
            }
        }
    }
    result = evaluate_readiness(
        observed=observed,
        strict_after=datetime(2026, 8, 17, 8, 57, tzinfo=TAIPEI).time(),
        market_names=(market,),
        public_receipt={},
        model_receipt={},
        simulation_receipt={},
        service_states={},
        engine_sync_receipt=engine_sync,
    )
    assert result["opening_execution"]["ready"] is True
    assert result["opening_execution"]["modes"][market]["commit_slo_met"] is True

    engine_sync["modes"][market]["entry_completed_at"] = None
    failed = evaluate_readiness(
        observed=observed,
        strict_after=datetime(2026, 8, 17, 8, 57, tzinfo=TAIPEI).time(),
        market_names=(market,),
        public_receipt={},
        model_receipt={},
        simulation_receipt={},
        service_states={},
        engine_sync_receipt=engine_sync,
    )
    assert failed["opening_execution"]["ready"] is False
    assert (
        "09:00 live signals were not durably committed with causal best-quote execution for every paper mode by 09:00:15"
        in failed["failures"]
    )

    engine_sync["modes"][market]["entry_completed_at"] = (
        "2026-08-17T09:00:16+08:00"
    )
    late = evaluate_readiness(
        observed=observed,
        strict_after=datetime(2026, 8, 17, 8, 57, tzinfo=TAIPEI).time(),
        market_names=(market,),
        public_receipt={},
        model_receipt={},
        simulation_receipt={},
        service_states={},
        engine_sync_receipt=engine_sync,
    )
    assert late["opening_execution"]["ready"] is False
    assert late["opening_execution"]["modes"][market]["commit_slo_met"] is False


def test_final_preopen_gate_timer_checks_until_085930() -> None:
    timer = Path(
        "deploy/systemd/stockagent-tw-day-trade-preopen-gate.timer.in"
    ).read_text(encoding="utf-8")
    assert "08:50:00 Asia/Taipei" in timer
    assert "08:56:00 Asia/Taipei" in timer
    assert "08:58:00 Asia/Taipei" in timer
    assert "08:59:30 Asia/Taipei" in timer
    assert "09:00:15 Asia/Taipei" in timer
    assert "09:00:30 Asia/Taipei" in timer
    assert "09:01:00 Asia/Taipei" in timer
    assert "09:01:15 Asia/Taipei" in timer
    assert "09:02:00 Asia/Taipei" in timer
    assert "09:03:00 Asia/Taipei" in timer
    assert "09:05:00 Asia/Taipei" in timer
    assert "09:10:00 Asia/Taipei" in timer
    assert "09:15:00 Asia/Taipei" in timer
    assert "RandomizedDelaySec=0" in timer


def test_day_trade_critical_services_have_fast_restart_and_watchdogs() -> None:
    for name in (
        "stockagent-discord-bot.service.in",
        "stockagent-tw-day-trade-simulation.service.in",
    ):
        service = (Path("deploy/systemd") / name).read_text(encoding="utf-8")
        assert "Type=notify" in service
        assert "NotifyAccess=all" in service
        assert "RestartSec=1s" in service
        assert "StartLimitBurst=20" in service

    discord = Path(
        "deploy/systemd/stockagent-discord-bot.service.in"
    ).read_text(encoding="utf-8")
    assert "WatchdogSec=90s" in discord

    executor = Path(
        "deploy/systemd/stockagent-tw-day-trade-simulation.service.in"
    ).read_text(encoding="utf-8")
    assert "Restart=always" in executor
    assert "WatchdogSec=20s" in executor
    assert "Nice=-5" in executor


def test_eligibility_watcher_retries_delayed_official_publication() -> None:
    service = (
        Path("deploy/systemd/stockagent-tw-day-trade-eligibility.service.in")
        .read_text(encoding="utf-8")
    )
    assert "Restart=on-failure" in service
    assert "RestartSec=5min" in service
    assert "StartLimitIntervalSec=0" in service


def test_service_wrappers_exec_resolved_python_not_shell_function() -> None:
    for name in (
        "run_tw_day_trade_eligibility_watcher.sh",
        "run_tw_public_publication_sweep.sh",
        "run_tw_public_0830_check.sh",
        "run_tw_public_source_event_monitor.sh",
    ):
        body = (Path("scripts") / name).read_text(encoding="utf-8")
        assert 'python_bin="$(resolve_fintech_python)"' in body
        assert "exec run_fintech_python" not in body

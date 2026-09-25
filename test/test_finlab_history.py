from __future__ import annotations

from datetime import UTC, datetime, timedelta
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from download_finlab_history import (
    AUTOMATICALLY_DEFERRED_KEYS,
    EmptyProviderFrame, attempt_retry_at, audit_local, classify_provider_error,
    credential_available, fetch_one, has_local_download,
    general_work_status, load_catalog, main, quota_cycle_start, refresh_due, safe_stem,
    serialize_provider_frame, unavailable_attempt,
    record_attempt, record_timed_out_sync, sync_catalog, sync_selection,
)
from stockagent.live.data_monitor_dashboard import (
    _automation_for_row, _finlab_acquisition_status, _finlab_candidate_sources,
    _finlab_receipt_file_exists, _operation_state, _service_state,
    _systemd_monotonic_time,
)


class FinLabHistoryTest(unittest.TestCase):
    def test_tick_gate_requires_fresh_discovery_and_no_actionable_general_work(self):
        now = datetime(2026, 9, 25, 3, tzinfo=UTC)
        discovery = {"observed_at_utc": now.isoformat(), "keys": ["price:收盤價"]}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            pending = general_work_status(discovery, {}, root, now=now, refresh_days=1)
            self.assertEqual(pending["state"], "general_work_pending")
            self.assertEqual(pending["actionable_pending"], 1)
            dataset = root / "datasets/price.parquet"
            dataset.parent.mkdir()
            dataset.write_bytes(b"stored")
            receipt = root / "receipts" / f"{safe_stem('price:收盤價')}.json"
            receipt.parent.mkdir()
            receipt.write_text(json.dumps({
                "dataset": "price:收盤價", "status": "downloaded_unverified_for_pit",
                "parquet_path": "datasets/price.parquet",
                "source_checked_at_utc": now.isoformat(),
            }))
            idle = general_work_status(discovery, {}, root, now=now, refresh_days=1)
            self.assertEqual(idle["state"], "general_work_idle")
            self.assertEqual(idle["actionable_pending"], 0)
            stale = general_work_status(discovery, {}, root,
                                        now=now + timedelta(hours=5), refresh_days=1)
            self.assertEqual(stale["state"], "discovery_unverified")
            blocked = general_work_status(
                {"observed_at_utc": now.isoformat(), "keys": ["broker_transactions"]},
                {}, root, now=now, refresh_days=1,
            )
            self.assertEqual(blocked["state"], "general_work_idle")
            self.assertEqual(blocked["missing_receipts"], 1)
            self.assertEqual(blocked["deferred_keys"], 1)

    def test_pending_cli_uses_local_discovery_without_provider_query(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch("download_finlab_history.provider_catalog", side_effect=AssertionError("network")):
                self.assertEqual(main(["pending", "--output-root", str(root)]), 2)
            path = root / "catalog/discovery.json"
            path.parent.mkdir()
            path.write_text(json.dumps({
                "observed_at_utc": datetime.now(UTC).isoformat(),
                "keys": ["price:收盤價"],
            }))
            with patch("download_finlab_history.provider_catalog", side_effect=AssertionError("network")):
                self.assertEqual(main(["pending", "--output-root", str(root)]), 0)

    def test_daily_refresh_follows_0800_quota_day_including_holiday(self):
        key = "price:收盤價"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data_path = root / "datasets/price.parquet"
            data_path.parent.mkdir()
            data_path.write_bytes(b"existing data")
            receipt_path = root / "receipts" / f"{safe_stem(key)}.json"
            receipt_path.parent.mkdir()
            receipt_path.write_text(json.dumps({
                "dataset": key, "status": "downloaded_unverified_for_pit",
                "parquet_path": "datasets/price.parquet",
                "source_checked_at_utc": "2026-09-24T15:00:00+00:00",
            }))
            before = datetime(2026, 9, 24, 23, 59, tzinfo=UTC)
            after = datetime(2026, 9, 25, 0, 1, tzinfo=UTC)
            assert quota_cycle_start(before).isoformat() == "2026-09-24T00:00:00+00:00"
            assert quota_cycle_start(after).isoformat() == "2026-09-25T00:00:00+00:00"
            assert not refresh_due(key, root, now=before, days=1)
            assert refresh_due(key, root, now=after, days=1)
            assert sync_selection([key], {}, root, now=after, refresh_days=1,
                                  retry_unavailable=False) == [key]

    def test_failure_can_retry_once_quota_resets(self):
        key = "provider:field"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "attempts" / f"{safe_stem(key)}.json"
            path.parent.mkdir()
            path.write_text(json.dumps({
                "dataset": key, "status": "provider_empty", "failure_streak": 4,
                "attempted_at_utc": "2026-09-24T23:55:00+00:00",
            }))
            before = datetime(2026, 9, 24, 23, 59, tzinfo=UTC)
            after = datetime(2026, 9, 25, 0, 1, tzinfo=UTC)
            assert sync_selection([key], {}, root, now=before, refresh_days=1,
                                  retry_unavailable=False) == []
            assert sync_selection([key], {}, root, now=after, refresh_days=1,
                                  retry_unavailable=False) == [key]

    def test_account_reserve_is_explicit_not_implicit_ten_percent(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with (patch("download_finlab_history.credential_available", return_value=True),
                  patch("download_finlab_history.sync_selection", return_value=["small:key"]),
                  patch("download_finlab_history.quota_room_mb", return_value=(440.0, 5000.0)),
                  patch("download_finlab_history.fetch_one", return_value={
                      "last_check_result": "downloaded", "rows_with_values": 1,
                  }) as fetch):
                summary = sync_catalog(
                    ["small:key"], {}, root, limit=1, refresh_days=1,
                    min_quota_remaining_mb=50.0, retry_unavailable=False,
                )
                fetch.assert_called_once()
            self.assertEqual(summary["state"], "pass_complete")
            self.assertEqual(summary["quota_reserve_mb"], 50.0)
            with (patch("download_finlab_history.credential_available", return_value=True),
                  patch("download_finlab_history.sync_selection", return_value=["small:key"]),
                  patch("download_finlab_history.quota_room_mb", return_value=(50.0, 5000.0)),
                  patch("download_finlab_history.fetch_one") as fetch):
                summary = sync_catalog(
                    ["small:key"], {}, root, limit=1, refresh_days=1,
                    min_quota_remaining_mb=50.0, retry_unavailable=False,
                )
                fetch.assert_not_called()
            self.assertEqual(summary["state"], "quota_margin_reached")

    def test_bounded_sync_timeout_preserves_partial_and_skips_only_exact_key(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run_path = root / "runs/latest.json"
            run_path.parent.mkdir(parents=True)
            run_path.write_text(json.dumps({
                "state": "running", "attempt_id": "attempt-1",
                "active_key": "slow:key",
                "active_started_at_utc": datetime.now(UTC).isoformat(),
                "pending_before": 2, "attempted": 1, "provider_errors": 0,
            }))
            with self.assertRaisesRegex(ValueError, "matching in-flight"):
                record_timed_out_sync(root, attempt_id="different", timeout_seconds=600)
            self.assertFalse((root / "attempts").exists())

            result = record_timed_out_sync(
                root, attempt_id="attempt-1", timeout_seconds=600,
            )
            self.assertEqual(result["state"], "partial")
            self.assertEqual(result["provider_errors"], 1)
            self.assertEqual(result["timed_out"], 1)
            self.assertEqual(result["pending_after_estimate"], 1)
            self.assertNotIn("active_key", result)
            attempt = json.loads(
                (root / "attempts" / f"{safe_stem('slow:key')}.json").read_text()
            )
            self.assertEqual(attempt["status"], "timed_out")
            self.assertEqual(attempt["timeout_seconds"], 600)
            self.assertFalse(attempt["message_retained"])
            self.assertEqual(
                sync_selection(
                    ["slow:key", "next:key"], {}, root,
                    now=datetime.now(UTC), refresh_days=1,
                    retry_unavailable=False,
                ),
                ["next:key"],
            )
            self.assertEqual(
                record_timed_out_sync(
                    root, attempt_id="attempt-1", timeout_seconds=600,
                ),
                result,
            )

    def test_finlab_wrapper_has_bounded_sdk_timeout_and_exact_recovery(self):
        subprocess.run(
            ["bash", "-n", "scripts/run_finlab_refresh.sh"], check=True,
        )
        subprocess.run(
            ["bash", "-n", "scripts/run_finlab_refresh_frozen.sh"], check=True,
        )
        wrapper = Path("scripts/run_finlab_refresh.sh").read_text()
        self.assertIn("FINLAB_SYNC_KEY_TIMEOUT_SECONDS:-600", wrapper)
        self.assertIn("timeout --signal=TERM --kill-after=30s", wrapper)
        self.assertIn("--attempt-id \"$finlab_attempt_id\"", wrapper)
        self.assertIn("record-timeout", wrapper)
        self.assertIn("FINLAB_SYNC_MAX_TIMEOUTS:-2", wrapper)
        self.assertIn("finlab_consecutive_timeouts=0", wrapper)
        self.assertIn("finlab_consecutive_timeouts=$((finlab_consecutive_timeouts + 1))", wrapper)
        self.assertIn("if (( finlab_consecutive_timeouts >= finlab_max_timeouts )); then", wrapper)
        self.assertNotIn("finlab_timeout_count", wrapper)
        self.assertIn("--confirm-daily-reset --wait-seconds 120", wrapper)
        self.assertIn("finlab_preopen_deadline", wrapper)
        self.assertIn("finlab_runway_seconds <= finlab_key_timeout_seconds + 30", wrapper)
        self.assertLess(wrapper.index('download_finlab_history.py" sync'),
                        wrapper.index('download_finlab_market_intraday.py'))
        self.assertLess(wrapper.index('finlab_release_gate.py'),
                        wrapper.index('download_finlab_market_intraday.py'))
        self.assertIn('if [[ "$finlab_state" == "pass_complete" ]]; then', wrapper)
        self.assertIn('download_finlab_history.py" pending', wrapper)
        self.assertIn('dotenv_values(".env").get("FINLAB_TICK_QUOTA_RESERVE_MB")', wrapper)
        self.assertIn("FINLAB_REPO_ROOT:-", wrapper)
        frozen = Path("scripts/run_finlab_refresh_frozen.sh").read_text()
        self.assertIn('cp -- "$finlab_source_script" "$finlab_frozen_script"', frozen)
        self.assertIn('cmp -s -- "$finlab_source_script" "$finlab_frozen_script"', frozen)
        self.assertIn('bash -n "$finlab_frozen_script"', frozen)
        self.assertIn('FINLAB_REPO_ROOT="$finlab_repo_root" bash "$finlab_frozen_script"', frozen)
        service = Path("deploy/systemd/stockagent-finlab-local-refresh.service.in").read_text()
        self.assertIn('ExecStartPre=/usr/bin/bash -n "@REPO_ROOT@/scripts/run_finlab_refresh_frozen.sh"', service)
        self.assertIn('ExecStart=/usr/bin/bash "@REPO_ROOT@/scripts/run_finlab_refresh_frozen.sh"', service)
        installer = Path("scripts/install_registered_data_refresh_services.sh").read_text()
        self.assertIn('bash -n "$repo_root/scripts/run_finlab_refresh.sh"', installer)
        self.assertIn('bash -n "$repo_root/scripts/run_finlab_refresh_frozen.sh"', installer)
        timer = Path("deploy/systemd/stockagent-finlab-local-refresh.timer.in").read_text()
        self.assertIn("08:00:05 Asia/Taipei", timer)
        self.assertIn("09:10:05 Asia/Taipei", timer)
        quota_timer = Path("deploy/systemd/stockagent-finlab-quota-snapshot.timer.in").read_text()
        self.assertIn("OnCalendar=*-*-* *:*:00 Asia/Taipei", quota_timer)
        self.assertNotIn("OnUnitActiveSec=5min", quota_timer)

    def test_timeout_recovery_cli_does_not_query_provider(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run_path = root / "runs/latest.json"
            run_path.parent.mkdir(parents=True)
            run_path.write_text(json.dumps({
                "state": "running", "attempt_id": "cli-attempt",
                "active_key": "slow:key",
                "active_started_at_utc": datetime.now(UTC).isoformat(),
                "pending_before": 1, "attempted": 1,
            }))
            with patch("download_finlab_history.provider_catalog", side_effect=AssertionError("network")):
                result = main([
                    "record-timeout", "--output-root", str(root),
                    "--attempt-id", "cli-attempt", "--timeout-seconds", "600",
                ])
            self.assertEqual(result, 0)
            self.assertEqual(json.loads(run_path.read_text())["state"], "partial")

    def test_oversized_broker_table_is_visible_but_not_auto_retried(self):
        self.assertIn("broker_transactions", AUTOMATICALLY_DEFERRED_KEYS)
        with tempfile.TemporaryDirectory() as directory:
            pending = sync_selection(
                ["broker_transactions", "small:key"], {}, Path(directory),
                now=datetime(2026, 9, 23, 12, tzinfo=UTC),
                refresh_days=1, retry_unavailable=False,
            )
        self.assertEqual(pending, ["small:key"])

    def test_all_candidates_are_distinct(self):
        catalog = load_catalog()
        keys = [entry["key"] for entry in catalog["datasets"]]
        self.assertEqual(len(keys), len(set(keys)))
        self.assertGreaterEqual(len(keys), 15)

    def test_period_label_is_not_publication_date(self):
        source = pd.DataFrame(
            {"2330": [None, 42.0], "2317": [None, None]},
            index=pd.Index(["2014-M01", "2014-M02"]),
        )
        table, stats = serialize_provider_frame(source)
        self.assertEqual(table["source_index"].tolist(), ["2014-M01", "2014-M02"])
        self.assertEqual(stats["rows_with_values"], 1)
        self.assertEqual(stats["first_non_null_source_index"], "2014-M02")
        self.assertEqual(stats["index_semantics"], "period_label_not_publication_date")

    def test_timestamp_remains_source_time_unverified(self):
        source = pd.DataFrame({"value": [1.0]}, index=pd.DatetimeIndex([datetime(2020, 1, 2)]))
        _, stats = serialize_provider_frame(source)
        self.assertEqual(stats["index_semantics"], "source_timestamp_not_verified_publication_date")

    def test_event_table_uses_date_not_ordinal_index_or_key_date(self):
        source = pd.DataFrame({
            "date": ["2001-01-02", "2018-12-28"],
            "key_date": ["2023-01-01", "2023-01-01"],
            "stock_id": ["2330", "2317"],
        })
        table, stats = serialize_provider_frame(source)
        self.assertEqual(table["source_index"].tolist(), ["0", "1"])
        self.assertEqual(stats["first_event_at"], "2001-01-02T00:00:00")
        self.assertEqual(stats["last_event_at"], "2018-12-28T00:00:00")
        self.assertEqual(stats["event_time_column"], "date")

    def test_filename_is_safe_and_stable(self):
        stem = safe_stem("../../trading_attention:測試")
        self.assertNotIn("/", stem)
        self.assertEqual(stem, safe_stem("../../trading_attention:測試"))
        self.assertNotEqual(stem, safe_stem("other:測試"))

    def test_credential_gate_uses_sdk_session_not_file_existence(self):
        with patch("dotenv.load_dotenv", return_value=False), patch(
            "finlab.auth.get_session", return_value=None
        ), patch.dict("os.environ", {}, clear=True):
            self.assertFalse(credential_available())
        complete = {"refresh_token": "r", "session_id": "s", "api_key": "k"}
        with patch("dotenv.load_dotenv", return_value=False), patch(
            "finlab.auth.get_session", return_value=complete
        ), patch.dict("os.environ", {}, clear=True):
            self.assertTrue(credential_available())

    def test_monitor_shows_each_candidate_without_claiming_download(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "configs").mkdir()
            (root / "configs/finlab_history_candidates.json").write_text(
                json.dumps({"datasets": [{"key": "monthly_revenue:當月營收", "group": "monthly_revenue"}]}),
                encoding="utf-8",
            )
            rows = _finlab_candidate_sources(root, now=datetime.now())
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["status"], "waiting")
            self.assertIsNone(rows[0]["record_stats"]["count"])
            self.assertTrue(rows[0]["automation_eligible"])
            self.assertFalse(rows[0]["acquisition_enabled"])
            source = pd.DataFrame({"2330": [100.0]}, index=pd.Index(["2014-M01"]))
            with patch("finlab.data.get", return_value=source):
                fetch_one("monthly_revenue:當月營收", root / "data_finlab")
            downloaded = _finlab_candidate_sources(root, now=datetime.now())
            self.assertEqual(downloaded[0]["record_stats"]["count"], 1)
            self.assertEqual(downloaded[0]["record_stats"]["first"], "2014-M01")
            self.assertEqual(downloaded[0]["status"], "legacy")
            with patch("finlab.data.get", return_value=source):
                fetch_one("extra:field", root / "data_finlab")
            expanded = _finlab_candidate_sources(root, now=datetime.now())
            self.assertEqual({row["title"] for row in expanded},
                             {"monthly_revenue:當月營收", "extra:field"})

    def test_finlab_receipt_file_fast_path_preserves_symlink_containment(self):
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory)
            root = workspace / "data_finlab"
            datasets = root / "datasets"
            datasets.mkdir(parents=True)
            (datasets / "price.parquet").write_bytes(b"stored")
            with patch.object(Path, "resolve", side_effect=AssertionError("slow path")):
                self.assertTrue(_finlab_receipt_file_exists(root, "datasets/price.parquet"))
                self.assertFalse(_finlab_receipt_file_exists(root, "datasets/missing.parquet"))
            (datasets / "inside.parquet").symlink_to("price.parquet")
            self.assertTrue(_finlab_receipt_file_exists(root, "datasets/inside.parquet"))
            outside = workspace / "outside.parquet"
            outside.write_bytes(b"outside")
            (datasets / "outside.parquet").symlink_to(outside)
            self.assertFalse(_finlab_receipt_file_exists(root, "datasets/outside.parquet"))
            self.assertFalse(_finlab_receipt_file_exists(root, "datasets/../../outside.parquet"))
            self.assertFalse(_finlab_receipt_file_exists(root, str(outside)))
            linked_root = workspace / "linked_finlab"
            (linked_root / "real").mkdir(parents=True)
            (linked_root / "real/inside.parquet").write_bytes(b"inside")
            (linked_root / "datasets").symlink_to("real", target_is_directory=True)
            self.assertTrue(_finlab_receipt_file_exists(linked_root, "datasets/inside.parquet"))
            external_root = workspace / "external_finlab"
            external_root.mkdir()
            (external_root / "datasets").symlink_to(datasets, target_is_directory=True)
            self.assertFalse(_finlab_receipt_file_exists(external_root, "datasets/price.parquet"))

    def test_vip_attempt_is_visible_but_not_claimed_as_download(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "configs").mkdir()
            key = "cb_price:收盤價"
            (root / "configs/finlab_history_candidates.json").write_text(
                json.dumps({"datasets": [{"key": key, "group": "convertible_bonds"}]}),
                encoding="utf-8",
            )
            attempts = root / "data_finlab/attempts"
            attempts.mkdir(parents=True)
            (attempts / f"{safe_stem(key)}.json").write_text(
                json.dumps({"dataset": key, "status": "vip_only"}), encoding="utf-8",
            )
            self.assertTrue(unavailable_attempt(key, root / "data_finlab"))
            self.assertFalse(has_local_download(key, root / "data_finlab"))
            row = _finlab_candidate_sources(root, now=datetime.now())[0]
            self.assertEqual(row["record_stats"]["state"], "vip_only")
            self.assertIsNone(row["record_stats"]["count"])
            self.assertEqual(row["status"], "blocked")

    def test_discovered_catalog_progress_counts_receipts_not_key_names(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "configs").mkdir()
            (root / "configs/finlab_history_candidates.json").write_text(
                json.dumps({"datasets": [{"key": "monthly_revenue:當月營收", "group": "monthly"}]}),
                encoding="utf-8",
            )
            catalog = root / "data_finlab/catalog/discovery.json"
            catalog.parent.mkdir(parents=True)
            catalog.write_text(json.dumps({
                "keys": ["monthly_revenue:當月營收", "extra:field", "after_market_fixed_price:資料來源"],
                "observed_at_utc": "2026-09-23T12:00:00+00:00",
            }), encoding="utf-8")
            run = root / "data_finlab/runs/latest.json"
            run.parent.mkdir(parents=True)
            run.write_text(json.dumps({
                "finished_at_utc": "2026-09-23T12:01:00+00:00",
                "quota_remaining_mb": 1000, "quota_limit_mb": 5000,
            }), encoding="utf-8")
            with patch("finlab.data.get", return_value=pd.DataFrame(
                {"2330": [100.0]}, index=pd.Index(["2014-M01"])
            )):
                fetch_one("monthly_revenue:當月營收", root / "data_finlab")
            now = datetime(2026, 9, 23, 12, 2, tzinfo=UTC)
            rows = _finlab_candidate_sources(root, now=now)
            assert len(rows) == 3
            by_key = {row["title"]: row for row in rows}
            assert by_key["extra:field"]["finlab_acquisition_state"] == "pending"
            assert by_key["after_market_fixed_price:資料來源"]["finlab_acquisition_state"] == "deferred_resource"
            assert all(row["registry_alias"] for row in rows)
            summary = _finlab_acquisition_status(
                root, rows, now=now,
                service={"active": True, "timer_active": True,
                         "next_run_at_utc": "2026-09-24T08:10:00+00:00"},
            )
            assert summary["state"] == "running"
            assert summary["catalog_total"] == 3
            assert summary["downloaded"] == 1
            assert summary["not_downloaded"] == 2
            assert summary["ratio"] == 1 / 3
            assert summary["deferred_resource"] == 1
            assert summary["quota_remaining_mb"] == 1000
            assert summary["cold_publish_configured"] is False
            assert summary["eta"]["remaining_seconds"] is None
            run.write_text(json.dumps({
                "finished_at_utc": "2026-09-23T12:01:00+00:00",
                "quota_remaining_mb": 440, "quota_limit_mb": 5000,
                "quota_reserve_mb": 50,
            }), encoding="utf-8")
            idle_service = {"active": False, "timer_active": True, "result": "success"}
            resumable = _finlab_acquisition_status(root, rows, now=now, service=idle_service)
            assert resumable["state"] == "scheduled"
            assert resumable["quota_reserve_mb"] == 50
            run.write_text(json.dumps({
                "finished_at_utc": "2026-09-23T12:01:00+00:00",
                "quota_remaining_mb": 50, "quota_limit_mb": 5000,
                "quota_reserve_mb": 50,
            }), encoding="utf-8")
            stopped = _finlab_acquisition_status(root, rows, now=now, service=idle_service)
            assert stopped["state"] == "waiting_quota"

    def test_finlab_group_automation_requires_its_own_timer(self):
        row = {"id": "group:finlab-research", "status": "waiting",
               "automation_eligible": True,
               "coverage": {"current": 1, "total": 3, "ratio": 1 / 3},
               "eta": {"state": "waiting_schedule"}}
        now = datetime(2026, 9, 23, 12, tzinfo=UTC)
        unavailable = _automation_for_row(row, now=now, refresh_services={
            "finlab_local": {"active": False, "timer_active": False},
        })
        assert unavailable["automatic_update"] is False
        active = _automation_for_row(row, now=now, refresh_services={
            "finlab_local": {"active": True, "timer_active": True},
        })
        assert active["automatic_update"] is True
        assert _operation_state(row, active)[0] == "catching_up"

    def test_provider_error_does_not_persist_secret_message(self):
        self.assertEqual(classify_provider_error(RuntimeError("data is only for VIP; token=SECRET")), "vip_only")
        self.assertEqual(classify_provider_error(RuntimeError("quota exceeded; token=SECRET")), "quota_exhausted")

    def test_fetch_writes_local_parquet_and_receipt_without_period_relabel(self):
        source = pd.DataFrame({"2330": [100.0]}, index=pd.Index(["2014-M01"]))
        with tempfile.TemporaryDirectory() as directory, patch("finlab.data.get", return_value=source):
            root = Path(directory)
            receipt = fetch_one("monthly_revenue:當月營收", root)
            self.assertEqual(receipt["rows_with_values"], 1)
            self.assertEqual(receipt["first_non_null_source_index"], "2014-M01")
            self.assertEqual(receipt["publication_time_status"], "not_verified")
            self.assertGreaterEqual(receipt["last_fetch_elapsed_seconds"], 0)
            self.assertGreater(receipt["parquet_size_bytes"], 0)
            output = pd.read_parquet(root / receipt["parquet_path"])
            self.assertEqual(output["source_index"].tolist(), ["2014-M01"])
            self.assertTrue((root / "receipts" / f"{safe_stem('monthly_revenue:當月營收')}.json").is_file())
            self.assertEqual(audit_local(root), (1, 0))

    def test_fetch_is_idempotent_and_preserves_prior_versions(self):
        key = "monthly_revenue:當月營收"
        source = pd.DataFrame({"2330": [100.0]}, index=pd.Index(["2014-M01"]))
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch("finlab.data.get", return_value=source):
                first = fetch_one(key, root)
                second = fetch_one(key, root)
            self.assertEqual(first["sha256"], second["sha256"])
            self.assertEqual(second["last_check_result"], "unchanged")
            self.assertEqual(len(list((root / "datasets").glob("*.parquet"))), 1)
            updated = pd.DataFrame({"2330": [101.0]}, index=pd.Index(["2014-M01"]))
            with patch("finlab.data.get", return_value=updated):
                third = fetch_one(key, root)
            self.assertNotEqual(third["sha256"], first["sha256"])
            self.assertTrue((root / first["parquet_path"]).is_file())
            self.assertTrue((root / "versions" / safe_stem(key) / f"{first['sha256']}.json").is_file())
            self.assertEqual(audit_local(root), (1, 0))

    def test_sync_selection_prioritizes_missing_curated_and_skips_vip(self):
        now = datetime.now(UTC)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            attempts = root / "attempts"
            attempts.mkdir()
            vip = "vip:field"
            (attempts / f"{safe_stem(vip)}.json").write_text(
                json.dumps({"dataset": vip, "status": "vip_only", "attempted_at_utc": now.isoformat()}),
                encoding="utf-8",
            )
            error = "error:field"
            (attempts / f"{safe_stem(error)}.json").write_text(
                json.dumps({"dataset": error, "status": "provider_error", "attempted_at_utc": now.isoformat()}),
                encoding="utf-8",
            )
            available = ["later:field", "missing:field", vip, error]
            curated = {"missing:field": {}, vip: {}}
            self.assertEqual(
                sync_selection(available, curated, root, now=now, refresh_days=30, retry_unavailable=False),
                ["missing:field", "later:field"],
            )

    def test_sync_selection_defers_known_unbounded_non_feature_metadata(self):
        with tempfile.TemporaryDirectory() as directory:
            self.assertEqual(
                sync_selection(
                    ["after_market_fixed_price:資料來源", "after_market_fixed_price:成交價"],
                    {}, Path(directory), now=datetime.now(UTC), refresh_days=1,
                    retry_unavailable=False,
                ),
                ["after_market_fixed_price:成交價"],
            )

    def test_quota_limited_refresh_visits_oldest_extra_before_alphabetical_prefix(self):
        now = datetime(2026, 9, 25, 1, tzinfo=UTC)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "datasets").mkdir()
            (root / "receipts").mkdir()
            keys = ["a:newer", "z:older", "m:missing", "b:curated"]
            for key, checked in (
                ("a:newer", "2026-09-24T16:00:00+00:00"),
                ("z:older", "2026-09-23T16:00:00+00:00"),
                ("b:curated", "2026-09-24T16:00:00+00:00"),
            ):
                path = root / "datasets" / f"{safe_stem(key)}.parquet"
                path.write_bytes(b"existing")
                (root / "receipts" / f"{safe_stem(key)}.json").write_text(
                    json.dumps({
                        "dataset": key,
                        "status": "downloaded_unverified_for_pit",
                        "parquet_path": str(path.relative_to(root)),
                        "source_checked_at_utc": checked,
                    })
                )
            assert sync_selection(
                keys, {"b:curated": {}}, root, now=now,
                refresh_days=1, retry_unavailable=False,
            ) == ["b:curated", "m:missing", "z:older", "a:newer"]

    def test_downloaded_official_price_overlap_refresh_uses_surplus_quota(self):
        now = datetime(2026, 9, 25, 1, tzinfo=UTC)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "datasets").mkdir()
            (root / "receipts").mkdir()
            key = "price:收盤價"
            path = root / "datasets" / f"{safe_stem(key)}.parquet"
            path.write_bytes(b"existing")
            (root / "receipts" / f"{safe_stem(key)}.json").write_text(json.dumps({
                "dataset": key, "status": "downloaded_unverified_for_pit",
                "parquet_path": str(path.relative_to(root)),
                "source_checked_at_utc": "2026-09-23T00:00:00+00:00",
            }))
            assert sync_selection(
                [key, "missing:feature"], {key: {}}, root,
                now=now, refresh_days=1, retry_unavailable=False,
            ) == ["missing:feature", key]

    def test_failure_cooldowns_are_reason_specific_and_intraday_needs_dates(self):
        now = datetime(2026, 9, 24, 15, tzinfo=UTC)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            attempts = root / "attempts"
            attempts.mkdir()
            statuses = {
                "provider:field": "provider_error",
                "timeout:field": "timed_out",
                "vip:field": "vip_only",
                "auth:field": "authentication_failed",
                "quota:field": "quota_exhausted",
            }
            for key, status in statuses.items():
                (attempts / f"{safe_stem(key)}.json").write_text(json.dumps({
                    "dataset": key, "status": status,
                    "attempted_at_utc": now.isoformat(),
                }), encoding="utf-8")
            keys = [*statuses, "tw_minute:2330", "tw_tick:2330"]
            assert sync_selection(keys, {}, root, now=now, refresh_days=1,
                                  retry_unavailable=False) == ["auth:field", "quota:field"]
            soon = now + timedelta(minutes=5, seconds=1)
            assert sync_selection(keys, {}, root, now=soon, refresh_days=1,
                                  retry_unavailable=False) == [
                "provider:field", "auth:field", "quota:field",
            ]
            later = now + timedelta(minutes=30, seconds=1)
            assert sync_selection(keys, {}, root, now=later, refresh_days=1,
                                  retry_unavailable=False) == [
                "provider:field", "timeout:field", "vip:field", "auth:field", "quota:field",
            ]
            assert sync_selection(keys, {}, root, now=now, refresh_days=1,
                                  retry_unavailable=True) == [
                "vip:field", "auth:field", "quota:field",
            ]
            assert attempt_retry_at({"status": "timed_out", "attempted_at_utc": now.isoformat()}) == now + timedelta(minutes=30)
            assert attempt_retry_at({"status": "timed_out", "attempted_at_utc": now.isoformat()}, downloaded=True) == now + timedelta(minutes=15)
            assert attempt_retry_at({"status": "timed_out", "attempted_at_utc": now.isoformat(), "failure_streak": 4}) == now + timedelta(hours=2)
            assert attempt_retry_at({"status": "authentication_failed", "attempted_at_utc": now.isoformat()}) is None

    def test_downloaded_key_respects_vip_response_backoff(self):
        now = datetime(2026, 9, 24, 16, tzinfo=UTC)
        key = "ordinary:source"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            stem = safe_stem(key)
            data_path = root / "datasets" / "old.parquet"
            data_path.parent.mkdir()
            data_path.write_bytes(b"preserved historical payload")
            receipt_path = root / "receipts" / f"{stem}.json"
            receipt_path.parent.mkdir()
            receipt_path.write_text(json.dumps({
                "dataset": key, "status": "downloaded_unverified_for_pit",
                "parquet_path": "datasets/old.parquet",
                "fetched_at_utc": (now - timedelta(days=2)).isoformat(),
                "source_checked_at_utc": (now - timedelta(days=2)).isoformat(),
            }), encoding="utf-8")
            attempt_path = root / "attempts" / f"{stem}.json"
            attempt_path.parent.mkdir()
            attempt_path.write_text(json.dumps({
                "dataset": key, "status": "vip_only", "attempted_at_utc": now.isoformat(),
            }), encoding="utf-8")
            assert sync_selection([key], {}, root, now=now, refresh_days=1,
                                  retry_unavailable=False) == []
            assert sync_selection([key], {}, root, now=now + timedelta(minutes=31),
                                  refresh_days=1, retry_unavailable=False) == [key]
            assert "after_market_fixed_price:市場別" in AUTOMATICALLY_DEFERRED_KEYS

    def test_next_run_uses_earlier_monotonic_retry_timer(self):
        with patch("stockagent.live.data_monitor_dashboard.clock.monotonic", return_value=1000.0):
            assert _systemd_monotonic_time("1150s") is not None
            assert _systemd_monotonic_time("infinity") is None
            assert _systemd_monotonic_time("malformed") is None
            tomorrow_local = datetime.now(UTC) + timedelta(days=1, hours=8)
            state = _service_state(
                "stockagent-finlab-local-refresh.service",
                "stockagent-finlab-local-refresh.timer",
                property_sets={
                    "stockagent-finlab-local-refresh.service": {"ActiveState": "inactive"},
                    "stockagent-finlab-local-refresh.timer": {
                        "ActiveState": "active",
                        "NextElapseUSecRealtime": f"Fri {tomorrow_local:%Y-%m-%d %H:%M:%S} CST",
                        "NextElapseUSecMonotonic": "1150s",
                    },
                },
            )
            next_run = datetime.fromisoformat(state["next_run_at_utc"].replace("Z", "+00:00"))
            assert 140 <= (next_run - datetime.now(UTC)).total_seconds() <= 160

    def test_repeated_failure_backoff_resets_after_success(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            key = "broken:key"
            assert record_attempt(key, root, ValueError("provider payload not usable")) == "provider_error"
            attempt_path = root / "attempts" / f"{safe_stem(key)}.json"
            assert json.loads(attempt_path.read_text())["failure_streak"] == 1
            record_attempt(key, root, ValueError("provider payload not usable"))
            second = json.loads(attempt_path.read_text())
            assert second["failure_streak"] == 2
            attempted = datetime.fromisoformat(second["attempted_at_utc"])
            assert attempt_retry_at(second) == attempted + timedelta(minutes=10)
            receipt_path = root / "receipts" / f"{safe_stem(key)}.json"
            receipt_path.parent.mkdir()
            receipt_path.write_text(json.dumps({
                "dataset": key, "fetched_at_utc": (attempted + timedelta(seconds=1)).isoformat(),
                "source_checked_at_utc": (attempted + timedelta(seconds=1)).isoformat(),
            }), encoding="utf-8")
            record_attempt(key, root, ValueError("provider payload not usable"))
            assert json.loads(attempt_path.read_text())["failure_streak"] == 1

    def test_empty_source_is_not_misreported_as_transport_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            key = "dividend_otc:權息"
            error = EmptyProviderFrame(rows=12, fields=3)
            assert classify_provider_error(error) == "provider_empty"
            assert record_attempt(key, root, error) == "provider_empty"
            attempt = json.loads((root / "attempts" / f"{safe_stem(key)}.json").read_text())
            assert attempt["provider_rows"] == 12
            assert attempt["provider_fields"] == 3
            assert attempt["message_retained"] is False
            assert classify_provider_error(RuntimeError(
                "**Error: account access unavailable. Please upgrade to VIP plan."
            )) == "vip_only"

    def test_monitor_reconciles_every_non_downloaded_reason(self):
        now = datetime(2026, 9, 24, 15, tzinfo=UTC)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            keys = [
                "after_market_fixed_price:資料來源", "broker_transactions",
                "dividend_otc:權息", "inventory",
                "management_change_events:變更交易開始日", "rotc_broker_transactions",
                "tw_minute:2330", "tw_tick:2330",
            ]
            discovery = root / "data_finlab/catalog/discovery.json"
            discovery.parent.mkdir(parents=True)
            discovery.write_text(json.dumps({"keys": keys}), encoding="utf-8")
            attempts = root / "data_finlab/attempts"
            attempts.mkdir()
            for key, status in zip(keys[2:6], ["provider_error", "timed_out", "provider_error", "timed_out"]):
                (attempts / f"{safe_stem(key)}.json").write_text(json.dumps({
                    "dataset": key, "status": status,
                    "attempted_at_utc": now.isoformat(),
                    "exception_class": "ValueError", "message_retained": False,
                }), encoding="utf-8")
            rows = _finlab_candidate_sources(root, now=now)
            by_key = {row["title"]: row for row in rows}
            assert by_key["inventory"]["finlab_acquisition_state"] == "resource_timeout"
            assert by_key["tw_minute:2330"]["finlab_acquisition_state"] == "deferred_windowed"
            assert by_key["tw_tick:2330"]["finlab_deferred_reason"] == "requires_date_window"
            assert datetime.fromisoformat(
                by_key["inventory"]["finlab_next_retry_at_utc"].replace("Z", "+00:00")
            ) == now + timedelta(minutes=30)
            summary = _finlab_acquisition_status(root, rows, now=now, service={})
            assert summary["catalog_total"] == 8
            assert summary["not_downloaded"] == 8
            assert summary["not_downloaded_by_reason"] == {
                "pending": 0, "deferred_resource": 2, "deferred_windowed": 2,
                "partial_windowed": 0,
                "partial_windowed": 0,
                "provider_error": 2, "provider_empty": 0,
                "resource_timeout": 2, "vip_only": 0,
                "authentication_failed": 0, "quota_wait": 0,
            }

    def test_monitor_shows_intraday_partitions_without_claiming_full_key(self):
        now = datetime(2026, 9, 25, 1, tzinfo=UTC)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            discovery = root / "data_finlab/catalog/discovery.json"
            discovery.parent.mkdir(parents=True)
            discovery.write_text(json.dumps({"keys": ["tw_minute:2330"]}))
            status = root / "data_finlab/intraday/status.json"
            status.parent.mkdir(parents=True)
            status.write_text(json.dumps({"by_key": {"tw_minute:2330": {
                "requested_weekday_partitions": 84, "receipted_partitions": 2,
                "rows": 532, "parquet_bytes": 26032,
                "first_data_date": "2026-06-01", "last_data_date": "2026-09-24",
            }}}))
            rows = _finlab_candidate_sources(root, now=now)
            row = next(item for item in rows if item["title"] == "tw_minute:2330")
            assert row["finlab_acquisition_state"] == "partial_windowed"
            assert row["record_stats"]["count"] == 532
            assert row["coverage"]["current"] == 2
            summary = _finlab_acquisition_status(root, rows, now=now, service={})
            assert summary["downloaded"] == 0
            assert summary["not_downloaded_by_reason"]["partial_windowed"] == 1


if __name__ == "__main__":
    unittest.main()

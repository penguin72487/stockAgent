from datetime import datetime, timedelta
import json
from types import SimpleNamespace

import pytest

from scripts.audit_tw_opening_latency import opening_coverage_milestones, opening_publication_slo


def event(provider, observed_elapsed_ms, observations=(), **overrides):
    start = datetime.fromisoformat("2026-10-13T09:00:00+08:00")
    return {"session_date": "2026-10-13", "provider": provider,
        "observed_at_taipei": (start + timedelta(milliseconds=observed_elapsed_ms)).isoformat(),
        "event": "observations_delta", "observations": list(observations), **overrides}


def write(path, rows):
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n")


def test_daily_audit_keeps_mis_failure_phase_and_unsent_scope_not_private_requests(tmp_path):
    path = tmp_path / "events.jsonl"
    timing = {"mis_failure_phase_counts": {"connect_tls_or_response_headers": 4},
        "mis_dispatch_stopped": True, "mis_dispatch_stop_reason": "consecutive_transport_failures",
        "mis_dispatch_policy": "healthy_parallel_transport_failure_drain_then_probe_v1",
        "mis_deferred_channel_count": 1981, "mis_request_count": 4,
        "mis_http_workers": 4, "mis_requests": [{"private": "must not copy"}]}
    write(path, [event("official_mis", 4200, event="finished", state="failed", transport_timing=timing)])
    result = opening_coverage_milestones(path, "2026-10-13", ["A"])
    kept = result["provider_latency"]["providers"]["official_mis"]["jobs_first_30s"][0]["transport_timing"]
    assert kept["mis_failure_phase_counts"] == {"connect_tls_or_response_headers": 4}
    assert kept["mis_deferred_channel_count"] == 1981 and kept["mis_dispatch_stopped"]
    assert kept["mis_dispatch_stop_reason"] == "consecutive_transport_failures"
    assert kept["mis_dispatch_policy"] == timing["mis_dispatch_policy"]
    assert "mis_requests" not in kept and "must not copy" not in str(result)


def test_asof_counts_keep_scope_unknown_and_no_first_print_distinct(tmp_path):
    path = tmp_path / "events.jsonl"
    write(path, [event("mis", 0, event="started"), event("shioaji_shared", 0, event="started"),
        event("mis", 75, [{"symbol": "A", "state": "observed_no_first_print"}]),
        event("shioaji_shared", 250, [{"symbol": "A", "state": "valid_open"}]),
        event("mis", 501, [{"symbol": "B", "state": "valid_open"},
                            {"symbol": "X", "state": "valid_open"}]),
        event("mis", 600, [{"symbol": "B", "state": "no_source_observation"}]),
        event("finmind_snapshot", 10000, [{"symbol": "C", "state": "valid_open"}]),
        event("mis", 11000, event="finished", state="failed")])
    before = path.read_bytes()
    result = opening_coverage_milestones(path, "2026-10-13", ["A", "B", "C"])
    assert result["scope_basis"] == "canonical_receipt_posthoc_axis_not_backdated_registry"
    assert result["measurement_boundary"] == "source_validation_event_clock_not_durable_quorum_or_broker_fill"
    assert result["failed_or_degraded_jobs"] == {"mis": 1}
    samples = {row["after_open_ms"]: row for row in result["milestones"]}
    assert (samples[100]["valid_open_count"], samples[100]["observed_no_print_count"], samples[100]["unknown_count"]) == (0, 1, 2)
    assert (samples[250]["valid_open_count"], samples[250]["observed_no_print_count"], samples[250]["unknown_count"]) == (1, 0, 2)
    assert samples[500]["valid_open_count"] == 1
    assert samples[1000]["valid_open_count"] == samples[5000]["valid_open_count"] == 2
    assert samples[5000]["unknown_count"] == 1
    assert samples[5000]["providers"]["finmind_snapshot"]["valid_open_count"] == 0
    assert samples[10000]["valid_open_count"] == samples[20000]["valid_open_count"] == samples[30000]["valid_open_count"] == 3
    assert samples[30000]["unknown_count"] == 0
    assert path.read_bytes() == before


@pytest.mark.parametrize("bad", [None, {}, "broken", {"session_date": "2026-10-13"}])
def test_malformed_evidence_remains_visible_as_degraded(tmp_path, bad):
    path = tmp_path / "events.jsonl"
    write(path, [bad, event("mis", 100, [{"symbol": "A", "state": "valid_open"}])])
    result = opening_coverage_milestones(path, "2026-10-13", ["A", "B"])
    assert result["state"] == "degraded"
    assert result["malformed_lines"] == 1
    assert result["milestones"][0]["unknown_count"] == 1


def test_preopen_other_day_and_naive_time_cannot_count_as_today(tmp_path):
    path = tmp_path / "events.jsonl"
    observation = [{"symbol": "A", "state": "valid_open"}]
    write(path, [event("mis", -1, observation), event("mis", 0, observation, session_date="2026-10-12"),
                event("mis", 1, observation, observed_at_taipei="2026-10-13T09:00:00.001")])
    result = opening_coverage_milestones(path, "2026-10-13", ["A"])
    assert all(row["unknown_count"] == 1 for row in result["milestones"])
    assert result["malformed_lines"] == 1


def test_half_open_health_probe_stays_partial_and_separate_from_full_sweep():
    import hashlib
    from scripts.audit_tw_opening_latency import provider_phase_latency
    gate = datetime.fromisoformat("2026-10-21T09:00:00+08:00")
    events = [(600., {"provider": "official_mis", "owner_pid": 1, "job_id": "probe:1",
        "event": "started", "requested_count": 1,
        "requested_scope_sha256": hashlib.sha256(b"A").hexdigest()}),
        (610., {"provider": "official_mis", "owner_pid": 1, "job_id": "probe:1", "event": "finished",
            "state": "complete", "transport_recovery_probe": True, "transport_health_recovered": True})]
    report = provider_phase_latency(events, ["A", "B"], gate)
    jobs = report["phases"]["opening"]["official_mis"]
    assert jobs["full_scope_jobs"] == 0
    job = jobs["jobs"][0]
    assert job["transport_recovery_probe"] and job["transport_health_recovered"]


def test_missing_evidence_is_unavailable_not_zero_latency(tmp_path):
    result = opening_coverage_milestones(tmp_path / "missing", "2026-10-13", ["A"])
    assert result["state"] == "unavailable"
    assert "milestones" not in result


def test_legacy_aggregate_without_ids_is_not_reported_as_zero_quotes(tmp_path):
    path = tmp_path / "events.jsonl"
    write(path, [event("shioaji_shared", 1100, event="first_batch", requested_count=2,
                     response_count=2, valid_open_count=1)])
    result = opening_coverage_milestones(path, "2026-10-13", ["A", "B"])
    assert result["state"] == "insufficient_evidence"
    assert not result["per_symbol_schema_usable"]
    assert all(row["valid_open_count"] is None and row["unknown_count"] is None for row in result["milestones"])
    assert result["legacy_aggregate_events"][0]["valid_open_count"] == 1


@pytest.mark.parametrize("symbols", [[], ["A", "A"]])
def test_audit_rejects_invalid_scope(tmp_path, symbols):
    with pytest.raises(ValueError, match="nonempty unique"):
        opening_coverage_milestones(tmp_path / "missing", "2026-10-13", symbols)


def full_snapshot(elapsed, valid, rejected=None, **overrides):
    gate = datetime.fromisoformat("2026-10-13T09:00:00+08:00")
    stamp = (gate + timedelta(milliseconds=elapsed - 1)).isoformat()
    coverage = {"contract": "finmind_unfiltered_market_snapshot_coverage_v1",
        "snapshot_id": str(elapsed), "valid_open_symbols": valid,
        "rejected_symbols": rejected or {}, "received_at_taipei": stamp,
        "validated_at_taipei": stamp, **overrides}
    return event("finmind_snapshot", elapsed, event="provider_snapshot", coverage=coverage)


def test_finmind_first_response_first_valid_and_full_scope_are_separate(tmp_path):
    path = tmp_path / "events.jsonl"
    write(path, [event("finmind_snapshot", 200, event="started", job_id="one"),
        full_snapshot(1000, [], {"other_session_rows": ["A", "B"]}),
        # Another provider's success cannot make FinMind complete.
        event("shioaji_shared", 1100, [{"symbol": "B", "state": "valid_open"}]),
        full_snapshot(2000, ["A", "outside"], {"other_session_rows": ["B"]}),
        # Deliberately out of append order: clock-order, not file-order, wins.
        full_snapshot(12000, ["A", "B", "outside"]), full_snapshot(5000, ["A"])])
    result = opening_coverage_milestones(path, "2026-10-13", ["A", "B"])
    p = result["provider_latency"]["providers"]["finmind_snapshot"]
    assert p["first_request_ms"] == 200
    assert p["first_valid_open_ms"] == 2000
    assert p["first_retained_payload_received_ms"] == 999
    assert p["first_retained_validated_open_ms"] == 1999
    assert p["first_full_positive_scope_observed_ms"] == 12000
    assert p["first_full_validated_payload_ms"] == 11999
    assert p["scope_evidence"] == "full_market_payload"
    assert p["latest_full_scope_snapshot"]["missing_count"] == 0
    milestones = {row["after_open_ms"]: row for row in result["milestones"]}
    assert milestones[1000]["providers"]["finmind_snapshot"]["valid_open_count"] == 0
    assert milestones[11000]["providers"]["finmind_snapshot"]["valid_open_count"] == 1
    assert milestones[20000]["providers"]["finmind_snapshot"]["valid_open_count"] == 2


def test_finmind_scope_keeps_old_missing_and_zero_volume_unknown_not_no_print(tmp_path):
    path = tmp_path / "events.jsonl"
    write(path, [full_snapshot(1000, ["A"], {"other_session_rows": ["B"],
                                           "no_positive_volume_rows": ["C"]})])
    p = opening_coverage_milestones(path, "2026-10-13", list("ABCD"))["provider_latency"]["providers"]["finmind_snapshot"]
    assert p["observed_no_print_count"] == 0 and p["unknown_count"] == 3
    assert p["first_full_positive_scope_observed_ms"] is None
    assert p["latest_full_scope_snapshot"]["missing_symbols_by_reason"] == {
        "other_session_rows": ["B"], "no_positive_volume_rows": ["C"], "absent_from_payload": ["D"]}


def test_old_consumer_delta_is_not_claimed_to_be_full_provider_coverage(tmp_path):
    path = tmp_path / "events.jsonl"
    write(path, [event("finmind_snapshot", 1000, [{"symbol": "A", "state": "valid_open"}])])
    p = opening_coverage_milestones(path, "2026-10-13", ["A", "B"])["provider_latency"]["providers"]["finmind_snapshot"]
    assert p["scope_evidence"] == "consumer_deltas_only_lower_bound"
    assert p["first_valid_open_ms"] == 1000
    assert p["first_full_validated_payload_ms"] is None


@pytest.mark.parametrize("overrides", [
    {"snapshot_id": None}, {"valid_open_symbols": [None]}, {"rejected_symbols": {"bad": None}},
    {"contract": "unknown_contract"},
    {"received_at_taipei": "2026-10-13T08:59:59+08:00"},
    {"validated_at_taipei": "2026-10-13T09:00:02+08:00"},
    {"validated_at_taipei": "2026-10-13T09:00:00.100"},
])
def test_invalid_full_source_evidence_is_degraded_not_counted(tmp_path, overrides):
    path = tmp_path / "events.jsonl"
    write(path, [full_snapshot(1000, ["A"], **overrides)])
    result = opening_coverage_milestones(path, "2026-10-13", ["A"])
    assert result["state"] == "degraded" and result["malformed_lines"] == 1
    assert all(row["unknown_count"] == 1 for row in result["milestones"])


def test_same_full_response_reused_by_another_scope_is_not_another_http_request(tmp_path):
    path = tmp_path / "events.jsonl"
    original = full_snapshot(1000, ["A"])
    duplicate = {**original, "parquet_root": "another_root",
        "observed_at_taipei": "2026-10-13T09:00:02+08:00"}
    write(path, [original, duplicate])
    p = opening_coverage_milestones(path, "2026-10-13", ["A"])["provider_latency"]["providers"]["finmind_snapshot"]
    assert p["full_scope_snapshot_count"] == p["retained_http_request_count"] == 1
    assert p["first_full_positive_scope_observed_ms"] == 1000


def publication(market_name, elapsed_ms, **overrides):
    gate = datetime.fromisoformat("2026-10-14T09:00:00+08:00")
    return {"status": "ready", "session_date": "2026-10-14", "market": market_name,
        "signal_id": market_name + "-immutable", "gate_at": gate.isoformat(),
        "signal_ready_at": (gate + timedelta(milliseconds=elapsed_ms - 1)).isoformat(),
        "artifact_published_at": (gate + timedelta(milliseconds=elapsed_ms)).isoformat(),
        "published_from_open_ms": 0, **overrides}


def test_staged_slo_uses_all_mode_original_clocks_not_cached_milliseconds():
    attempts = {"fast": {"attempt_count": 1, "failed_count": 0, "ready": [publication("fast", 250)]},
        "slow": {"attempt_count": 4, "failed_count": 3, "ready": [publication("slow", 30001), publication("slow", 20000)]}}
    result = opening_publication_slo("2026-10-14", attempts, ["fast", "slow"])
    assert result["state"] == "observed"
    assert result["first_mode_published_from_open_ms"] == 250
    assert result["all_modes_published_from_open_ms"] == 20000
    assert [row["passed"] for row in result["stages"]] == [True, True, False, False, False, False]
    assert result["modes"]["slow"]["failed_count"] == 3


@pytest.mark.parametrize("elapsed,passed", [(11000, True), (11001, False)])
def test_eleven_second_publication_deadline_is_inclusive_and_uses_all_modes(elapsed, passed):
    result = opening_publication_slo("2026-10-14", {
        "fast": {"ready": [publication("fast", 500)]},
        "slow": {"ready": [publication("slow", elapsed)]}}, ["fast", "slow"])
    stage = next(row for row in result["stages"] if row["target_ms"] == 11000)
    assert stage["passed"] is passed


def test_missing_mode_cannot_earn_a_stage_from_fast_subset():
    result = opening_publication_slo("2026-10-14", {"fast": {"ready": [publication("fast", 250)]}}, ["fast", "missing"])
    assert result["state"] == "incomplete"
    assert result["published_mode_count"] == 1
    assert result["all_modes_published_from_open_ms"] is None
    assert all(not row["passed"] and row["state"] == "incomplete" for row in result["stages"])


@pytest.mark.parametrize("overrides", [
    {"artifact_published_at": "2026-10-14T09:00:00.500"},
    {"artifact_published_at": "2026-10-14T08:59:59+08:00"},
    {"artifact_published_at": "2026-10-14T09:00:00.498+08:00"},
    {"signal_ready_at": "2026-10-14T08:59:59+08:00"},
    {"gate_at": "2026-10-14T08:59:00+08:00"},
    {"session_date": "2026-10-13"}, {"market": "other"}, {"signal_id": None},
    {"status": "failed"}, {"artifact_published_at": None},
])
def test_invalid_or_backdated_publication_cannot_pass_slo(overrides):
    result = opening_publication_slo("2026-10-14", {"mode": {"ready": [publication("mode", 500, **overrides)]}}, ["mode"])
    assert result["modes"]["mode"]["invalid_ready_records"] == 1
    assert result["all_modes_published_from_open_ms"] is None
    assert not any(row["passed"] for row in result["stages"])


def test_equivalent_utc_clocks_preserve_the_publication_boundary():
    row = publication("mode", 1000, signal_ready_at="2026-10-14T01:00:00.999+00:00",
        artifact_published_at="2026-10-14T01:00:01+00:00")
    result = opening_publication_slo("2026-10-14", {"mode": {"ready": [row]}}, ["mode"])
    assert result["all_modes_published_from_open_ms"] == 1000
    assert all(stage["passed"] for stage in result["stages"])


@pytest.mark.parametrize("markets", [[], ["mode", "mode"]])
def test_publication_slo_requires_explicit_unique_scope(markets):
    with pytest.raises(ValueError, match="nonempty unique"):
        opening_publication_slo("2026-10-14", {}, markets)


@pytest.fixture
def daily_case(monkeypatch, tmp_path):
    from scripts import audit_tw_opening_latency as audit
    from stockagent.live import market_config, market_status
    monkeypatch.setattr(audit, "ROOT", tmp_path)
    monkeypatch.setenv("STOCKAGENT_TW_OPENING_SNAPSHOT_ROOT", str(tmp_path / "snapshots"))
    configs = {"mode": SimpleNamespace(enabled=True, day_trade_simulation_enabled=True,
        holidays=(), day_trade_rule_data_dir=str(tmp_path))}
    monkeypatch.setattr(market_config, "load_market_configs", lambda path: configs)
    monkeypatch.setattr(market_status, "verified_tw_stock_session_day", lambda *a, **k: (True, "official session"))
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    output_root = tmp_path / "observations"
    gate = datetime.fromisoformat("2026-10-14T09:00:00+08:00")

    def observe(seconds):
        return audit.daily_audit(session_date="2026-10-14", state_dir=state_dir,
            markets_dir=tmp_path, output_root=output_root, observed_at=gate + timedelta(seconds=seconds))

    return SimpleNamespace(audit=audit, observe=observe, state=state_dir, output=output_root,
        gate=gate, configs=configs, market_status=market_status)


def fill_evidence(case, elapsed, *, replay=False):
    from stockagent.live.opening_execution_latency import OpeningExecutionObserver
    modes = {"mode": {"session_date": "2026-10-14", "signal_id": "mode-immutable",
        "entry_requested_shares": 1000, "entry_filled_shares": 1000, "entry_unfilled_shares": 0,
        "pending_entry_shares": 0, "pending_entry_source_count": 0, "unsized_entry_target_count": 0,
        "entry_fill_outcome": "filled", "signal_registration_committed": True,
        "entry_fill_policy": "official_open_replay" if replay else "causal_market_full_target_at_best_quote"}}
    (case.state / "status.json").write_text(json.dumps({"modes": modes}))
    samples = OpeningExecutionObserver({}).samples(modes,
        committed_at=case.gate + timedelta(seconds=elapsed), state_revision=1,
        engine_run_id="actual-engine", clock_basis="engine_publication_clock_not_economic_event")
    write(case.state / "execution_latency.jsonl", samples)


def test_daily_freezes_preopen_scope_and_accepts_only_full_original_clock_fills(daily_case):
    case = daily_case
    assert case.observe(-300)["state"] == "armed"
    write(case.state / "opening_signal_latency.jsonl", [publication("mode", 500)])
    fill_evidence(case, 10.9)
    report = case.observe(12)
    assert report["state"] == "passed"
    deadlines = {row["target_ms"]: row for row in report["deadlines"]}
    assert deadlines[11000]["passed"] and not deadlines[10000]["passed"]
    assert report["provider_requests"] == 0 and not report["ledger_modified"]
    assert "execution_samples" not in report


def test_daily_late_fills_never_rewrite_the_first_deadline(daily_case):
    case = daily_case
    case.observe(-300)
    write(case.state / "opening_signal_latency.jsonl", [publication("mode", 500)])
    fill_evidence(case, 12)
    assert case.observe(13)["state"] == "missed_or_unverified"
    path = case.output / "2026-10-14/first_assessment_11000ms.json"
    original = path.read_bytes()
    assert not json.loads(original)["passed"]
    assert case.observe(60)["state"] == "missed_or_unverified"
    assert path.read_bytes() == original
    assert len((case.output / "daily.jsonl").read_text().splitlines()) == 3


def test_daily_late_boot_cannot_fabricate_preopen_scope(daily_case):
    case = daily_case
    write(case.state / "opening_signal_latency.jsonl", [publication("mode", 500)])
    fill_evidence(case, 1)
    report = case.observe(60)
    assert not report["scope_matches_preopen"]
    assert report["state"] == "missed_or_unverified"


def test_daily_roster_change_cannot_shrink_away_a_missing_mode(daily_case):
    case = daily_case
    case.configs["missing"] = SimpleNamespace(**vars(case.configs["mode"]))
    case.observe(-300)
    case.configs["missing"].enabled = False
    write(case.state / "opening_signal_latency.jsonl", [publication("mode", 500)])
    fill_evidence(case, 1)
    report = case.observe(12)
    assert report["expected_markets"] == ["missing", "mode"]
    assert not report["scope_matches_preopen"]
    assert report["state"] == "missed_or_unverified"


@pytest.mark.parametrize("warm_run", ["current-owner", "previous-owner"])
def test_daily_retains_preparation_context_without_rebinding_an_old_warmup(daily_case, warm_run):
    case = daily_case
    path = case.audit.ROOT / "artifacts/discord_bot/service_status.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({"updated_at": (case.gate - timedelta(seconds=301)).isoformat(),
        "run_id": "current-owner", "discord_connected": True, "core_health": "ready",
        "startup_inference_warmup": {"run_id": warm_run, "status": "ready",
            "ready_count": 1, "market_count": 1, "model_cache_resident_ready": True,
            "markets": [{"market": "mode", "status": "ready"}]}}))
    report = case.observe(-300)
    context = report["owner_context"]
    assert context["state"] == "observed" and context["age_seconds"] == 1
    assert context["startup_inference_warmup"]["run_id"] == warm_run
    assert context["same_owner_resident_warmup_declared"] == (warm_run == "current-owner")


@pytest.mark.parametrize("problem", ["replay", "missing_fills", "corrupt_attempt"])
def test_daily_never_hides_missing_corrupt_or_replay_evidence(daily_case, problem):
    case = daily_case
    case.observe(-300)
    write(case.state / "opening_signal_latency.jsonl", [publication("mode", 500)])
    if problem != "missing_fills":
        fill_evidence(case, 1, replay=problem == "replay")
    if problem == "corrupt_attempt":
        with (case.state / "opening_signal_latency.jsonl").open("a") as handle:
            handle.write("broken row\n")
    report = case.observe(12)
    assert report["state"] == "missed_or_unverified"
    if problem != "replay":
        assert report["evidence_errors"]


def test_daily_closed_calendar_does_not_count_as_a_trading_day_failure(daily_case, monkeypatch):
    case = daily_case
    monkeypatch.setattr(case.market_status, "verified_tw_stock_session_day", lambda *a, **k: (False, "official holiday"))
    report = case.observe(12)
    assert report["state"] == "calendar_closed_or_unverified"
    assert "deadlines" not in report


@pytest.mark.parametrize("broken", ["not json", '{"markets": []}', '{"markets": ["mode", "mode"]}'])
def test_daily_corrupt_scope_is_retained_and_cannot_pass(daily_case, broken):
    case = daily_case
    case.observe(-300)
    scope_path = case.output / "2026-10-14/scope.json"
    scope_path.write_text(broken)
    write(case.state / "opening_signal_latency.jsonl", [publication("mode", 500)])
    fill_evidence(case, 1)
    report = case.observe(12)
    assert report["state"] == "missed_or_unverified"
    assert report["expected_markets"] == ["mode"]
    assert any(row.startswith("preopen_scope_") for row in report["evidence_errors"])
    assert scope_path.read_text() == broken


def test_daily_malformed_override_is_visible_without_dropping_mode(daily_case):
    case = daily_case
    path = case.audit.ROOT / "artifacts/discord_bot/state.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({"markets": {"mode": None}}))
    report = case.observe(-300)
    assert report["expected_markets"] == ["mode"]
    assert report["evidence_errors"] == ["invalid_market_override:mode"]
    assert report["state"] == "scope_unverified"


@pytest.mark.parametrize("changes", [
    {"captured_before_open": "true"},
    {"observed_at": "2026-10-14T09:00:01+08:00"},
    {"observed_at": "2026-10-14T08:55:00"},
    {"observed_at": "2026-10-13T08:55:00+08:00"},
])
def test_daily_preopen_flag_does_not_replace_original_clock(daily_case, changes):
    case = daily_case
    case.observe(-300)
    path = case.output / "2026-10-14/scope.json"
    scope = json.loads(path.read_text())
    scope.update(changes)
    path.write_text(json.dumps(scope))
    write(case.state / "opening_signal_latency.jsonl", [publication("mode", 500)])
    fill_evidence(case, 1)
    assert case.observe(12)["state"] == "missed_or_unverified"


def test_daily_module_import_does_not_load_discord_torch_or_provider(tmp_path):
    import subprocess
    import sys
    subprocess.run([sys.executable, "-c", "import sys; import scripts.audit_tw_opening_latency; "
        "assert not ({'torch', 'services.discord_bot.bot', 'stockagent.live.quote_provider'} & sys.modules.keys())"],
        check=True, timeout=20)


def test_daily_native_units_are_read_only_observers_with_scoped_install():
    from pathlib import Path
    service = Path("deploy/systemd/stockagent-tw-opening-audit.service.in").read_text()
    timer = Path("deploy/systemd/stockagent-tw-opening-audit.timer.in").read_text()
    installer = Path("scripts/install_discord_bot_service.sh").read_text()
    assert "--daily" in service and "Type=oneshot" in service
    assert "PrivateNetwork=true" in service and "ProtectSystem=strict" in service
    assert "ReadWritePaths=@REPO_ROOT@/artifacts/live" in service
    assert "CPUWeight=10" in service and "IOWeight=10" in service
    assert "Requires=" not in service and "PartOf=" not in service
    assert "08:55:00 Asia/Taipei" in timer and "09:00:11 Asia/Taipei" in timer
    assert "09:00:31 Asia/Taipei" in timer and "13:40:00 Asia/Taipei" in timer
    assert "Persistent=true" in timer and "RandomizedDelaySec=0" in timer
    scoped = installer.split('if [[ "$OPENING_AUDIT_ONLY" == true ]]; then')[-1].split("fi", 1)[0]
    assert 'systemctl restart "$SERVICE_NAME"' not in scoped
    assert '--opening-audit-only' in installer
    phase_service = Path("deploy/systemd/stockagent-tw-quote-phase-benchmark.service.in").read_text()
    phase_timer = Path("deploy/systemd/stockagent-tw-quote-phase-benchmark.timer.in").read_text()
    assert "--scheduled" in phase_service and "Type=oneshot" in phase_service
    assert "ReadWritePaths=@REPO_ROOT@/artifacts/operations" in phase_service
    assert "CPUWeight=10" in phase_service and "IOWeight=10" in phase_service
    assert "Requires=" not in phase_service and "PartOf=" not in phase_service
    assert "Mon..Fri *-*-* 10:30:00 Asia/Taipei" in phase_timer
    assert "Mon..Fri *-*-* 13:40:00 Asia/Taipei" in phase_timer
    assert "Persistent=false" in phase_timer and "RandomizedDelaySec=0" in phase_timer
    assert "10:31:00 Asia/Taipei" in timer and "15:10:00 Asia/Taipei" in timer


def test_phase_latency_separates_full_scope_delta_no_print_and_failures(tmp_path):
    from hashlib import sha256
    from scripts.audit_tw_opening_latency import quote_session_phase
    symbols = ["A", "B"]
    path = tmp_path / "phase.jsonl"
    base = {"job_id": "full", "owner_pid": 1, "requested_count": 2}
    progress = {**base, "event": "coverage_progress", "measurement_clock": "provider_job_perf_counter_v1",
        "valid_open_count": 1, "observed_no_print_count": 1, "response_count": 2, "elapsed_ms": 12.5}
    stage = {"mis_requests": [{"http_headers_ms": 8., "body_read_ms": 1., "json_decode_ms": .5,
        "row_decode_ms": .25, "http_session_origin": "validated_prewarm_exclusive_handoff"}], "mis_progress_spans": [{"callback_ms": 2.}],
        "mis_span_policy": "nested_local_wall_spans_overlap_http_not_additive_v1"}
    write(path, [event("mis", 1, event="started", requested_scope_sha256=sha256(b"A\nB").hexdigest(), **base),
        event("mis", 14, **progress), event("mis", 20, event="finished", state="complete", last_total_ms=18.,
            source_and_persistence_ms=19., transport_timing=stage, last_cache_merge_ms=.25,
            provider_call_offset_ms=.1, provider_invocation_at_taipei="2026-10-13T09:00:00.002+08:00",
            dispatch_telemetry_wait_ms=.2, dispatch_telemetry_recorded=False, **base),
        event("mis", 3600000, event="started", job_id="delta", owner_pid=1, requested_count=1),
        event("mis", 3600010, event="finished", job_id="delta", owner_pid=1, requested_count=1, state="failed", last_total_ms=4000.),
        event("mis", 18000000, event="started", job_id="close", owner_pid=1, requested_count=2),
        event("mis", 18000010, event="finished", job_id="close", owner_pid=1, state="failed", last_total_ms=4200.)])
    result = opening_coverage_milestones(path, "2026-10-13", symbols)["provider_phase_latency"]
    assert result["contract"] == "quote_phase_full_scope_per_job_latency_v2"
    opening = result["phases"]["opening"]["mis"]
    assert opening["full_scope_jobs"] == 1
    job = opening["jobs"][0]
    assert job["first_valid_open_ms"] == job["resolved_coverage_ms"]["1.0"] == 12.5
    assert "1.0" not in job["positive_coverage_ms"]
    assert job["unknown_count"] == 0 and job["source_and_persistence_ms"] == 19.
    assert job["stage_timing"] == {"transport_timing": stage, "last_cache_merge_ms": .25,
        "provider_call_offset_ms": .1, "dispatch_telemetry_wait_ms": .2, "dispatch_telemetry_recorded": False}
    assert job["stage_timing_boundary"] == "nested_wall_spans_overlap_not_additive_v1"
    assert job["provider_invocation_after_open_ms"] == 2.
    assert job["provider_invocation_boundary"] == "provider_function_entry_not_http_wire_or_exchange_publish"
    intraday = result["phases"]["intraday"]["mis"]
    assert intraday["full_scope_jobs"] == 0 and intraday["failed_or_degraded_jobs"] == 1
    assert intraday["source_duration_percentiles_ms"]["0.99"] == 4000.
    assert intraday["jobs"][0]["stage_timing"] == {}  # Legacy is unmeasured, not zero.
    assert result["phases"]["postclose"]["mis"]["failed_or_degraded_jobs"] == 1
    assert quote_session_phase(datetime.fromisoformat("2026-10-13T01:00:00+00:00")) == "opening"
    with pytest.raises(ValueError, match="aware"):
        quote_session_phase(datetime(2026, 10, 13, 9))
